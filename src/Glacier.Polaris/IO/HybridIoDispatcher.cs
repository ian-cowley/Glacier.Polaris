using System;
using System.Buffers;
using System.IO;
using System.IO.MemoryMappedFiles;
using Microsoft.Win32.SafeHandles;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// Local Tier-0 Hybrid I/O Dispatcher that routes small files (&lt; 1 MB) through pooled
    /// <see cref="RandomAccess"/> reads and larger files (&gt;= 1 MB) through sliding 64 KB-aligned
    /// <see cref="MemoryMappedFile"/> views.
    /// </summary>
    internal static class HybridIoDispatcher
    {
        public const long OneMegaByte = 1024 * 1024;
        private const int LookaheadBytes = 65536;
        private const long ViewAlignmentMask = ~0xFFFFL; // 64 KB OS allocation granularity

        /// <summary>
        /// Maximum chunk size for memory-mapped file processing. Defaults to 16 MB.
        /// Configurable internally for boundary condition testing.
        /// </summary>
        internal static int MaxChunkSize { get; set; } = 16 * 1024 * 1024;

        internal delegate void ChunkProcessor(ReadOnlySpan<byte> chunk, bool isLastChunk);

        public static void ProcessFile(string path, ChunkProcessor processor)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);
            ArgumentNullException.ThrowIfNull(processor);

            int maxChunk = Math.Clamp(MaxChunkSize, 64, 512 * 1024 * 1024);

            using SafeFileHandle handle = File.OpenHandle(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                FileOptions.SequentialScan);

            long actualLength = RandomAccess.GetLength(handle);
            if (actualLength <= 0)
            {
                processor(ReadOnlySpan<byte>.Empty, isLastChunk: true);
                return;
            }

            if (actualLength < OneMegaByte && actualLength <= maxChunk)
            {
                ProcessRentedArray(handle, (int)actualLength, maxChunk, processor);
            }
            else
            {
                ProcessMemoryMapped(handle, actualLength, maxChunk, processor);
            }
        }

        private static void ProcessRentedArray(
            SafeFileHandle handle,
            int initialLength,
            int maxChunk,
            ChunkProcessor processor)
        {
            int targetCap = Math.Max(1, initialLength);
            byte[] rented = ArrayPool<byte>.Shared.Rent(targetCap);
            int totalRead = 0;
            bool promoteToMmf = false;
            Span<byte> probe = stackalloc byte[1];

            try
            {
                while (true)
                {
                    while (totalRead < targetCap)
                    {
                        int bytesRead = RandomAccess.Read(handle, rented.AsSpan(totalRead, targetCap - totalRead), totalRead);
                        if (bytesRead == 0)
                        {
                            processor(rented.AsSpan(0, totalRead), isLastChunk: true);
                            return;
                        }
                        totalRead += bytesRead;
                    }

                    int probeRead = RandomAccess.Read(handle, probe, totalRead);
                    if (probeRead == 0)
                    {
                        processor(rented.AsSpan(0, totalRead), isLastChunk: true);
                        return;
                    }

                    long currentLength = Math.Max(RandomAccess.GetLength(handle), totalRead + 1L);
                    if (currentLength >= OneMegaByte || currentLength > maxChunk)
                    {
                        promoteToMmf = true;
                        break;
                    }

                    int maxRentedLimit = (int)Math.Min(OneMegaByte - 1, maxChunk);
                    int newCap = (int)Math.Min(maxRentedLimit, Math.Max(currentLength, (long)targetCap * 2));
                    if (newCap <= targetCap)
                    {
                        promoteToMmf = true;
                        break;
                    }

                    if (newCap > rented.Length)
                    {
                        byte[] newRented = ArrayPool<byte>.Shared.Rent(newCap);
                        rented.AsSpan(0, totalRead).CopyTo(newRented);
                        ArrayPool<byte>.Shared.Return(rented);
                        rented = newRented;
                    }

                    rented[totalRead] = probe[0];
                    totalRead += 1;
                    targetCap = newCap;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }

            if (promoteToMmf)
            {
                long latestLength = RandomAccess.GetLength(handle);
                ProcessMemoryMapped(handle, latestLength, maxChunk, processor);
            }
        }

        private static unsafe void ProcessMemoryMapped(
            SafeFileHandle handle,
            long actualLength,
            int maxChunk,
            ChunkProcessor processor)
        {
            if (actualLength <= 0)
            {
                processor(ReadOnlySpan<byte>.Empty, isLastChunk: true);
                return;
            }

            using var mmf = MemoryMappedFile.CreateFromFile(
                handle,
                mapName: null,
                capacity: 0,
                MemoryMappedFileAccess.Read,
                HandleInheritability.None,
                leaveOpen: true);

            if (actualLength <= maxChunk)
            {
                using var accessor = mmf.CreateViewAccessor(0, actualLength, MemoryMappedFileAccess.Read);
                byte* pointer = null;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
                try
                {
                    byte* start = pointer + accessor.PointerOffset;
                    processor(new ReadOnlySpan<byte>(start, (int)actualLength), isLastChunk: true);
                }
                finally
                {
                    accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                }
                return;
            }

            long logicalOffset = 0;
            while (logicalOffset < actualLength)
            {
                long alignedViewStart = logicalOffset & ViewAlignmentMask;
                long nominalLogicalEnd = Math.Min(logicalOffset + maxChunk, actualLength);
                long maxViewEnd = Math.Min(actualLength, nominalLogicalEnd + LookaheadBytes);
                long viewSize = maxViewEnd - alignedViewStart;

                using var accessor = mmf.CreateViewAccessor(alignedViewStart, viewSize, MemoryMappedFileAccess.Read);
                byte* pointer = null;
                accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
                try
                {
                    byte* viewBase = pointer + accessor.PointerOffset;
                    long logicalEnd = nominalLogicalEnd;

                    if (logicalEnd < actualLength)
                    {
                        long scanLimit = Math.Min(actualLength, logicalEnd + LookaheadBytes);
                        long scan = logicalEnd;
                        bool foundNewline = false;
                        while (scan < scanLimit)
                        {
                            if (viewBase[scan - alignedViewStart] == (byte)'\n')
                            {
                                logicalEnd = scan + 1;
                                foundNewline = true;
                                break;
                            }
                            scan++;
                        }

                        if (!foundNewline)
                        {
                            int backtrack = 0;
                            while (backtrack < 3 &&
                                   logicalEnd - backtrack > logicalOffset + 1 &&
                                   IsUtf8ContinuationByte(viewBase[logicalEnd - backtrack - alignedViewStart]))
                            {
                                backtrack++;
                            }
                            logicalEnd -= backtrack;
                        }
                    }

                    int chunkSize = checked((int)(logicalEnd - logicalOffset));
                    bool isLast = logicalEnd >= actualLength;
                    var chunkData = new ReadOnlySpan<byte>(viewBase + (logicalOffset - alignedViewStart), chunkSize);

                    processor(chunkData, isLast);
                    logicalOffset = logicalEnd;
                }
                finally
                {
                    accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                }
            }
        }

        private static bool IsUtf8ContinuationByte(byte b) => (b & 0xC0) == 0x80;
    }
}
