using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public enum FilterOperation
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual
    }

    /// <summary>
    /// Implements high-performance SIMD vectorized and multi-threaded filter operations.
    /// Supports sbyte, byte, short, ushort, int, uint, long, ulong, float, and double.
    /// </summary>
    public static class FilterKernels
    {
        public static (int[] rentedIndices, int count) Filter<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            if (typeof(T) == typeof(int) || typeof(T) == typeof(double) ||
                typeof(T) == typeof(float) || typeof(T) == typeof(long) ||
                typeof(T) == typeof(ulong) || typeof(T) == typeof(uint) ||
                typeof(T) == typeof(short) || typeof(T) == typeof(ushort) ||
                typeof(T) == typeof(sbyte) || typeof(T) == typeof(byte))
            {
                return FilterGeneric<T>(data, threshold, op);
            }

            // Scalar fallback for other non-numeric unmanaged types (if any)
            int[] indices = ArrayPool<int>.Shared.Rent(data.Length > 0 ? data.Length : 1);
            int count = 0;
            for (int i = 0; i < data.Length; i++)
            {
                if (Matches(data[i], threshold, op))
                {
                    indices[count++] = i;
                }
            }
            return (indices, count);
        }

        private static unsafe (int[] rentedIndices, int count) FilterGeneric<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            int dataLen = data.Length;
            int parallelThreshold = ParallelThresholds.GetFilterParallelThreshold<T>();

            if (dataLen >= parallelThreshold)
            {
                int numThreads = Environment.ProcessorCount;
                int maxThreadsNeeded = (dataLen + 63) / 64;
                numThreads = Math.Min(numThreads, Math.Max(1, maxThreadsNeeded));

                int rawChunk = (dataLen + numThreads - 1) / numThreads;
                int chunkSize = Math.Max(64, ((rawChunk + 63) / 64) * 64);
                int wordsRequired = (dataLen + 63) / 64;

                ulong[] rentedMask = ArrayPool<ulong>.Shared.Rent(wordsRequired > 0 ? wordsRequired : 1);
                int[] counts = new int[numThreads];

                try
                {
                    fixed (T* pData = data)
                    fixed (ulong* pMask = rentedMask)
                    {
                        nint ptrData = (nint)pData;
                        nint ptrMask = (nint)pMask;

                        // ─────────────────────────────────────────────────────────────
                        // PHASE 1: Single DRAM scan -> Generate bitmask + PopCount
                        // ─────────────────────────────────────────────────────────────
                        System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                        {
                            T* localData = (T*)ptrData;
                            ulong* localMask = (ulong*)ptrMask;

                            int start = p * chunkSize;
                            int end = Math.Min(start + chunkSize, dataLen);
                            if (start >= end) return;

                            int localCount = 0;
                            int i = start;

                            if (Vector512.IsHardwareAccelerated && (end - start) >= 64)
                            {
                                var vThreshold = Vector512.Create(threshold);
                                int step = Vector512<T>.Count;
                                int numVectors = 64 / step;
                                int limit = end - 64;

                                switch (op)
                                {
                                    case FilterOperation.Equal:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector512.Load(localData + i + s * step);
                                                var vMask = Vector512.Equals(vData, vThreshold);
                                                word |= (vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.NotEqual:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector512.Load(localData + i + s * step);
                                                var vMask = ~Vector512.Equals(vData, vThreshold);
                                                word |= (vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.GreaterThan:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector512.Load(localData + i + s * step);
                                                var vMask = Vector512.GreaterThan(vData, vThreshold);
                                                word |= (vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.GreaterThanOrEqual:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector512.Load(localData + i + s * step);
                                                var vMask = Vector512.GreaterThanOrEqual(vData, vThreshold);
                                                word |= (vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.LessThan:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector512.Load(localData + i + s * step);
                                                var vMask = Vector512.LessThan(vData, vThreshold);
                                                word |= (vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.LessThanOrEqual:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector512.Load(localData + i + s * step);
                                                var vMask = Vector512.LessThanOrEqual(vData, vThreshold);
                                                word |= (vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;
                                }
                            }
                            else if (Vector256.IsHardwareAccelerated && (end - start) >= 64)
                            {
                                var vThreshold = Vector256.Create(threshold);
                                int step = Vector256<T>.Count;
                                int numVectors = 64 / step;
                                int limit = end - 64;

                                switch (op)
                                {
                                    case FilterOperation.Equal:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector256.Load(localData + i + s * step);
                                                var vMask = Vector256.Equals(vData, vThreshold);
                                                word |= ((ulong)vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.NotEqual:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector256.Load(localData + i + s * step);
                                                var vMask = ~Vector256.Equals(vData, vThreshold);
                                                word |= ((ulong)vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.GreaterThan:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector256.Load(localData + i + s * step);
                                                var vMask = Vector256.GreaterThan(vData, vThreshold);
                                                word |= ((ulong)vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.GreaterThanOrEqual:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector256.Load(localData + i + s * step);
                                                var vMask = Vector256.GreaterThanOrEqual(vData, vThreshold);
                                                word |= ((ulong)vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.LessThan:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector256.Load(localData + i + s * step);
                                                var vMask = Vector256.LessThan(vData, vThreshold);
                                                word |= ((ulong)vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;

                                    case FilterOperation.LessThanOrEqual:
                                        while (i <= limit)
                                        {
                                            ulong word = 0UL;
                                            for (int s = 0; s < numVectors; s++)
                                            {
                                                var vData = Vector256.Load(localData + i + s * step);
                                                var vMask = Vector256.LessThanOrEqual(vData, vThreshold);
                                                word |= ((ulong)vMask.ExtractMostSignificantBits() << (s * step));
                                            }
                                            localMask[i / 64] = word;
                                            localCount += BitOperations.PopCount(word);
                                            i += 64;
                                        }
                                        break;
                                }
                            }
                            else
                            {
                                int limit = end - 64;
                                while (i <= limit)
                                {
                                    ulong word = 0UL;
                                    for (int k = 0; k < 64; k++)
                                    {
                                        if (Matches(localData[i + k], threshold, op))
                                        {
                                            word |= (1UL << k);
                                        }
                                    }
                                    localMask[i / 64] = word;
                                    localCount += BitOperations.PopCount(word);
                                    i += 64;
                                }
                            }

                            // Remainder elements for this thread partition
                            if (i < end)
                            {
                                int wordIdx = i / 64;
                                ulong remWord = 0UL;
                                int bit = 0;
                                for (; i < end; i++, bit++)
                                {
                                    if (Matches(localData[i], threshold, op))
                                    {
                                        remWord |= (1UL << bit);
                                        localCount++;
                                    }
                                }
                                localMask[wordIdx] = remWord;
                            }

                            counts[p] = localCount;
                        });

                        // Prefix sum over thread partition counts to compute destination offsets
                        int totalCount = 0;
                        int[] offsets = new int[numThreads];
                        for (int p = 0; p < numThreads; p++)
                        {
                            offsets[p] = totalCount;
                            totalCount += counts[p];
                        }

                        int[] pIndices = ArrayPool<int>.Shared.Rent(totalCount > 0 ? totalCount : 1);

                        // ─────────────────────────────────────────────────────────────
                        // PHASE 2: Cache-Resident Index Gathering (0 DRAM reads for data)
                        // ─────────────────────────────────────────────────────────────
                        fixed (int* pOutIndices = pIndices)
                        {
                            nint ptrOut = (nint)pOutIndices;

                            System.Threading.Tasks.Parallel.For(0, numThreads, p =>
                            {
                                ulong* localMask = (ulong*)ptrMask;
                                int* localOut = (int*)ptrOut;

                                int start = p * chunkSize;
                                int end = Math.Min(start + chunkSize, dataLen);
                                if (start >= end) return;

                                int destOffset = offsets[p];
                                int startWord = start / 64;
                                int endWord = (end + 63) / 64;

                                Vector512<int> vIota0 = default, vIota1 = default, vIota2 = default, vIota3 = default;
                                if (Vector512.IsHardwareAccelerated)
                                {
                                    vIota0 = Vector512.Create(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
                                    vIota1 = Vector512.Create(16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
                                    vIota2 = Vector512.Create(32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47);
                                    vIota3 = Vector512.Create(48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63);
                                }

                                Vector256<int> vIota256_0 = default, vIota256_1 = default, vIota256_2 = default, vIota256_3 = default;
                                Vector256<int> vIota256_4 = default, vIota256_5 = default, vIota256_6 = default, vIota256_7 = default;
                                if (Vector256.IsHardwareAccelerated)
                                {
                                    vIota256_0 = Vector256.Create(0, 1, 2, 3, 4, 5, 6, 7);
                                    vIota256_1 = Vector256.Create(8, 9, 10, 11, 12, 13, 14, 15);
                                    vIota256_2 = Vector256.Create(16, 17, 18, 19, 20, 21, 22, 23);
                                    vIota256_3 = Vector256.Create(24, 25, 26, 27, 28, 29, 30, 31);
                                    vIota256_4 = Vector256.Create(32, 33, 34, 35, 36, 37, 38, 39);
                                    vIota256_5 = Vector256.Create(40, 41, 42, 43, 44, 45, 46, 47);
                                    vIota256_6 = Vector256.Create(48, 49, 50, 51, 52, 53, 54, 55);
                                    vIota256_7 = Vector256.Create(56, 57, 58, 59, 60, 61, 62, 63);
                                }

                                for (int w = startWord; w < endWord; w++)
                                {
                                    ulong word = localMask[w];
                                    int baseIdx = w * 64;

                                    // FAST PATH 1: 0 elements match -> skip 64 rows instantly
                                    if (word == 0UL) continue;

                                    // FAST PATH 2: All 64 elements match -> bulk 64-index SIMD store
                                    if (word == ulong.MaxValue && (baseIdx + 63) < end)
                                    {
                                        if (Vector512.IsHardwareAccelerated)
                                        {
                                            var vBase = Vector512.Create(baseIdx);
                                            (vBase + vIota0).Store(localOut + destOffset);
                                            (vBase + vIota1).Store(localOut + destOffset + 16);
                                            (vBase + vIota2).Store(localOut + destOffset + 32);
                                            (vBase + vIota3).Store(localOut + destOffset + 48);
                                            destOffset += 64;
                                            continue;
                                        }
                                        else if (Vector256.IsHardwareAccelerated)
                                        {
                                            var vBase = Vector256.Create(baseIdx);
                                            (vBase + vIota256_0).Store(localOut + destOffset);
                                            (vBase + vIota256_1).Store(localOut + destOffset + 8);
                                            (vBase + vIota256_2).Store(localOut + destOffset + 16);
                                            (vBase + vIota256_3).Store(localOut + destOffset + 24);
                                            (vBase + vIota256_4).Store(localOut + destOffset + 32);
                                            (vBase + vIota256_5).Store(localOut + destOffset + 40);
                                            (vBase + vIota256_6).Store(localOut + destOffset + 48);
                                            (vBase + vIota256_7).Store(localOut + destOffset + 56);
                                            destOffset += 64;
                                            continue;
                                        }
                                    }

                                    // GENERAL PATH: Bit scanning with single-cycle tzcnt and blsr
                                    while (word != 0UL)
                                    {
                                        int bit = BitOperations.TrailingZeroCount(word);
                                        int rowIdx = baseIdx + bit;
                                        if (rowIdx < end)
                                        {
                                            localOut[destOffset++] = rowIdx;
                                        }
                                        word &= (word - 1UL);
                                    }
                                }
                            });
                        }

                        return (pIndices, totalCount);
                    }
                }
                finally
                {
                    ArrayPool<ulong>.Shared.Return(rentedMask);
                }
            }

            return FilterSequential(data, threshold, op);
        }

        private static (int[] rentedIndices, int count) FilterSequential<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            int[] indices = ArrayPool<int>.Shared.Rent(data.Length > 0 ? data.Length : 1);
            int count = 0;
            int idx = 0;

            if (Vector512.IsHardwareAccelerated && data.Length >= Vector512<T>.Count)
            {
                int step = Vector512<T>.Count;
                var vThreshold = Vector512.Create(threshold);
                for (; idx <= data.Length - step; idx += step)
                {
                    var vData = Vector512.LoadUnsafe(ref MemoryMarshal.GetReference(data.Slice(idx)));
                    Vector512<T> vMask = op switch
                    {
                        FilterOperation.Equal => Vector512.Equals(vData, vThreshold),
                        FilterOperation.NotEqual => ~Vector512.Equals(vData, vThreshold),
                        FilterOperation.GreaterThan => Vector512.GreaterThan(vData, vThreshold),
                        FilterOperation.GreaterThanOrEqual => Vector512.GreaterThanOrEqual(vData, vThreshold),
                        FilterOperation.LessThan => Vector512.LessThan(vData, vThreshold),
                        FilterOperation.LessThanOrEqual => Vector512.LessThanOrEqual(vData, vThreshold),
                        _ => throw new NotImplementedException()
                    };
                    ulong mask = vMask.ExtractMostSignificantBits();
                    while (mask != 0)
                    {
                        int bit = BitOperations.TrailingZeroCount(mask);
                        indices[count++] = idx + bit;
                        mask &= (mask - 1);
                    }
                }
            }
            else if (Vector256.IsHardwareAccelerated && data.Length >= Vector256<T>.Count)
            {
                int step = Vector256<T>.Count;
                var vThreshold = Vector256.Create(threshold);
                for (; idx <= data.Length - step; idx += step)
                {
                    var vData = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(data.Slice(idx)));
                    Vector256<T> vMask = op switch
                    {
                        FilterOperation.Equal => Vector256.Equals(vData, vThreshold),
                        FilterOperation.NotEqual => ~Vector256.Equals(vData, vThreshold),
                        FilterOperation.GreaterThan => Vector256.GreaterThan(vData, vThreshold),
                        FilterOperation.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(vData, vThreshold),
                        FilterOperation.LessThan => Vector256.LessThan(vData, vThreshold),
                        FilterOperation.LessThanOrEqual => Vector256.LessThanOrEqual(vData, vThreshold),
                        _ => throw new NotImplementedException()
                    };
                    ulong mask = vMask.ExtractMostSignificantBits();
                    while (mask != 0)
                    {
                        int bit = BitOperations.TrailingZeroCount(mask);
                        indices[count++] = idx + bit;
                        mask &= (mask - 1);
                    }
                }
            }
            for (; idx < data.Length; idx++)
            {
                if (Matches(data[idx], threshold, op)) indices[count++] = idx;
            }
            return (indices, count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Matches<T>(T val, T threshold, FilterOperation op) where T : IComparable<T>
        {
            int cmp = val.CompareTo(threshold);
            return op switch
            {
                FilterOperation.Equal => cmp == 0,
                FilterOperation.NotEqual => cmp != 0,
                FilterOperation.GreaterThan => cmp > 0,
                FilterOperation.GreaterThanOrEqual => cmp >= 0,
                FilterOperation.LessThan => cmp < 0,
                FilterOperation.LessThanOrEqual => cmp <= 0,
                _ => false
            };
        }
    }
}
