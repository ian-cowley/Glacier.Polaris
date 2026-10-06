using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// High-performance radix sort kernels for Int32 and Float64.
    /// Uses sequential 8-bit packed-ulong radix for Int32 ArgSort and
    /// Array.Sort with struct LongIndexPair for Float64 ArgSort (corrected IEEE 754 transform).
    /// </summary>
    public static partial class SortKernels
    {
        private const int BucketCount = 256;

        /// <summary>In-place parallel radix sort for Int32 data.</summary>
        public static unsafe void Sort(Span<int> data)
        {
            if (data.Length <= 1) return;

            int[]? rented = null;
            int* buffer;
            if (data.Length <= 1024)
            {
                int* stackBuffer = stackalloc int[data.Length];
                buffer = stackBuffer;
            }
            else
            {
                rented = System.Buffers.ArrayPool<int>.Shared.Rent(data.Length);
                fixed (int* pRented = rented) buffer = pRented;
            }

            try
            {
                fixed (int* pData = data)
                {
                    int* src = pData;
                    int* dst = buffer;
                    int totalLen = data.Length;

                    for (int shift = 0; shift < 32; shift += 8)
                    {
                        DoRadixPass(src, dst, null, totalLen, shift);
                        int* temp = src;
                        src = dst;
                        dst = temp;
                    }

                    if (src != pData)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(
                            pData, src, (uint)(totalLen * sizeof(int)));
                    }
                }
            }
            finally
            {
                if (rented != null)
                    System.Buffers.ArrayPool<int>.Shared.Return(rented);
            }
        }

        /// <summary>ArgSort for Int32 — returns new int[]. Packed ulong + 4-pass 8-bit radix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe int[] ArgSort(ReadOnlySpan<int> data)
        {
            if (data.Length == 0) return Array.Empty<int>();
            int n = data.Length;
            int[] indices = new int[n];

            ulong[] packed = System.Buffers.ArrayPool<ulong>.Shared.Rent(n);
            ulong[] buffer = System.Buffers.ArrayPool<ulong>.Shared.Rent(n);

            try
            {
                fixed (int* pData = data)
                fixed (int* pIdx = indices)
                fixed (ulong* pPacked = packed)
                fixed (ulong* pBuffer = buffer)
                {
                    ulong* pk = pPacked;
                    for (int i = 0; i < n; i++)
                        pk[i] = ((ulong)((uint)pData[i] ^ 0x80000000) << 32) | (uint)i;

                    ulong* src = pPacked;
                    ulong* dst = pBuffer;
                    int* counts = stackalloc int[256];
                    for (int shift = 32; shift < 64; shift += 8)
                    {
                        for (int j = 0; j < n; j++) counts[(int)((src[j] >> shift) & 0xFF)]++;
                        int off = 0;
                        for (int j = 0; j < 256; j++) { int c = counts[j]; counts[j] = off; off += c; }
                        for (int j = 0; j < n; j++) dst[counts[(int)((src[j] >> shift) & 0xFF)]++] = src[j];
                        ulong* t = src; src = dst; dst = t;
                        for (int j = 0; j < 256; j++) counts[j] = 0;
                    }

                    if (src != pPacked)
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pPacked, src, (uint)(n * sizeof(ulong)));

                    for (int i = 0; i < n; i++)
                        pIdx[i] = (int)(pPacked[i] & 0xFFFFFFFF);
                }
            }
            finally
            {
                System.Buffers.ArrayPool<ulong>.Shared.Return(packed);
                System.Buffers.ArrayPool<ulong>.Shared.Return(buffer);
            }

            return indices;
        }

        /// <summary>In-place ArgSort for Int32 (re-sorts existing indices). Packed ulong + 4-pass 8-bit radix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe void ArgSort(ReadOnlySpan<int> data, Span<int> indices, bool descending = false)
        {
            if (data.Length == 0) return;
            int n = data.Length;

            ulong[] packed = System.Buffers.ArrayPool<ulong>.Shared.Rent(n);
            ulong[] buffer = System.Buffers.ArrayPool<ulong>.Shared.Rent(n);

            try
            {
                fixed (int* pData = data)
                fixed (int* pIdx = indices)
                fixed (ulong* pPacked = packed)
                fixed (ulong* pBuffer = buffer)
                {
                    ulong* pk = pPacked;
                    for (int i = 0; i < n; i++)
                        pk[i] = ((ulong)((uint)pData[pIdx[i]] ^ 0x80000000) << 32) | (uint)i;

                    ulong* src = pPacked;
                    ulong* dst = pBuffer;
                    int* counts = stackalloc int[256];
                    for (int shift = 32; shift < 64; shift += 8)
                    {
                        for (int j = 0; j < n; j++) counts[(int)((src[j] >> shift) & 0xFF)]++;
                        int off = 0;
                        for (int j = 0; j < 256; j++) { int c = counts[j]; counts[j] = off; off += c; }
                        for (int j = 0; j < n; j++) dst[counts[(int)((src[j] >> shift) & 0xFF)]++] = src[j];
                        ulong* t = src; src = dst; dst = t;
                        for (int j = 0; j < 256; j++) counts[j] = 0;
                    }

                    if (src != pPacked)
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pPacked, src, (uint)(n * sizeof(ulong)));

                    for (int i = 0; i < n; i++)
                        pIdx[i] = (int)(pPacked[i] & 0xFFFFFFFF);
                }

                if (descending)
                    for (int i = 0; i < n / 2; i++) { int t = indices[i]; indices[i] = indices[n - 1 - i]; indices[n - 1 - i] = t; }
            }
            finally
            {
                System.Buffers.ArrayPool<ulong>.Shared.Return(packed);
                System.Buffers.ArrayPool<ulong>.Shared.Return(buffer);
            }
        }

        /// <summary>
        /// ArgSort using the system's built-in Array.Sort (introsort, potentially SIMD-accelerated).
        /// Creates (key, index) pairs and sorts. For data where n log n < n * radix_passes * constant,
        /// this can be faster than the custom radix sort.
        /// </summary>
        public static int[] ArgSortSystem(ReadOnlySpan<int> data, bool descending = false)
        {
            int n = data.Length;
            var idx = new int[n];
            var keys = new int[n];
            data.CopyTo(keys.AsSpan());
            for (int i = 0; i < n; i++) idx[i] = i;
            Array.Sort(keys, idx);
            if (descending) Array.Reverse(idx);
            return idx;
        }

        /// <summary>Fast ArgSort using .NET's built-in Array.Sort with parallel comparer. Matches Python sort performance.</summary>
        public static unsafe int[] FastArgSortInt32(int[] data)
        {
            int n = data.Length;
            int[] indices = new int[n];
            for (int i = 0; i < n; i++) indices[i] = i;
            Array.Sort(indices, (x, y) => data[x].CompareTo(data[y]));
            return indices;
        }

        /// <summary>
        /// One radix pass for 32-bit keys.
        /// When dataIndirect is null: sorts values directly (src holds values to sort).
        /// When dataIndirect is not null: sorts indices by the values at dataIndirect[src[j]].
        /// Both counting and scatter are parallelized for large arrays.
        /// </summary>
        private static unsafe void DoRadixPass(int* src, int* dst, int* dataIndirect, int length, int shift)
        {
            if (length <= 1) return;

            int* counts = stackalloc int[BucketCount];
            for (int j = 0; j < BucketCount; j++) counts[j] = 0;

            int numThreads = ComputeThreadCount(length);

            if (numThreads <= 1)
            {
                if (dataIndirect == null)
                {
                    for (int j = 0; j < length; j++)
                        counts[(src[j] >> shift) & 0xFF]++;

                    int offset = 0;
                    for (int j = 0; j < BucketCount; j++)
                    {
                        int c = counts[j];
                        counts[j] = offset;
                        offset += c;
                    }

                    for (int j = 0; j < length; j++)
                    {
                        int val = src[j];
                        dst[counts[(val >> shift) & 0xFF]++] = val;
                    }
                }
                else
                {
                    for (int j = 0; j < length; j++)
                        counts[(dataIndirect[src[j]] >> shift) & 0xFF]++;

                    int offset = 0;
                    for (int j = 0; j < BucketCount; j++)
                    {
                        int c = counts[j];
                        counts[j] = offset;
                        offset += c;
                    }

                    for (int j = 0; j < length; j++)
                    {
                        int idx = src[j];
                        dst[counts[(dataIndirect[idx] >> shift) & 0xFF]++] = idx;
                    }
                }
                return;
            }

            int[][] localCounts = new int[numThreads][];
            for (int t = 0; t < numThreads; t++)
                localCounts[t] = new int[BucketCount];

            int len = length;

            if (dataIndirect == null)
            {
                Parallel.For(0, numThreads, t =>
                {
                    int chunkSize = (len + numThreads - 1) / numThreads;
                    int start = t * chunkSize;
                    int end = Math.Min(start + chunkSize, len);
                    var local = localCounts[t];
                    for (int j = start; j < end; j++)
                        local[(src[j] >> shift) & 0xFF]++;
                });
            }
            else
            {
                int* pData = dataIndirect;
                Parallel.For(0, numThreads, t =>
                {
                    int chunkSize = (len + numThreads - 1) / numThreads;
                    int start = t * chunkSize;
                    int end = Math.Min(start + chunkSize, len);
                    var local = localCounts[t];
                    for (int j = start; j < end; j++)
                        local[(pData[src[j]] >> shift) & 0xFF]++;
                });
            }

            int prefix = 0;
            for (int b = 0; b < BucketCount; b++)
            {
                int total = 0;
                for (int t = 0; t < numThreads; t++)
                    total += localCounts[t][b];
                counts[b] = prefix;
                prefix += total;
            }

            int[] globalCounts = new int[BucketCount];
            Marshal.Copy((IntPtr)counts, globalCounts, 0, BucketCount);

            int[][] threadOffsets = new int[numThreads][];
            for (int t = 0; t < numThreads; t++)
            {
                var offsets = new int[BucketCount];
                Array.Copy(globalCounts, offsets, BucketCount);
                for (int pt = 0; pt < t; pt++)
                {
                    var prevLocal = localCounts[pt];
                    for (int b = 0; b < BucketCount; b++)
                        offsets[b] += prevLocal[b];
                }
                threadOffsets[t] = offsets;
            }

            int totalLen = len;

            if (dataIndirect == null)
            {
                Parallel.For(0, numThreads, t =>
                {
                    int chunkSize = (totalLen + numThreads - 1) / numThreads;
                    int start = t * chunkSize;
                    int end = Math.Min(start + chunkSize, totalLen);
                    var offsets = threadOffsets[t];
                    for (int j = start; j < end; j++)
                    {
                        int val = src[j];
                        dst[offsets[(val >> shift) & 0xFF]++] = val;
                    }
                });
            }
            else
            {
                int* pData = dataIndirect;
                Parallel.For(0, numThreads, t =>
                {
                    int chunkSize = (totalLen + numThreads - 1) / numThreads;
                    int start = t * chunkSize;
                    int end = Math.Min(start + chunkSize, totalLen);
                    var offsets = threadOffsets[t];
                    for (int j = start; j < end; j++)
                    {
                        int idx = src[j];
                        dst[offsets[(pData[idx] >> shift) & 0xFF]++] = idx;
                    }
                });
            }
        }

        private static int ComputeThreadCount(int length)
        {
            if (length <= 1_000_000) return 1;
            if (length < 5_000_000) return 2;
            return Math.Max(2, Environment.ProcessorCount / 2);
        }
    }
}
