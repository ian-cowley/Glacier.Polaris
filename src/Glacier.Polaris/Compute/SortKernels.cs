using System;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// High-performance radix sort kernels for Int32 and Float64.
    /// Uses sequential 8-bit packed-ulong radix for Int32 ArgSort and
    /// Array.Sort with struct LongIndexPair for Float64 ArgSort (corrected IEEE 754 transform).
    /// </summary>
    public static class SortKernels
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
                        // Swap src and dst manually (can't use tuple deconstruction with int*)
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
                        // Zero out counts for next pass
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
                        // Zero out counts for next pass
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

        /// <summary>ArgSort for Float64 — returns new int[]. Parallel 8-bit LSD radix on parallel arrays.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe int[] ArgSort(ReadOnlySpan<double> data)
        {
            if (data.Length == 0) return Array.Empty<int>();
            int n = data.Length;

            int[] indices = new int[n];
            if (n <= 2_000_000)
            {
                ArgSortFloat64SingleThreaded8BitSeparate(data, indices, isSequential: true);
                return indices;
            }

            int[] indicesBuf = System.Buffers.ArrayPool<int>.Shared.Rent(n);
            long[] keys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            long[] keysBuf = System.Buffers.ArrayPool<long>.Shared.Rent(n);

            try
            {
                // Vectorized and parallelized key transform step
                ConvertDoublesToSortableLongs(data, keys);

                // Initialise indices
                for (int i = 0; i < n; i++) indices[i] = i;

                fixed (int* pIdx = indices)
                fixed (int* pIdxBuf = indicesBuf)
                fixed (long* pKeys = keys)
                fixed (long* pKeysBuf = keysBuf)
                {
                    int* srcIdx = pIdx;
                    int* dstIdx = pIdxBuf;
                    long* srcKeys = pKeys;
                    long* dstKeys = pKeysBuf;

                    // 8 passes × 8 bits = 64-bit key
                    for (int shift = 0; shift < 64; shift += 8)
                    {
                        DoRadixPass64_8bit_Parallel(srcIdx, dstIdx, srcKeys, dstKeys, n, shift);

                        // Swap pointers
                        int* tIdx = srcIdx; srcIdx = dstIdx; dstIdx = tIdx;
                        long* tKeys = srcKeys; srcKeys = dstKeys; dstKeys = tKeys;
                    }

                    // After 8 passes (even number), the result is back in pIdx.
                    if (srcIdx != pIdx)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pIdx, srcIdx, (uint)(n * sizeof(int)));
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<int>.Shared.Return(indicesBuf);
                System.Buffers.ArrayPool<long>.Shared.Return(keys);
                System.Buffers.ArrayPool<long>.Shared.Return(keysBuf);
            }

            return indices;
        }

        /// <summary>In-place ArgSort for Float64 (re-sorts existing indices). Parallel 8-bit LSD radix on parallel arrays.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe void ArgSort(ReadOnlySpan<double> data, Span<int> indices, bool descending = false)
        {
            if (data.Length == 0) return;
            int n = data.Length;

            if (n <= 2_000_000)
            {
                ArgSortFloat64SingleThreaded8BitSeparate(data, indices, isSequential: false);
                if (descending)
                {
                    for (int i = 0, j = n - 1; i < j; i++, j--)
                    {
                        int t = indices[i]; indices[i] = indices[j]; indices[j] = t;
                    }
                }
                return;
            }

            int[] indicesBuf = System.Buffers.ArrayPool<int>.Shared.Rent(n);
            long[] keys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            long[] keysBuf = System.Buffers.ArrayPool<long>.Shared.Rent(n);

            try
            {
                // Vectorized and parallelized key transform step
                fixed (int* pIdx = indices)
                {
                    ConvertDoublesToSortableLongs(data, keys, pIdx);
                }

                fixed (int* pIdx = indices)
                fixed (int* pIdxBuf = indicesBuf)
                fixed (long* pKeys = keys)
                fixed (long* pKeysBuf = keysBuf)
                {
                    int* srcIdx = pIdx;
                    int* dstIdx = pIdxBuf;
                    long* srcKeys = pKeys;
                    long* dstKeys = pKeysBuf;

                    // 8 passes × 8 bits = 64-bit key (Cache-friendly!)
                    for (int shift = 0; shift < 64; shift += 8)
                    {
                        DoRadixPass64_8bit_Parallel(srcIdx, dstIdx, srcKeys, dstKeys, n, shift);

                        // Swap pointers
                        int* tIdx = srcIdx; srcIdx = dstIdx; dstIdx = tIdx;
                        long* tKeys = srcKeys; srcKeys = dstKeys; dstKeys = tKeys;
                    }

                    // After 8 passes (even number), the result is back in pIdx.
                    if (srcIdx != pIdx)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pIdx, srcIdx, (uint)(n * sizeof(int)));
                    }
                }

                if (descending)
                {
                    for (int i = 0, j = n - 1; i < j; i++, j--)
                    {
                        int t = indices[i]; indices[i] = indices[j]; indices[j] = t;
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<int>.Shared.Return(indicesBuf);
                System.Buffers.ArrayPool<long>.Shared.Return(keys);
                System.Buffers.ArrayPool<long>.Shared.Return(keysBuf);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void ArgSortFloat64SingleThreaded8BitSeparate(ReadOnlySpan<double> data, Span<int> indices, bool isSequential)
        {
            int n = data.Length;
            if (n <= 1) return;

            long[] keys = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            long[] keysBuf = System.Buffers.ArrayPool<long>.Shared.Rent(n);
            int[] indicesBuf = System.Buffers.ArrayPool<int>.Shared.Rent(n);

            try
            {
                fixed (int* pIndices = indices)
                {
                    ConvertDoublesToSortableLongs(data, keys, isSequential ? null : pIndices);
                }

                fixed (long* pKeys = keys)
                fixed (long* pKeysBuf = keysBuf)
                fixed (int* pIdx = indices)
                fixed (int* pIdxBuf = indicesBuf)
                {
                    long* srcKeys = pKeys;
                    long* dstKeys = pKeysBuf;
                    int* srcIdx = pIdx;
                    int* dstIdx = pIdxBuf;

                    const int bucketCount = 256;
                    int* counts = stackalloc int[bucketCount];

                    // =========================================================
                    // PASS 1 (Shift 0)
                    // =========================================================
                    for (int i = 0; i < bucketCount; i++) counts[i] = 0;

                    // UNROLLED COUNTING
                    int j = 0;
                    for (; j <= n - 4; j += 4)
                    {
                        counts[(int)(srcKeys[j] & 0xFF)]++;
                        counts[(int)(srcKeys[j + 1] & 0xFF)]++;
                        counts[(int)(srcKeys[j + 2] & 0xFF)]++;
                        counts[(int)(srcKeys[j + 3] & 0xFF)]++;
                    }
                    for (; j < n; j++) counts[(int)(srcKeys[j] & 0xFF)]++;

                    int offset = 0;
                    for (int i = 0; i < bucketCount; i++)
                    {
                        int c = counts[i];
                        counts[i] = offset;
                        offset += c;
                    }

                    // UNROLLED SCATTER
                    j = 0;
                    if (isSequential)
                    {
                        for (; j <= n - 4; j += 4)
                        {
                            long k0 = srcKeys[j];
                            long k1 = srcKeys[j + 1];
                            long k2 = srcKeys[j + 2];
                            long k3 = srcKeys[j + 3];

                            int b0 = (int)(k0 & 0xFF);
                            int b1 = (int)(k1 & 0xFF);
                            int b2 = (int)(k2 & 0xFF);
                            int b3 = (int)(k3 & 0xFF);

                            int p0 = counts[b0]++; dstKeys[p0] = k0; dstIdx[p0] = j;
                            int p1 = counts[b1]++; dstKeys[p1] = k1; dstIdx[p1] = j + 1;
                            int p2 = counts[b2]++; dstKeys[p2] = k2; dstIdx[p2] = j + 2;
                            int p3 = counts[b3]++; dstKeys[p3] = k3; dstIdx[p3] = j + 3;
                        }
                        for (; j < n; j++)
                        {
                            long key = srcKeys[j];
                            int pos = counts[(int)(key & 0xFF)]++;
                            dstKeys[pos] = key;
                            dstIdx[pos] = j;
                        }
                    }
                    else
                    {
                        for (; j <= n - 4; j += 4)
                        {
                            long k0 = srcKeys[j]; int i0 = srcIdx[j];
                            long k1 = srcKeys[j + 1]; int i1 = srcIdx[j + 1];
                            long k2 = srcKeys[j + 2]; int i2 = srcIdx[j + 2];
                            long k3 = srcKeys[j + 3]; int i3 = srcIdx[j + 3];

                            int b0 = (int)(k0 & 0xFF);
                            int b1 = (int)(k1 & 0xFF);
                            int b2 = (int)(k2 & 0xFF);
                            int b3 = (int)(k3 & 0xFF);

                            int p0 = counts[b0]++; dstKeys[p0] = k0; dstIdx[p0] = i0;
                            int p1 = counts[b1]++; dstKeys[p1] = k1; dstIdx[p1] = i1;
                            int p2 = counts[b2]++; dstKeys[p2] = k2; dstIdx[p2] = i2;
                            int p3 = counts[b3]++; dstKeys[p3] = k3; dstIdx[p3] = i3;
                        }
                        for (; j < n; j++)
                        {
                            long key = srcKeys[j];
                            int pos = counts[(int)(key & 0xFF)]++;
                            dstKeys[pos] = key;
                            dstIdx[pos] = srcIdx[j];
                        }
                    }

                    long* tKeys = srcKeys; srcKeys = dstKeys; dstKeys = tKeys;
                    int* tIdx = srcIdx; srcIdx = dstIdx; dstIdx = tIdx;

                    // =========================================================
                    // PASSES 2 to 8 (Shifts 8 to 56)
                    // =========================================================
                    for (int shift = 8; shift < 64; shift += 8)
                    {
                        for (int i = 0; i < bucketCount; i++) counts[i] = 0;

                        // UNROLLED COUNTING
                        j = 0;
                        for (; j <= n - 4; j += 4)
                        {
                            counts[(int)((srcKeys[j] >> shift) & 0xFF)]++;
                            counts[(int)((srcKeys[j + 1] >> shift) & 0xFF)]++;
                            counts[(int)((srcKeys[j + 2] >> shift) & 0xFF)]++;
                            counts[(int)((srcKeys[j + 3] >> shift) & 0xFF)]++;
                        }
                        for (; j < n; j++) counts[(int)((srcKeys[j] >> shift) & 0xFF)]++;

                        offset = 0;
                        for (int i = 0; i < bucketCount; i++)
                        {
                            int c = counts[i];
                            counts[i] = offset;
                            offset += c;
                        }

                        // UNROLLED SCATTER
                        j = 0;
                        for (; j <= n - 4; j += 4)
                        {
                            long k0 = srcKeys[j]; int i0 = srcIdx[j];
                            long k1 = srcKeys[j + 1]; int i1 = srcIdx[j + 1];
                            long k2 = srcKeys[j + 2]; int i2 = srcIdx[j + 2];
                            long k3 = srcKeys[j + 3]; int i3 = srcIdx[j + 3];

                            int b0 = (int)((k0 >> shift) & 0xFF);
                            int b1 = (int)((k1 >> shift) & 0xFF);
                            int b2 = (int)((k2 >> shift) & 0xFF);
                            int b3 = (int)((k3 >> shift) & 0xFF);

                            int p0 = counts[b0]++; dstKeys[p0] = k0; dstIdx[p0] = i0;
                            int p1 = counts[b1]++; dstKeys[p1] = k1; dstIdx[p1] = i1;
                            int p2 = counts[b2]++; dstKeys[p2] = k2; dstIdx[p2] = i2;
                            int p3 = counts[b3]++; dstKeys[p3] = k3; dstIdx[p3] = i3;
                        }
                        for (; j < n; j++)
                        {
                            long k = srcKeys[j];
                            int b = (int)((k >> shift) & 0xFF);
                            int pos = counts[b]++;
                            dstKeys[pos] = k;
                            dstIdx[pos] = srcIdx[j];
                        }

                        tKeys = srcKeys; srcKeys = dstKeys; dstKeys = tKeys;
                        tIdx = srcIdx; srcIdx = dstIdx; dstIdx = tIdx;
                    }

                    // After 8 passes (even number), the result is back in pIdx.
                    if (srcIdx != pIdx)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(pIdx, srcIdx, (uint)(n * sizeof(int)));
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<long>.Shared.Return(keys);
                System.Buffers.ArrayPool<long>.Shared.Return(keysBuf);
                System.Buffers.ArrayPool<int>.Shared.Return(indicesBuf);
            }
        }

        /// <summary>
        /// Multi-column sort via stable chained radix sorts in reverse order.
        /// Uses ArgSort for each column in reverse order (stable sort by last column first).
        /// </summary>
        public static int[] MultiColumnSort(DataFrame df, string[] columnNames, bool[] descending)
        {
            if (columnNames.Length == 0 || df.Columns.Count == 0) return Array.Empty<int>();
            int rowCount = df.Columns[0].Length;
            int[] indices = new int[rowCount];
            for (int i = 0; i < rowCount; i++) indices[i] = i;

            for (int i = columnNames.Length - 1; i >= 0; i--)
            {
                var col = df.GetColumn(columnNames[i]);
                bool desc = descending.Length > i && descending[i];
                if (col is Data.Int32Series intCol)
                    ArgSort(intCol.Memory.Span, indices, desc);
                else if (col is Data.Float64Series doubleCol)
                    ArgSort(doubleCol.Memory.Span, indices, desc);
                else if (col is Data.Utf8StringSeries u8)
                {
                    var comparer = new Utf8IndexComparer(u8);
                    indices = desc
                        ? indices.OrderByDescending(idx => idx, comparer).ToArray()
                        : indices.OrderBy(idx => idx, comparer).ToArray();
                }
            }
            return indices;
        }

        public static int[] TopK(DataFrame df, string[] columnNames, bool[] descending, int k)
        {
            if (k <= 0) return Array.Empty<int>();
            int rowCount = df.Columns[0].Length;
            if (k >= rowCount) return MultiColumnSort(df, columnNames, descending);
            var indices = MultiColumnSort(df, columnNames, descending);
            var result = new int[k];
            Array.Copy(indices, result, k);
            return result;
        }

        // =====================================================================
        //  Core radix pass implementations
        // =====================================================================

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
                // Sequential pass — no thread overhead
                if (dataIndirect == null)
                {
                    // Direct sort: value is src[j]
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
                    // Indirect sort via data pointer
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

            // ================================================================
            // Parallel pass
            // ================================================================

            // --- Phase 1: Parallel counting with thread-local histograms ---
            int[][] localCounts = new int[numThreads][];
            for (int t = 0; t < numThreads; t++)
                localCounts[t] = new int[BucketCount];

            int len = length;

            if (dataIndirect == null)
            {
                // Direct sort
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
                // Indirect sort (ArgSort)
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

            // --- Phase 2: Merge histograms + prefix sum ---
            int prefix = 0;
            for (int b = 0; b < BucketCount; b++)
            {
                int total = 0;
                for (int t = 0; t < numThreads; t++)
                    total += localCounts[t][b];
                counts[b] = prefix;
                prefix += total;
            }

            // --- Phase 3: Compute per-thread scatter offsets ---
            // For each thread t, for each bucket b:
            //   offset = global_prefix[b] + sum(localCounts[0..t-1][b])
            // This lets each thread write to disjoint regions of dst.
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

            // --- Phase 4: Parallel scatter
            int totalLen = len;

            if (dataIndirect == null)
            {
                // Direct sort
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
                // Indirect sort (ArgSort)
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

        /// <summary>8-bit parallel radix pass for 64-bit keys. Uses ArrayPool for thread-local 256 tables.</summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void DoRadixPass64_8bit_Parallel(
            int* srcIdx, int* dstIdx, long* srcKeys, long* dstKeys, int length, int shift)
        {
            if (length <= 1) return;

            int numThreads = ComputeThreadCount(length);

            if (numThreads <= 1)
            {
                // Highly optimized sequential 8-bit radix pass
                int* counts = stackalloc int[256];
                for (int i = 0; i < 256; i++) counts[i] = 0;

                for (int j = 0; j < length; j++)
                {
                    counts[(int)((srcKeys[j] >> shift) & 0xFF)]++;
                }

                int offset = 0;
                for (int j = 0; j < 256; j++)
                {
                    int c = counts[j];
                    counts[j] = offset;
                    offset += c;
                }

                for (int j = 0; j < length; j++)
                {
                    long key = srcKeys[j];
                    int bucket = (int)((key >> shift) & 0xFF);
                    int pos = counts[bucket]++;
                    dstKeys[pos] = key;
                    dstIdx[pos] = srcIdx[j];
                }
                return;
            }

            int chunkSize = (length + numThreads - 1) / numThreads;

            // Rent local count arrays for each thread
            int[][] localCounts = new int[numThreads][];
            for (int t = 0; t < numThreads; t++)
            {
                localCounts[t] = System.Buffers.ArrayPool<int>.Shared.Rent(256);
                Array.Clear(localCounts[t], 0, 256);
            }

            // Capture pointers as safe IntPtr to allow lambda capture
            IntPtr srcIdxPtr = (IntPtr)srcIdx;
            IntPtr dstIdxPtr = (IntPtr)dstIdx;
            IntPtr srcKeysPtr = (IntPtr)srcKeys;
            IntPtr dstKeysPtr = (IntPtr)dstKeys;

            try
            {
                // Phase 1: Parallel Counting (Linear read, highly predictable)
                Parallel.For(0, numThreads, t =>
                {
                    long* sKeys = (long*)srcKeysPtr;
                    int start = t * chunkSize;
                    int end = Math.Min(start + chunkSize, length);
                    int[] local = localCounts[t];
                    
                    fixed (int* pLocal = local)
                    {
                        for (int j = start; j < end; j++)
                        {
                            int bucket = (int)((sKeys[j] >> shift) & 0xFF);
                            pLocal[bucket]++;
                        }
                    }
                });

                // Phase 2: Prefix sums & Per-thread offsets
                int[][] threadOffsets = new int[numThreads][];
                for (int t = 0; t < numThreads; t++)
                {
                    threadOffsets[t] = System.Buffers.ArrayPool<int>.Shared.Rent(256);
                }

                try
                {
                    int prefix = 0;
                    for (int b = 0; b < 256; b++)
                    {
                        for (int t = 0; t < numThreads; t++)
                        {
                            threadOffsets[t][b] = prefix;
                            prefix += localCounts[t][b];
                        }
                    }

                    // Phase 3: Parallel Scatter (Linear read, Hot-Cache scattered write)
                    Parallel.For(0, numThreads, t =>
                    {
                        int* sIdx = (int*)srcIdxPtr;
                        int* dIdx = (int*)dstIdxPtr;
                        long* sKeys = (long*)srcKeysPtr;
                        long* dKeys = (long*)dstKeysPtr;

                        int start = t * chunkSize;
                        int end = Math.Min(start + chunkSize, length);
                        
                        // Use stackalloc for thread-local offsets to avoid array bounds checks inside the tight loop
                        int* offsets = stackalloc int[256];
                        fixed (int* pThreadOffsets = threadOffsets[t])
                        {
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(offsets, pThreadOffsets, 256 * sizeof(int));
                        }

                        for (int j = start; j < end; j++)
                        {
                            long key = sKeys[j];
                            int bucket = (int)((key >> shift) & 0xFF);
                            int pos = offsets[bucket]++;
                            
                            dKeys[pos] = key;
                            dIdx[pos] = sIdx[j];
                        }
                    });
                }
                finally
                {
                    for (int t = 0; t < numThreads; t++)
                        System.Buffers.ArrayPool<int>.Shared.Return(threadOffsets[t]);
                }
            }
            finally
            {
                for (int t = 0; t < numThreads; t++)
                    System.Buffers.ArrayPool<int>.Shared.Return(localCounts[t]);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe void ConvertDoublesToSortableLongs(
            ReadOnlySpan<double> data, Span<long> keys, int* pIdx = null)
        {
            int n = data.Length;
            int parallelThreshold = 128_000;

            fixed (double* pData = data)
            fixed (long* pKeys = keys)
            {
                if (n >= parallelThreshold)
                {
                    // Capture pointers as safe IntPtr to allow lambda capture
                    IntPtr dIntPtr = (IntPtr)pData;
                    IntPtr kIntPtr = (IntPtr)pKeys;
                    IntPtr idxIntPtr = (IntPtr)pIdx;

                    int numThreads = Environment.ProcessorCount;
                    int chunkSize = (n + numThreads - 1) / numThreads;

                    Parallel.For(0, numThreads, p =>
                    {
                        double* dPtr = (double*)dIntPtr;
                        long* kPtr = (long*)kIntPtr;
                        int* idxPtr = (int*)idxIntPtr;

                        int start = p * chunkSize;
                        int end = Math.Min(start + chunkSize, n);
                        if (start >= end) return;

                        if (idxPtr == null)
                        {
                            // Vectorized direct key mapping
                            int i = start;
                            if (Vector256.IsHardwareAccelerated && (end - start) >= Vector256<double>.Count)
                            {
                                int step = Vector256<double>.Count; // 4
                                var vZero = Vector256<long>.Zero;
                                var vSignBit = Vector256.Create(long.MinValue);
                                var vAllOnes = Vector256.Create(-1L);

                                for (; i <= end - step; i += step)
                                {
                                    var vDouble = Vector256.Load(dPtr + i);
                                    var vBits = vDouble.As<double, long>();
                                    var vNegMask = Vector256.LessThan(vBits, vZero);
                                    var vXorMask = Vector256.ConditionalSelect(vNegMask, vAllOnes, vSignBit);
                                    var vResult = vBits ^ vXorMask;
                                    Vector256.Store(vResult, kPtr + i);
                                }
                            }
                            // Trailing scalar
                            for (; i < end; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(dPtr[i]);
                                kPtr[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                        else
                        {
                            // Indirect key mapping (parallelized)
                            for (int i = start; i < end; i++)
                            {
                                long bits = BitConverter.DoubleToInt64Bits(dPtr[idxPtr[i]]);
                                kPtr[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                            }
                        }
                    });
                }
                else
                {
                    // Sequential mapping
                    if (pIdx == null)
                    {
                        int i = 0;
                        if (Vector256.IsHardwareAccelerated && n >= Vector256<double>.Count)
                        {
                            int step = Vector256<double>.Count;
                            var vZero = Vector256<long>.Zero;
                            var vSignBit = Vector256.Create(long.MinValue);
                            var vAllOnes = Vector256.Create(-1L);

                            for (; i <= n - step; i += step)
                            {
                                var vDouble = Vector256.Load(pData + i);
                                var vBits = vDouble.As<double, long>();
                                var vNegMask = Vector256.LessThan(vBits, vZero);
                                var vXorMask = Vector256.ConditionalSelect(vNegMask, vAllOnes, vSignBit);
                                var vResult = vBits ^ vXorMask;
                                Vector256.Store(vResult, pKeys + i);
                            }
                        }
                        for (; i < n; i++)
                        {
                            long bits = BitConverter.DoubleToInt64Bits(pData[i]);
                            pKeys[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                        }
                    }
                    else
                    {
                        for (int i = 0; i < n; i++)
                        {
                            long bits = BitConverter.DoubleToInt64Bits(pData[pIdx[i]]);
                            pKeys[i] = bits < 0 ? ~bits : bits ^ long.MinValue;
                        }
                    }
                }
            }
        }

        private static int ComputeThreadCount(int length)
        {
            // Sequential for <= 1M (fits L3, avoids parallel overhead of thread-local histograms + barriers)
            if (length <= 1_000_000) return 1;
            if (length < 5_000_000) return 2;
            return Math.Max(2, Environment.ProcessorCount / 2);
        }
        private class Utf8IndexComparer : IComparer<int>
        {
            private readonly Data.Utf8StringSeries _series;
            public Utf8IndexComparer(Data.Utf8StringSeries series) => _series = series;
            public int Compare(int x, int y)
            {
                var spanX = _series.GetStringSpan(x);
                var spanY = _series.GetStringSpan(y);
                return spanX.SequenceCompareTo(spanY);
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

        /// <summary>
        /// ArgSort using Array.Sort with double-to-long conversion for Float64.
        /// </summary>
        public static int[] ArgSortSystem(ReadOnlySpan<double> data, bool descending = false)
        {
            int n = data.Length;
            var idx = new int[n];
            var keys = new long[n];
            for (int i = 0; i < n; i++)
            {
                long val = BitConverter.DoubleToInt64Bits(data[i]);
                if (val < 0) val ^= long.MaxValue;
                else val ^= unchecked((long)0x8000000000000000);
                keys[i] = val;
            }
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
        /// <summary>Fast 16-bit radix pass for 64-bit keys. Uses heap-allocated 65536 bucket table.</summary>
        private static unsafe void DoRadixPass64_16bit(
            int* src, int* dst, long* keys, int length, int shift)
        {
            if (length <= 1) return;

            int[] countsArr = System.Buffers.ArrayPool<int>.Shared.Rent(65536);
            countsArr.AsSpan(0, 65536).Clear();

            try
            {
                fixed (int* counts = countsArr)
                {
                    // Counting
                    for (int j = 0; j < length; j++)
                        counts[(int)((keys[src[j]] >> shift) & 0xFFFF)]++;

                    // Prefix sum
                    int offset = 0;
                    for (int j = 0; j < 65536; j++)
                    {
                        int c = counts[j];
                        counts[j] = offset;
                        offset += c;
                    }

                    // Scatter
                    for (int j = 0; j < length; j++)
                    {
                        int idx = src[j];
                        dst[counts[(int)((keys[idx] >> shift) & 0xFFFF)]++] = idx;
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<int>.Shared.Return(countsArr);
            }
        }
        [StructLayout(LayoutKind.Sequential)]
        private readonly struct LongIndexPair : IComparable<LongIndexPair>
        {
            public readonly long Key;
            public readonly int Index;
            public LongIndexPair(long key, int index) { Key = key; Index = index; }
            public int CompareTo(LongIndexPair other) => Key.CompareTo(other.Key);
        }
    }
}
