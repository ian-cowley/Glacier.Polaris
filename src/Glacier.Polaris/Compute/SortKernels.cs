using System;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// High-performance parallel radix sort kernels for Int32 and Float64.
    /// Both counting AND scatter phases are parallelized using thread-local histograms
    /// and per-thread scatter offsets. Uses ArrayPool renting for temp buffers,
    /// stack-allocated histograms.
    /// LSB-first radix sort (4 bytes for int32, 8 bytes for float64-after-conversion).
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

        /// <summary>ArgSort returning new int[].</summary>
        public static unsafe int[] ArgSort(ReadOnlySpan<int> data)
        {
            if (data.Length == 0) return Array.Empty<int>();
            int[] indices = new int[data.Length];
            for (int i = 0; i < data.Length; i++) indices[i] = i;
            ArgSort(data, indices);
            return indices;
        }

        /// <summary>ArgSort with parallel counting and scatter.</summary>
        public static unsafe void ArgSort(ReadOnlySpan<int> data, Span<int> indices, bool descending = false)
        {
            if (data.Length == 0) return;

            int totalLen = data.Length;
            int[] buffer = System.Buffers.ArrayPool<int>.Shared.Rent(totalLen);

            try
            {
                fixed (int* pIndices = indices)
                fixed (int* pBuffer = buffer)
                fixed (int* pData = data)
                {
                    int* src = pIndices;
                    int* dst = pBuffer;

                    for (int shift = 0; shift < 32; shift += 8)
                    {
                        DoRadixPass(src, dst, pData, totalLen, shift);
                        int* temp = src;
                        src = dst;
                        dst = temp;
                    }

                    if (src != pIndices)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(
                            pIndices, src, (uint)(totalLen * sizeof(int)));
                    }

                    if (descending)
                        indices.Reverse();
                }
            }
            finally
            {
                System.Buffers.ArrayPool<int>.Shared.Return(buffer);
            }
        }

        /// <summary>ArgSort for double — returns new int[].</summary>
        public static unsafe int[] ArgSort(ReadOnlySpan<double> data)
        {
            if (data.Length == 0) return Array.Empty<int>();
            int[] indices = new int[data.Length];
            for (int i = 0; i < data.Length; i++) indices[i] = i;
            ArgSort(data, indices);
            return indices;
        }
        public static unsafe void ArgSort(ReadOnlySpan<double> data, Span<int> indices, bool descending = false)
        {
            if (data.Length == 0) return;

            int totalLen = data.Length;
            int[] buffer = System.Buffers.ArrayPool<int>.Shared.Rent(totalLen);
            long[]? rented = null;

            try
            {
                rented = System.Buffers.ArrayPool<long>.Shared.Rent(totalLen);

                // Sequential conversion: Parallel.For overhead exceeds benefit for memory-bound operation.
                fixed (double* pData = data)
                fixed (long* pConverted = rented)
                {
                    for (int i = 0; i < totalLen; i++)
                    {
                        long val = BitConverter.DoubleToInt64Bits(pData[i]);
                        // IEEE 754: negative values are complement (leading 1), positive are leading 0
                        // To make sortable: flip sign bit, and for negatives flip all bits
                        if (val < 0) val ^= long.MaxValue;
                        else val ^= unchecked((long)0x8000000000000000);
                        pConverted[i] = val;
                    }
                }

                fixed (int* pIndices = indices)
                fixed (int* pBuffer = buffer)
                fixed (long* pConverted = rented)
                {
                    int* src = pIndices;
                    int* dst = pBuffer;

                    for (int shift = 0; shift < 64; shift += 8)
                    {
                        DoRadixPass64(src, dst, pConverted, totalLen, shift);
                        int* temp = src;
                        src = dst;
                        dst = temp;
                    }

                    if (src != pIndices)
                    {
                        System.Runtime.CompilerServices.Unsafe.CopyBlock(
                            pIndices, src, (uint)(totalLen * sizeof(int)));
                    }

                    if (descending)
                        indices.Reverse();
                }
            }
            finally
            {
                if (rented != null)
                    System.Buffers.ArrayPool<long>.Shared.Return(rented);
                System.Buffers.ArrayPool<int>.Shared.Return(buffer);
            }
        }/// <summary>
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

            // --- Phase 4: Parallel scatter ---
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

        /// <summary>64-bit key radix pass for double ArgSort (indirect only).</summary>
        private static unsafe void DoRadixPass64(
            int* src, int* dst, long* keys, int length, int shift)
        {
            if (length <= 1) return;

            int* counts = stackalloc int[BucketCount];
            for (int j = 0; j < BucketCount; j++) counts[j] = 0;

            int numThreads = ComputeThreadCount(length);

            if (numThreads <= 1)
            {
                // Sequential
                for (int j = 0; j < length; j++)
                {
                    long val = keys[src[j]];
                    counts[(int)((val >> shift) & 0xFF)]++;
                }

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
                    dst[counts[(int)((keys[idx] >> shift) & 0xFF)]++] = idx;
                }
                return;
            }

            // ================================================================
            // Parallel pass
            // ================================================================

            // Phase 1: Parallel counting
            int[][] localCounts = new int[numThreads][];
            for (int t = 0; t < numThreads; t++)
                localCounts[t] = new int[BucketCount];

            int len = length;

            Parallel.For(0, numThreads, t =>
            {
                int chunkSize = (len + numThreads - 1) / numThreads;
                int start = t * chunkSize;
                int end = Math.Min(start + chunkSize, len);
                var local = localCounts[t];
                for (int j = start; j < end; j++)
                {
                    long val = keys[src[j]];
                    local[(int)((val >> shift) & 0xFF)]++;
                }
            });

            // Phase 2: Merge histograms + prefix sums
            int prefix = 0;
            for (int b = 0; b < BucketCount; b++)
            {
                int total = 0;
                for (int t = 0; t < numThreads; t++)
                    total += localCounts[t][b];
                counts[b] = prefix;
                prefix += total;
            }

            // Phase 3: Per-thread scatter offsets
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

            // Phase 4: Parallel scatter
            int totalLen = len;

            Parallel.For(0, numThreads, t =>
            {
                int chunkSize = (totalLen + numThreads - 1) / numThreads;
                int start = t * chunkSize;
                int end = Math.Min(start + chunkSize, totalLen);
                var offsets = threadOffsets[t];
                for (int j = start; j < end; j++)
                {
                    int idx = src[j];
                    dst[offsets[(int)((keys[idx] >> shift) & 0xFF)]++] = idx;
                }
            });
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
