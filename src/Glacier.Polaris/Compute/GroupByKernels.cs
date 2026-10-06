using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Implements multi-column GroupBy kernels.
    /// </summary>
    public static partial class GroupByKernels
    {
        private const int Partitions = 64;

        /// <summary>
        /// Groups rows by key columns. Uses sort-based grouping when there's a single
        /// Int32 or Float64 key column (fast radix sort path), falling back to the
        /// hash-based partitioned approach for multi-column or string keys.
        /// </summary>
        public static List<List<int>> GroupBy(params ISeries[] columns)
        {
            if (columns.Length == 0) return new List<List<int>>();

            // Use sort-based grouping for single numeric key columns (most common case).
            // The radix sort is O(n) per byte, and the linear group boundary scan is O(n),
            // which is dramatically faster than the hash-based approach for low-cardinality keys.
            if (columns.Length == 1 && (columns[0] is Int32Series || columns[0] is Float64Series))
                return GroupBySortBased(columns);

            return GroupByHashBased(columns);
        }

        private static unsafe void ComputeHashes(ISeries[] columns, Span<long> hashes)
        {
            hashes.Fill(17);
            int length = hashes.Length;

            foreach (var col in columns)
            {
                if (col is Int32Series i32)
                {
                    fixed (long* pHashes = hashes)
                    fixed (int* pVals = i32.Memory.Span)
                    {
                        long* h = pHashes;
                        int* v = pVals;

                        int i = 0;
                        if (Vector256.IsHardwareAccelerated && length >= Vector256<int>.Count)
                        {
                            int step = Vector256<int>.Count;
                            var prime = Vector256.Create(31L);

                            for (; i <= length - step; i += step)
                            {
                                var vHashesL = Vector256.LoadUnsafe(ref Unsafe.AsRef<long>(h + i));
                                var vHashesU = Vector256.LoadUnsafe(ref Unsafe.AsRef<long>(h + i + 4));

                                var vInts = Vector256.LoadUnsafe(ref Unsafe.AsRef<int>(v + i));
                                var (vWidenL, vWidenU) = Vector256.Widen(vInts);

                                vHashesL = vHashesL * prime;
                                vHashesU = vHashesU * prime;

                                vHashesL = vHashesL + vWidenL;
                                vHashesU = vHashesU + vWidenU;

                                vHashesL.StoreUnsafe(ref Unsafe.AsRef<long>(h + i));
                                vHashesU.StoreUnsafe(ref Unsafe.AsRef<long>(h + i + 4));
                            }
                        }
                        for (; i < length; i++)
                        {
                            h[i] = h[i] * 31 + v[i];
                        }
                    }
                }
                else if (col is Float64Series f64)
                {
                    fixed (long* pHashes = hashes)
                    fixed (double* pVals = f64.Memory.Span)
                    {
                        long* h = pHashes;
                        double* v = pVals;

                        int i = 0;
                        if (Vector256.IsHardwareAccelerated && length >= Vector256<long>.Count)
                        {
                            int step = Vector256<long>.Count;
                            var prime = Vector256.Create(31L);

                            for (; i <= length - step; i += step)
                            {
                                var vHashes = Vector256.LoadUnsafe(ref Unsafe.AsRef<long>(h + i));
                                var vDoubles = Vector256.LoadUnsafe(ref Unsafe.AsRef<double>(v + i));
                                var vLongs = Vector256.AsInt64(vDoubles);

                                vHashes = vHashes * prime;
                                vHashes = vHashes + vLongs;
                                vHashes.StoreUnsafe(ref Unsafe.AsRef<long>(h + i));
                            }
                        }
                        for (; i < length; i++)
                        {
                            h[i] = h[i] * 31 + BitConverter.DoubleToInt64Bits(v[i]);
                        }
                    }
                }
                else if (col is Utf8StringSeries u8)
                {
                    fixed (long* pHashes = hashes)
                    {
                        long* h = pHashes;
                        for (int i = 0; i < length; i++)
                        {
                            var s = u8.GetStringSpan(i);
                            long stringHash = 0;
                            if (s.Length > 0)
                            {
                                foreach (byte b in s) stringHash = (stringHash ^ b) * 16777619;
                            }
                            h[i] = h[i] * 31 + stringHash;
                        }
                    }
                }
            }
        }

        private static List<int>[] Bucketize(ReadOnlySpan<long> hashes, int count)
        {
            var buckets = new List<int>[Partitions];
            for (int i = 0; i < Partitions; i++) buckets[i] = new List<int>(count / Partitions + 1);

            for (int i = 0; i < count; i++)
            {
                long h = hashes[i];
                uint p = (uint)(h ^ (h >> 32));
                int partition = (int)(p % Partitions);
                buckets[partition].Add(i);
            }
            return buckets;
        }

        private static bool AreRowsEqual(int r1, int r2, ISeries[] columns)
        {
            foreach (var col in columns)
            {
                if (col is Int32Series i32)
                {
                    if (i32.Memory.Span[r1] != i32.Memory.Span[r2]) return false;
                }
                else if (col is Float64Series f64)
                {
                    if (f64.Memory.Span[r1] != f64.Memory.Span[r2]) return false;
                }
                else if (col is Utf8StringSeries u8)
                {
                    if (!u8.GetStringSpan(r1).SequenceEqual(u8.GetStringSpan(r2))) return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Sort-based grouping for low-cardinality keys.
        /// Sorts row indices by key values, then scans linearly for group boundaries.
        /// Much faster than hash-based grouping when number of unique keys is small (&lt; 100K).
        /// </summary>
        public static List<List<int>> GroupBySortBased(params ISeries[] columns)
        {
            if (columns.Length == 0) return new List<List<int>>();
            int rowCount = columns[0].Length;

            var indices = new int[rowCount];
            for (int i = 0; i < rowCount; i++) indices[i] = i;

            if (columns.Length == 1 && columns[0] is Int32Series i32)
            {
                SortKernels.ArgSort(i32.Memory.Span, indices);
            }
            else if (columns.Length == 1 && columns[0] is Float64Series f64)
            {
                SortKernels.ArgSort(f64.Memory.Span, indices);
            }
            else
            {
                Array.Sort(indices, (a, b) =>
                {
                    foreach (var col in columns)
                    {
                        int cmp;
                        if (col is Int32Series i32) cmp = i32.Memory.Span[a].CompareTo(i32.Memory.Span[b]);
                        else if (col is Float64Series f64) cmp = f64.Memory.Span[a].CompareTo(f64.Memory.Span[b]);
                        else if (col is Utf8StringSeries u8)
                        {
                            cmp = u8.GetStringSpan(a).SequenceCompareTo(u8.GetStringSpan(b));
                        }
                        else cmp = 0;
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                });
            }

            var groups = new List<List<int>>();
            int groupStart = 0;
            for (int i = 0; i < rowCount; i++)
            {
                if (i == rowCount - 1 || !RowsEqual(indices[i], indices[i + 1], columns))
                {
                    var group = new List<int>(i - groupStart + 1);
                    for (int j = groupStart; j <= i; j++)
                        group.Add(indices[j]);
                    groups.Add(group);
                    groupStart = i + 1;
                }
            }

            return groups;
        }

        private static bool RowsEqual(int r1, int r2, ISeries[] columns)
        {
            foreach (var col in columns)
            {
                if (col is Int32Series i32)
                {
                    if (i32.Memory.Span[r1] != i32.Memory.Span[r2]) return false;
                }
                else if (col is Float64Series f64)
                {
                    if (f64.Memory.Span[r1] != f64.Memory.Span[r2]) return false;
                }
                else if (col is Utf8StringSeries u8)
                {
                    if (!u8.GetStringSpan(r1).SequenceEqual(u8.GetStringSpan(r2))) return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Hash-based partitioned GroupBy for multi-column or string keys.
        /// </summary>
        private static List<List<int>> GroupByHashBased(ISeries[] columns)
        {
            if (columns.Length == 0) return new List<List<int>>();

            int rowCount = columns[0].Length;
            long[] hashes = new long[rowCount];
            ComputeHashes(columns, hashes);

            var buckets = Bucketize(hashes, rowCount);

            var globalGroups = new System.Collections.Concurrent.ConcurrentBag<List<List<int>>>();

            System.Threading.Tasks.Parallel.For(0, Partitions, new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1) }, p =>
            {
                var bucket = buckets[p];
                if (bucket.Count == 0) return;

                var localMap = new Dictionary<long, object>();

                foreach (int rowIdx in bucket)
                {
                    long hash = hashes[rowIdx];
                    if (!localMap.TryGetValue(hash, out var obj))
                    {
                        var list = new List<int> { rowIdx };
                        localMap[hash] = list;
                    }
                    else
                    {
                        if (obj is List<int> singleGroup)
                        {
                            if (AreRowsEqual(rowIdx, singleGroup[0], columns))
                            {
                                singleGroup.Add(rowIdx);
                            }
                            else
                            {
                                var newGroup = new List<int> { rowIdx };
                                var collisionList = new List<List<int>> { singleGroup, newGroup };
                                localMap[hash] = collisionList;
                            }
                        }
                        else if (obj is List<List<int>> multipleGroups)
                        {
                            bool found = false;
                            foreach (var group in multipleGroups)
                            {
                                if (AreRowsEqual(rowIdx, group[0], columns))
                                {
                                    group.Add(rowIdx);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                multipleGroups.Add(new List<int> { rowIdx });
                            }
                        }
                    }
                }

                var threadGroups = new List<List<int>>(localMap.Count);
                foreach (var kvp in localMap)
                {
                    if (kvp.Value is List<int> single) threadGroups.Add(single);
                    else if (kvp.Value is List<List<int>> multiple) threadGroups.AddRange(multiple);
                }
                globalGroups.Add(threadGroups);
            });

            var result = new List<List<int>>();
            foreach (var bag in globalGroups)
            {
                result.AddRange(bag);
            }

            result.Sort((a, b) => a[0].CompareTo(b[0]));
            return result;
        }
    }
}
