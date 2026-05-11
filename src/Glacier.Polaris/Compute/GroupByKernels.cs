using System;
using System.Collections.Generic;
using System.Runtime.Intrinsics;
using System.Runtime.CompilerServices;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Implements multi-column GroupBy kernels.
    /// </summary>
    public static class GroupByKernels
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
                            int step = Vector256<int>.Count; // 8 ints, 8 longs
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
                            int step = Vector256<long>.Count; // 4 longs, 4 doubles
                            var prime = Vector256.Create(31L);

                            for (; i <= length - step; i += step)
                            {
                                var vHashes = Vector256.LoadUnsafe(ref Unsafe.AsRef<long>(h + i));
                                var vDoubles = Vector256.LoadUnsafe(ref Unsafe.AsRef<double>(v + i));
                                var vLongs = Vector256.AsInt64(vDoubles); // Bitwise representation

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
                // Add string hashing if needed, fallback for now
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
                                // FNV-1a style or simple additive
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
                // Math.Abs can fail for long.MinValue
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

        public static DataFrame Aggregate(DataFrame source, string[] groupKeys, List<List<int>> groups, params (string col, string agg)[] aggregations)
        {
            int groupCount = groups.Count;
            var resultColumns = new List<ISeries>();

            // Add groupby key columns (first value from each group)
            foreach (var keyName in groupKeys)
            {
                var col = source.GetColumn(keyName);
                if (col is Int32Series i32Key)
                {
                    var keyCol = new Int32Series(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            keyCol.Memory.Span[i] = i32Key.Memory.Span[groups[i][0]];
                    resultColumns.Add(keyCol);
                }
                else if (col is Utf8StringSeries u8Key)
                {
                    var keyCol = new Utf8StringSeries(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            u8Key.Take(keyCol, groups[i][0], i);
                    resultColumns.Add(keyCol);
                }
                else if (col is Float64Series f64Key)
                {
                    var keyCol = new Float64Series(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            f64Key.Take(keyCol, groups[i][0], i);
                    resultColumns.Add(keyCol);
                }
                else if (col is Int64Series i64Key)
                {
                    var keyCol = new Int64Series(keyName, groupCount);
                    for (int i = 0; i < groupCount; i++)
                        if (groups[i].Count > 0)
                            i64Key.Take(keyCol, groups[i][0], i);
                    resultColumns.Add(keyCol);
                }
            }

            // Delegate each aggregation to the ISeries Aggregate method for proper type handling
            foreach (var (colName, aggType) in aggregations)
            {
                var sourceCol = source.GetColumn(colName);
                var resultCol = Aggregate(sourceCol, groups, aggType);
                resultColumns.Add(resultCol);
            }

            return new DataFrame(resultColumns);
        }

        public static ISeries Aggregate(ISeries source, List<List<int>> groups, string aggType)
        {
            int groupCount = groups.Count;
            var resultCol = CreateResultColumn(source.Name, aggType, groupCount, source);
            var options = new System.Threading.Tasks.ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1) };

            if (aggType == "sum" && source is Int32Series i32)
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    int sum = 0;
                    foreach (int idx in groups[i]) sum += i32.Memory.Span[idx];
                    dest.Memory.Span[i] = sum;
                });
            }
            else if (aggType == "sum" && source is Float64Series f64)
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    foreach (int idx in groups[i]) sum += f64.Memory.Span[idx];
                    dest.Memory.Span[i] = sum;
                });
            }
            else if (aggType == "count")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    dest.Memory.Span[i] = groups[i].Count;
                });
            }
            else if (aggType == "min")
            {
                if (resultCol is Int32Series i32Dest && source is Int32Series i32Source)
                {
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        int min = int.MaxValue;
                        foreach (int idx in groups[i]) if (i32Source.Memory.Span[idx] < min) min = i32Source.Memory.Span[idx];
                        i32Dest.Memory.Span[i] = min;
                    });
                }
                else
                {
                    var dest = (Float64Series)resultCol;
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        double min = double.MaxValue;
                        foreach (int idx in groups[i])
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val < min) min = val;
                        }
                        dest.Memory.Span[i] = min;
                    });
                }
            }
            else if (aggType == "max")
            {
                if (resultCol is Int32Series i32Dest && source is Int32Series i32Source)
                {
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        int max = int.MinValue;
                        foreach (int idx in groups[i]) if (i32Source.Memory.Span[idx] > max) max = i32Source.Memory.Span[idx];
                        i32Dest.Memory.Span[i] = max;
                    });
                }
                else
                {
                    var dest = (Float64Series)resultCol;
                    System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                    {
                        double max = double.MinValue;
                        foreach (int idx in groups[i])
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val > max) max = val;
                        }
                        dest.Memory.Span[i] = max;
                    });
                }
            }
            else if (aggType == "mean" || aggType == "avg")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    int count = 0;
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        sum += val;
                        count++;
                    }
                    dest.Memory.Span[i] = count > 0 ? sum / count : 0;
                });
            }
            else if (aggType == "implode")
            {
                // Implode: collect all values per group into a ListSeries
                var offsets = new int[groupCount + 1];
                int totalVals = 0;
                for (int i = 0; i < groupCount; i++)
                {
                    offsets[i] = totalVals;
                    totalVals += groups[i].Count;
                }
                offsets[groupCount] = totalVals;

                // Create inner values array
                // For now, create as Float64Series since we need a concrete type
                var values = new Float64Series("values", totalVals);
                int pos = 0;
                for (int i = 0; i < groupCount; i++)
                {
                    foreach (int idx in groups[i])
                    {
                        values.Memory.Span[pos++] = GetValueAsDouble(source, idx);
                    }
                }

                // The offsets Int32Series
                var offsetsCol = new Int32Series(source.Name + "_offsets", offsets);

                var listSeries = new ListSeries(source.Name + "_implode", offsetsCol, values);
                return listSeries;
            }
            else if (aggType == "first")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    dest.Memory.Span[i] = groups[i].Count > 0 ? GetValueAsDouble(source, groups[i][0]) : 0;
                });
            }
            else if (aggType == "std")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    double sumSq = 0;
                    int cnt = 0;
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        sum += val;
                        sumSq += val * val;
                        cnt++;
                    }
                    if (cnt > 1)
                    {
                        double variance = (sumSq - (sum * sum) / cnt) / (cnt - 1);
                        dest.Memory.Span[i] = Math.Sqrt(Math.Max(0, variance));
                    }
                    else
                        dest.Memory.Span[i] = 0;
                });
            }
            else if (aggType == "var")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double sum = 0;
                    double sumSq = 0;
                    int cnt = 0;
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        sum += val;
                        sumSq += val * val;
                        cnt++;
                    }
                    if (cnt > 1)
                    {
                        double variance = (sumSq - (sum * sum) / cnt) / (cnt - 1);
                        dest.Memory.Span[i] = Math.Max(0, variance);
                    }
                    else
                        dest.Memory.Span[i] = 0;
                });
            }
            else if (aggType == "median")
            {
                var dest = (Float64Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    var vals = new double[groups[i].Count];
                    int j = 0;
                    foreach (int idx in groups[i])
                        vals[j++] = GetValueAsDouble(source, idx);
                    Array.Sort(vals);
                    if (vals.Length % 2 == 0)
                        dest.Memory.Span[i] = (vals[vals.Length / 2 - 1] + vals[vals.Length / 2]) / 2.0;
                    else
                        dest.Memory.Span[i] = vals[vals.Length / 2];
                });
            }
            else if (aggType == "null_count")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    int nulls = 0;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsNull(idx))
                            nulls++;
                    }
                    dest.Memory.Span[i] = nulls;
                });
            }
            else if (aggType == "arg_min")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double min = double.MaxValue;
                    int argMin = -1;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsValid(idx))
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val < min)
                            {
                                min = val;
                                argMin = idx;
                            }
                        }
                    }
                    dest.Memory.Span[i] = argMin >= 0 ? argMin : -1;
                });
            }
            else if (aggType == "arg_max")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    double max = double.MinValue;
                    int argMax = -1;
                    foreach (int idx in groups[i])
                    {
                        if (source.ValidityMask.IsValid(idx))
                        {
                            double val = GetValueAsDouble(source, idx);
                            if (val > max)
                            {
                                max = val;
                                argMax = idx;
                            }
                        }
                    }
                    dest.Memory.Span[i] = argMax >= 0 ? argMax : -1;
                });
            }
            else if (aggType == "nunique")
            {
                var dest = (Int32Series)resultCol;
                System.Threading.Tasks.Parallel.For(0, groupCount, options, i =>
                {
                    var seen = new HashSet<double>();
                    foreach (int idx in groups[i])
                    {
                        double val = GetValueAsDouble(source, idx);
                        if (!seen.Contains(val)) seen.Add(val);
                    }
                    dest.Memory.Span[i] = seen.Count;
                });
            }
            // Add more as needed

            return resultCol;
        }

        private static double GetValueAsDouble(ISeries s, int idx)
        {
            if (s is Int32Series i32) return i32.Memory.Span[idx];
            if (s is Float64Series f64) return f64.Memory.Span[idx];
            if (s is Int64Series i64) return i64.Memory.Span[idx];
            var val = s.Get(idx);
            return val != null ? Convert.ToDouble(val) : 0;
        }

        private static ISeries CreateResultColumn(string name, string agg, int length, ISeries? source = null)
        {
            if (agg == "sum")
            {
                if (source != null && (source is Float64Series || source is Int64Series))
                    return new Float64Series($"{name}_{agg}", length);
                return new Int32Series($"{name}_{agg}", length);
            }
            if (agg == "count" || agg == "nunique" || agg == "null_count" || agg == "arg_min" || agg == "arg_max")
                return new Int32Series($"{name}_{agg}", length);
            if (agg == "implode") return new ListSeries($"{name}_{agg}", new Int32Series($"{name}_offsets", length + 1), new Float64Series($"{name}_values", 0));
            // For type-preserving aggregations (min, max, first), preserve the source series type when possible
            if (source != null && (agg == "min" || agg == "max" || agg == "first"))
            {
                if (source is Int32Series) return new Int32Series($"{name}_{agg}", length);
                if (source is Int64Series) return new Int64Series($"{name}_{agg}", length);
                // For all other types, fall through to Float64Series default
            }
            return new Float64Series($"{name}_{agg}", length);
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

            // Use the first column as the primary sort key
            var indices = new int[rowCount];
            for (int i = 0; i < rowCount; i++) indices[i] = i;

            if (columns.Length == 1 && columns[0] is Int32Series i32)
            {
                // Fast path: radix sort by Int32 key
                SortKernels.ArgSort(i32.Memory.Span, indices);
            }
            else if (columns.Length == 1 && columns[0] is Float64Series f64)
            {
                SortKernels.ArgSort(f64.Memory.Span, indices);
            }
            else
            {
                // Multi-column sort using comparer
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

            // Scan for group boundaries
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

        /// <summary>Compare two rows by index for equality across all key columns.</summary>
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
        public static DataFrame GroupBySumInt32(Int32Series keys, Int32Series values)
        {
            return GroupBySumInt32Fast(keys, values);
        }
        /// <summary>
        /// Single-pass GroupBy + Sum for Int32 keys and Float64 values.
        /// </summary>
        public static DataFrame GroupBySumF64(Int32Series keys, Float64Series values)
        {
            int rowCount = keys.Length;

            var indices = SortKernels.ArgSort(keys.Memory.Span);
            var uniqueKeys = new List<int>();
            uniqueKeys.Add(keys.Memory.Span[indices[0]]);
            for (int i = 1; i < indices.Length; i++)
            {
                if (keys.Memory.Span[indices[i]] != keys.Memory.Span[indices[i - 1]])
                    uniqueKeys.Add(keys.Memory.Span[indices[i]]);
            }

            int groupCount = uniqueKeys.Count;
            var keyCol = new Int32Series("key", groupCount);
            var sumCol = new Float64Series("a_sum", groupCount);

            var keyToIndex = new Dictionary<int, int>(groupCount);
            for (int i = 0; i < groupCount; i++)
            {
                keyToIndex[uniqueKeys[i]] = i;
                keyCol.Memory.Span[i] = uniqueKeys[i];
                sumCol.Memory.Span[i] = 0;
            }

            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            for (int i = 0; i < rowCount; i++)
            {
                if (keyToIndex.TryGetValue(keySpan[i], out int idx))
                    sumCol.Memory.Span[idx] += valSpan[i];
            }

            return new DataFrame(new ISeries[] { keyCol, sumCol });
        }
        public static DataFrame GroupByMeanF64(Int32Series keys, Float64Series values)
        {
            return GroupByMeanF64Fast(keys, values);
        }        /// <summary>
                 /// Single-pass hash-based GroupBy + Sum for Int32 keys and Int32 values.
                 /// Uses CollectionsMarshal.GetValueRefOrAddDefault for zero-overhead dictionary
                 /// accumulation. Avoids sort entirely. ~2-5× faster than sort-based for typical workloads.
                 /// </summary>
        public static DataFrame GroupBySumInt32Fast(Int32Series keys, Int32Series values)
        {
            int rowCount = keys.Length;
            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            var sums = new Dictionary<int, int>();

            for (int i = 0; i < rowCount; i++)
            {
                ref int sum = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(sums, keySpan[i], out _);
                sum += valSpan[i];
            }

            int groupCount = sums.Count;
            var keyCol = new Int32Series("key", groupCount);
            var sumCol = new Int32Series("a_sum", groupCount);
            int pos = 0;
            foreach (var kvp in sums)
            {
                keyCol.Memory.Span[pos] = kvp.Key;
                sumCol.Memory.Span[pos] = kvp.Value;
                pos++;
            }
            return new DataFrame(new ISeries[] { keyCol, sumCol });
        }

        /// <summary>
        /// Single-pass hash-based GroupBy + Mean for Int32 keys and Float64 values.
        /// </summary>
        public static DataFrame GroupByMeanF64Fast(Int32Series keys, Float64Series values)
        {
            int rowCount = keys.Length;
            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;
            var sums = new Dictionary<int, double>();
            var counts = new Dictionary<int, int>();

            for (int i = 0; i < rowCount; i++)
            {
                int k = keySpan[i];
                ref double s = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(sums, k, out _);
                s += valSpan[i];
                ref int c = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(counts, k, out _);
                c++;
            }

            int groupCount = sums.Count;
            var keyCol = new Int32Series("key", groupCount);
            var meanCol = new Float64Series("a_mean", groupCount);
            int pos = 0;
            foreach (var kvp in sums)
            {
                keyCol.Memory.Span[pos] = kvp.Key;
                meanCol.Memory.Span[pos] = counts[kvp.Key] > 0 ? kvp.Value / counts[kvp.Key] : 0;
                pos++;
            }
            return new DataFrame(new ISeries[] { keyCol, meanCol });
        }

        /// <summary>
        /// Single-pass hash-based GroupBy with multiple aggregations (sum, mean, min, max, count).
        /// Processes all aggregations in a single pass over data.
        /// </summary>
        public static DataFrame GroupByMultiAggF64Fast(Int32Series keys, Float64Series values)
        {
            int rowCount = keys.Length;
            var keySpan = keys.Memory.Span;
            var valSpan = values.Memory.Span;

            var sums = new Dictionary<int, double>();
            var counts = new Dictionary<int, int>();
            var mins = new Dictionary<int, double>();
            var maxs = new Dictionary<int, double>();
            var order = new List<int>(); // key insertion order

            for (int i = 0; i < rowCount; i++)
            {
                int k = keySpan[i];
                double v = valSpan[i];

                ref double s = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(sums, k, out bool exists);
                ref int c = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(counts, k, out _);
                ref double min = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(mins, k, out _);
                ref double max = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(maxs, k, out _);

                if (!exists)
                {
                    order.Add(k);
                    s = v;
                    c = 1;
                    min = v;
                    max = v;
                }
                else
                {
                    s += v;
                    c++;
                    if (v < min) min = v;
                    if (v > max) max = v;
                }
            }

            int groupCount = order.Count;
            var keyCol = new Int32Series("key", groupCount);
            var sumCol = new Float64Series("a_sum", groupCount);
            var meanCol = new Float64Series("a_mean", groupCount);
            var minCol = new Float64Series("a_min", groupCount);
            var maxCol = new Float64Series("a_max", groupCount);
            var countCol = new Int32Series("a_count", groupCount);

            for (int i = 0; i < groupCount; i++)
            {
                int k = order[i];
                keyCol.Memory.Span[i] = k;
                sumCol.Memory.Span[i] = sums[k];
                meanCol.Memory.Span[i] = counts[k] > 0 ? sums[k] / counts[k] : 0;
                minCol.Memory.Span[i] = mins[k];
                maxCol.Memory.Span[i] = maxs[k];
                countCol.Memory.Span[i] = counts[k];
            }

            return new DataFrame(new ISeries[] { keyCol, sumCol, meanCol, minCol, maxCol, countCol });
        }

        /// <summary>
        /// Generates groups for group_by_dynamic (time-window-based grouping).
        /// Splits the index column into windows of the specified period/offset.
        /// Ported from Glacier.Polaris_OLD.
        /// </summary>
        public static (List<List<int>> Groups, ISeries WindowKeys) GenerateDynamicGroups(
        ISeries indexColumn,
        double every,
        double period,
        double offset,
        string closed = "left",
        string startBy = "window")
        {
            int rowCount = indexColumn.Length;
            if (rowCount == 0)
            {
                return (new List<List<int>>(), CreateResultColumn(indexColumn.Name, "first", 0, indexColumn));
            }

            var vals = new double[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                vals[i] = GetValueAsDouble(indexColumn, i);
            }

            double vMin = vals[0];
            double vMax = vals[rowCount - 1];

            double start;
            if (startBy == "datapoint")
            {
                start = vMin;
            }
            else
            {
                start = Math.Floor(vMin / every) * every;
            }

            var groups = new List<List<int>>();
            var keysList = new List<double>();

            for (double w = start; w <= vMax || Math.Abs(w - vMax) < 1e-9; w += every)
            {
                double startK = w + offset;
                double endK = startK + period;

                int startIdx = FindFirstIndex(vals, startK, closed == "left" || closed == "both");
                int endIdx = FindLastIndex(vals, endK, closed == "right" || closed == "both");

                if (startIdx <= endIdx && startIdx >= 0 && endIdx >= 0)
                {
                    var group = new List<int>();
                    for (int idx = startIdx; idx <= endIdx; idx++)
                    {
                        group.Add(idx);
                    }
                    groups.Add(group);
                    keysList.Add(w);
                }
            }

            ISeries windowKeys;
            if (indexColumn is Int32Series)
            {
                var wk = new Int32Series(indexColumn.Name, keysList.Count);
                for (int i = 0; i < keysList.Count; i++) wk.Memory.Span[i] = (int)keysList[i];
                windowKeys = wk;
            }
            else if (indexColumn is Int64Series)
            {
                var wk = new Int64Series(indexColumn.Name, keysList.Count);
                for (int i = 0; i < keysList.Count; i++) wk.Memory.Span[i] = (long)keysList[i];
                windowKeys = wk;
            }
            else
            {
                var wk = new Float64Series(indexColumn.Name, keysList.Count);
                for (int i = 0; i < keysList.Count; i++) wk.Memory.Span[i] = keysList[i];
                windowKeys = wk;
            }

            return (groups, windowKeys);
        }

        /// <summary>
        /// Generates groups for rolling group-by (sliding-window-based grouping).
        /// Each row becomes a window with the specified period centered/adjusted by offset.
        /// Ported from Glacier.Polaris_OLD.
        /// </summary>
        public static List<List<int>> GenerateRollingGroups(
        ISeries indexColumn,
        double period,
        double offset,
        string closed = "right")
        {
            int rowCount = indexColumn.Length;
            var groups = new List<List<int>>(rowCount);
            if (rowCount == 0) return groups;

            var vals = new double[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                vals[i] = GetValueAsDouble(indexColumn, i);
            }

            int leftPtr = 0;
            int rightPtr = 0;

            for (int r = 0; r < rowCount; r++)
            {
                double valR = vals[r];
                double startK = valR + offset;
                double endK = startK + period;

                bool includeLeft = closed == "left" || closed == "both";
                while (leftPtr < rowCount && (includeLeft ? vals[leftPtr] < startK : vals[leftPtr] <= startK))
                {
                    leftPtr++;
                }

                bool includeRight = closed == "right" || closed == "both";
                if (rightPtr < leftPtr) rightPtr = leftPtr;
                while (rightPtr < rowCount && (includeRight ? vals[rightPtr] <= endK : vals[rightPtr] < endK))
                {
                    rightPtr++;
                }

                var group = new List<int>();
                for (int i = leftPtr; i < rightPtr; i++)
                {
                    group.Add(i);
                }
                groups.Add(group);
            }

            return groups;
        }

        private static int FindFirstIndex(double[] vals, double bound, bool includeEqual)
        {
            int low = 0;
            int high = vals.Length - 1;
            int result = -1;

            while (low <= high)
            {
                int mid = low + (high - low) / 2;
                bool condition = includeEqual ? vals[mid] >= bound : vals[mid] > bound;
                if (condition)
                {
                    result = mid;
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }
            return result;
        }

        private static int FindLastIndex(double[] vals, double bound, bool includeEqual)
        {
            int low = 0;
            int high = vals.Length - 1;
            int result = -1;

            while (low <= high)
            {
                int mid = low + (high - low) / 2;
                bool condition = includeEqual ? vals[mid] <= bound : vals[mid] < bound;
                if (condition)
                {
                    result = mid;
                    low = mid + 1;
                }
                else
                {
                    high = mid - 1;
                }
            }
            return result;
        }
    }
}
