using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class UniqueKernels
    {
        /// <summary>
        /// Allocates an empty target series of the exact runtime type of <paramref name="source"/> sized for <paramref name="indices"/>,
        /// computing exact byte buffer sizes for variable-width string and binary columns without reflection.
        /// </summary>
        public static ISeries CreateSeriesForTake(ISeries source, ReadOnlySpan<int> indices, string? nameOverride = null)
        {
            string targetName = nameOverride ?? source.Name;
            if (source is Utf8StringSeries u8)
            {
                int totalBytes = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    int idx = indices[i];
                    if (idx >= 0 && !u8.ValidityMask.IsNull(idx))
                    {
                        totalBytes += u8.GetStringSpan(idx).Length;
                    }
                }
                return new Utf8StringSeries(targetName, indices.Length, totalBytes);
            }
            if (source is BinarySeries bin)
            {
                int totalBytes = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    int idx = indices[i];
                    if (idx >= 0 && !bin.ValidityMask.IsNull(idx))
                    {
                        totalBytes += bin.GetSpan(idx).Length;
                    }
                }
                return new BinarySeries(targetName, indices.Length, totalBytes);
            }

            var cloned = source.CloneEmpty(indices.Length);
            if (targetName != cloned.Name)
            {
                cloned.Rename(targetName);
            }
            return cloned;
        }

        /// <summary>
        /// Allocates an empty series of the same type as <paramref name="source"/> with the specified name and length without reflection.
        /// </summary>
        public static ISeries CloneWithName(ISeries source, string name, int length)
        {
            var cloned = source.CloneEmpty(length);
            if (cloned.Name != name)
            {
                cloned.Rename(name);
            }
            return cloned;
        }

        public static ISeries NUnique(ISeries series)
        {
            int count = CountUnique(series);
            var result = new Int32Series(series.Name + "_nunique", 1);
            result.Memory.Span[0] = count;
            return result;
        }

        public static int ApproxNUnique(ISeries series) => CountUnique(series);

        private static int CountUnique(ISeries series)
        {
            int len = series.Length;
            if (len == 0) return 0;

            if (series is NullSeries)
            {
                return 1;
            }

            if (series is Int32Series i32)
            {
                return CountUniqueInt32Like(i32.Memory.Span.Slice(0, len), i32.ValidityMask);
            }
            if (series is DateSeries dt)
            {
                return CountUniqueInt32Like(dt.Memory.Span.Slice(0, len), dt.ValidityMask);
            }
            if (series is Int64Series i64)
            {
                return CountUniqueInt64Like(i64.Memory.Span.Slice(0, len), i64.ValidityMask);
            }
            if (series is DatetimeSeries dtm)
            {
                return CountUniqueInt64Like(dtm.Memory.Span.Slice(0, len), dtm.ValidityMask);
            }
            if (series is DurationSeries dur)
            {
                return CountUniqueInt64Like(dur.Memory.Span.Slice(0, len), dur.ValidityMask);
            }
            if (series is TimeSeries tm)
            {
                return CountUniqueInt64Like(tm.Memory.Span.Slice(0, len), tm.ValidityMask);
            }
            if (series is Float64Series f64)
            {
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                if (!mask.HasNulls)
                {
                    for (int i = 0; i < len; i++) set.Add(span[i]);
                    return set.Count;
                }
                bool hasNull = false;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i)) hasNull = true;
                    else set.Add(span[i]);
                }
                return set.Count + (hasNull ? 1 : 0);
            }
            if (series is Float32Series f32)
            {
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var span = f32.Memory.Span;
                var mask = f32.ValidityMask;
                bool hasNull = false;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i)) hasNull = true;
                    else set.Add(span[i]);
                }
                return set.Count + (hasNull ? 1 : 0);
            }

            return UniqueIndices(series).Count;
        }

        private static int CountUniqueInt32Like(ReadOnlySpan<int> span, Memory.ValidityMask mask)
        {
            int len = span.Length;
            int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
            var set = new FastIntSet(cap);
            if (!mask.HasNulls)
            {
                for (int i = 0; i < len; i++) set.Add(span[i]);
                return set.Count;
            }
            bool hasNull = false;
            for (int i = 0; i < len; i++)
            {
                if (mask.IsNull(i)) hasNull = true;
                else set.Add(span[i]);
            }
            return set.Count + (hasNull ? 1 : 0);
        }

        private static int CountUniqueInt64Like(ReadOnlySpan<long> span, Memory.ValidityMask mask)
        {
            int len = span.Length;
            var set = new HashSet<long>(Math.Min(len, 32768));
            if (!mask.HasNulls)
            {
                for (int i = 0; i < len; i++) set.Add(span[i]);
                return set.Count;
            }
            bool hasNull = false;
            for (int i = 0; i < len; i++)
            {
                if (mask.IsNull(i)) hasNull = true;
                else set.Add(span[i]);
            }
            return set.Count + (hasNull ? 1 : 0);
        }

        /// <summary>
        /// Returns the indices of unique elements in a single series, preserving first occurrence order
        /// and retaining the first null occurrence distinctly from zero/empty values.
        /// </summary>
        public static List<int> UniqueIndices(ISeries series)
        {
            int len = series.Length;
            if (len == 0) return new List<int>(0);

            if (series is NullSeries)
            {
                return new List<int>(1) { 0 };
            }

            if (series is Int32Series i32)
            {
                return UniqueIndicesInt32Like(i32.Memory.Span.Slice(0, len), i32.ValidityMask);
            }
            if (series is DateSeries dt)
            {
                return UniqueIndicesInt32Like(dt.Memory.Span.Slice(0, len), dt.ValidityMask);
            }
            if (series is Int64Series i64)
            {
                return UniqueIndicesUnmanaged(i64.Memory.Span.Slice(0, len), i64.ValidityMask);
            }
            if (series is DatetimeSeries dtm)
            {
                return UniqueIndicesUnmanaged(dtm.Memory.Span.Slice(0, len), dtm.ValidityMask);
            }
            if (series is DurationSeries dur)
            {
                return UniqueIndicesUnmanaged(dur.Memory.Span.Slice(0, len), dur.ValidityMask);
            }
            if (series is TimeSeries tm)
            {
                return UniqueIndicesUnmanaged(tm.Memory.Span.Slice(0, len), tm.ValidityMask);
            }
            if (series is Int8Series i8)
            {
                return UniqueIndicesUnmanaged(i8.Memory.Span.Slice(0, len), i8.ValidityMask);
            }
            if (series is Int16Series i16)
            {
                return UniqueIndicesUnmanaged(i16.Memory.Span.Slice(0, len), i16.ValidityMask);
            }
            if (series is UInt8Series u8s)
            {
                return UniqueIndicesUnmanaged(u8s.Memory.Span.Slice(0, len), u8s.ValidityMask);
            }
            if (series is UInt16Series u16)
            {
                return UniqueIndicesUnmanaged(u16.Memory.Span.Slice(0, len), u16.ValidityMask);
            }
            if (series is UInt32Series u32)
            {
                return UniqueIndicesUnmanaged(u32.Memory.Span.Slice(0, len), u32.ValidityMask);
            }
            if (series is UInt64Series u64)
            {
                return UniqueIndicesUnmanaged(u64.Memory.Span.Slice(0, len), u64.ValidityMask);
            }
            if (series is DecimalSeries dec)
            {
                return UniqueIndicesUnmanaged(dec.Memory.Span.Slice(0, len), dec.ValidityMask);
            }
            if (series is BooleanSeries b)
            {
                var unique = new List<int>(3);
                var span = b.Memory.Span;
                var mask = b.ValidityMask;
                bool hasFalse = false, hasTrue = false, hasNull = false;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i))
                    {
                        if (!hasNull) { hasNull = true; unique.Add(i); }
                    }
                    else if (span[i])
                    {
                        if (!hasTrue) { hasTrue = true; unique.Add(i); }
                    }
                    else
                    {
                        if (!hasFalse) { hasFalse = true; unique.Add(i); }
                    }
                    if (hasFalse && hasTrue && (hasNull || !mask.HasNulls)) break;
                }
                return unique;
            }
            if (series is Float64Series f64)
            {
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var unique = new List<int>(Math.Min(len, 32768));
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                if (!mask.HasNulls)
                {
                    for (int i = 0; i < len; i++)
                    {
                        if (set.Add(span[i])) unique.Add(i);
                    }
                    return unique;
                }
                bool hasNull = false;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i))
                    {
                        if (!hasNull) { hasNull = true; unique.Add(i); }
                    }
                    else if (set.Add(span[i]))
                    {
                        unique.Add(i);
                    }
                }
                return unique;
            }
            if (series is Float32Series f32)
            {
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var unique = new List<int>(Math.Min(len, 32768));
                var span = f32.Memory.Span;
                var mask = f32.ValidityMask;
                bool hasNull = false;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i))
                    {
                        if (!hasNull) { hasNull = true; unique.Add(i); }
                    }
                    else if (set.Add(span[i]))
                    {
                        unique.Add(i);
                    }
                }
                return unique;
            }

            // Use multi-column row-tuple deduplicator over the single column for strings, binary, categorical, enum, list, struct, object
            return UniqueRowIndicesCore(new ISeries[] { series });
        }

        private static List<int> UniqueIndicesInt32Like(ReadOnlySpan<int> span, Memory.ValidityMask mask)
        {
            int len = span.Length;
            int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
            var set = new FastIntSet(cap);
            var unique = new List<int>(Math.Min(len, 32768));
            if (!mask.HasNulls)
            {
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(i);
                }
                return unique;
            }
            bool hasNull = false;
            for (int i = 0; i < len; i++)
            {
                if (mask.IsNull(i))
                {
                    if (!hasNull) { hasNull = true; unique.Add(i); }
                }
                else if (set.Add(span[i]))
                {
                    unique.Add(i);
                }
            }
            return unique;
        }

        private static List<int> UniqueIndicesUnmanaged<T>(ReadOnlySpan<T> span, Memory.ValidityMask mask) where T : unmanaged, IEquatable<T>
        {
            int len = span.Length;
            var set = new HashSet<T>(Math.Min(len, 32768));
            var unique = new List<int>(Math.Min(len, 32768));
            if (!mask.HasNulls)
            {
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(i);
                }
                return unique;
            }
            bool hasNull = false;
            for (int i = 0; i < len; i++)
            {
                if (mask.IsNull(i))
                {
                    if (!hasNull) { hasNull = true; unique.Add(i); }
                }
                else if (set.Add(span[i]))
                {
                    unique.Add(i);
                }
            }
            return unique;
        }

        /// <summary>
        /// Computes unique row indices across one or more columns, preserving null vs 0/0.0/"" distinction
        /// and retaining the first occurrence of each distinct row tuple.
        /// </summary>
        public static List<int> UniqueRowIndices(IReadOnlyList<ISeries> columns, bool maintainOrder = true)
        {
            if (columns == null || columns.Count == 0) return new List<int>(0);
            if (columns.Count == 1)
            {
                return UniqueIndices(columns[0]);
            }
            return UniqueRowIndicesCore(columns);
        }

        private static List<int> UniqueRowIndicesCore(IReadOnlyList<ISeries> columns)
        {
            int colCount = columns.Count;
            int rowCount = columns[0].Length;
            if (rowCount == 0) return new List<int>(0);

            int bucketCount = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, Math.Min(1 << 24, rowCount * 2)));
            int bucketMask = bucketCount - 1;
            int[] buckets = new int[bucketCount];
            Array.Fill(buckets, -1);
            int[] next = new int[rowCount];
            int[] hashes = new int[rowCount];
            var uniqueRows = new List<int>(Math.Min(rowCount, 32768));

            for (int row = 0; row < rowCount; row++)
            {
                int hash = ComputeRowHash(columns, colCount, row);
                hashes[row] = hash;
                int bucket = hash & bucketMask;

                bool duplicate = false;
                for (int candidate = buckets[bucket]; candidate != -1; candidate = next[candidate])
                {
                    if (hashes[candidate] == hash && RowsEqual(columns, colCount, candidate, row))
                    {
                        duplicate = true;
                        break;
                    }
                }

                if (!duplicate)
                {
                    next[row] = buckets[bucket];
                    buckets[bucket] = row;
                    uniqueRows.Add(row);
                }
            }

            return uniqueRows;
        }

        private static int ComputeRowHash(IReadOnlyList<ISeries> columns, int colCount, int row)
        {
            var hc = new HashCode();
            for (int c = 0; c < colCount; c++)
            {
                ISeries col = columns[c];
                if (col.ValidityMask.IsNull(row))
                {
                    hc.Add(0x5A17C3E9 ^ c);
                    continue;
                }

                hc.Add(0x1F2E3D4C ^ c);
                switch (col)
                {
                    case Int32Series i32:
                        hc.Add(i32.Memory.Span[row]);
                        break;
                    case Int64Series i64:
                        hc.Add(i64.Memory.Span[row]);
                        break;
                    case Float64Series f64:
                        hc.Add(CanonicalizeDoubleBits(f64.Memory.Span[row]));
                        break;
                    case Float32Series f32:
                        hc.Add(CanonicalizeFloatBits(f32.Memory.Span[row]));
                        break;
                    case BooleanSeries b:
                        hc.Add(b.Memory.Span[row]);
                        break;
                    case Utf8StringSeries u8:
                        hc.AddBytes(u8.GetStringSpan(row));
                        break;
                    case BinarySeries bin:
                        hc.AddBytes(bin.GetSpan(row));
                        break;
                    case DateSeries dt:
                        hc.Add(dt.Memory.Span[row]);
                        break;
                    case DatetimeSeries dtm:
                        hc.Add(dtm.Memory.Span[row]);
                        break;
                    case DurationSeries dur:
                        hc.Add(dur.Memory.Span[row]);
                        break;
                    case TimeSeries tm:
                        hc.Add(tm.Memory.Span[row]);
                        break;
                    case DecimalSeries dec:
                        hc.Add(dec.Memory.Span[row]);
                        break;
                    case Int8Series i8:
                        hc.Add(i8.Memory.Span[row]);
                        break;
                    case Int16Series i16:
                        hc.Add(i16.Memory.Span[row]);
                        break;
                    case UInt8Series ui8:
                        hc.Add(ui8.Memory.Span[row]);
                        break;
                    case UInt16Series ui16:
                        hc.Add(ui16.Memory.Span[row]);
                        break;
                    case UInt32Series ui32:
                        hc.Add(ui32.Memory.Span[row]);
                        break;
                    case UInt64Series ui64:
                        hc.Add(ui64.Memory.Span[row]);
                        break;
                    default:
                        AddObjectValueHash(ref hc, col.Get(row));
                        break;
                }
            }
            return hc.ToHashCode();
        }

        private static void AddObjectValueHash(ref HashCode hc, object? val)
        {
            if (val == null)
            {
                hc.Add(0x7F3A91B2);
                return;
            }
            if (val is object?[] arr)
            {
                hc.Add(arr.Length);
                for (int i = 0; i < arr.Length; i++) AddObjectValueHash(ref hc, arr[i]);
                return;
            }
            if (val is IDictionary<string, object?> dict)
            {
                hc.Add(dict.Count);
                foreach (var kvp in dict)
                {
                    hc.Add(kvp.Key);
                    AddObjectValueHash(ref hc, kvp.Value);
                }
                return;
            }
            if (val is byte[] bytes)
            {
                hc.AddBytes(bytes);
                return;
            }
            hc.Add(val);
        }

        private static bool RowsEqual(IReadOnlyList<ISeries> columns, int colCount, int rowA, int rowB)
        {
            for (int c = 0; c < colCount; c++)
            {
                ISeries col = columns[c];
                bool nullA = col.ValidityMask.IsNull(rowA);
                bool nullB = col.ValidityMask.IsNull(rowB);
                if (nullA != nullB) return false;
                if (nullA) continue;

                bool eq = col switch
                {
                    Int32Series i32 => i32.Memory.Span[rowA] == i32.Memory.Span[rowB],
                    Int64Series i64 => i64.Memory.Span[rowA] == i64.Memory.Span[rowB],
                    Float64Series f64 => CanonicalizeDoubleBits(f64.Memory.Span[rowA]) == CanonicalizeDoubleBits(f64.Memory.Span[rowB]),
                    Float32Series f32 => CanonicalizeFloatBits(f32.Memory.Span[rowA]) == CanonicalizeFloatBits(f32.Memory.Span[rowB]),
                    BooleanSeries b => b.Memory.Span[rowA] == b.Memory.Span[rowB],
                    Utf8StringSeries u8 => u8.GetStringSpan(rowA).SequenceEqual(u8.GetStringSpan(rowB)),
                    BinarySeries bin => bin.GetSpan(rowA).SequenceEqual(bin.GetSpan(rowB)),
                    DateSeries dt => dt.Memory.Span[rowA] == dt.Memory.Span[rowB],
                    DatetimeSeries dtm => dtm.Memory.Span[rowA] == dtm.Memory.Span[rowB],
                    DurationSeries dur => dur.Memory.Span[rowA] == dur.Memory.Span[rowB],
                    TimeSeries tm => tm.Memory.Span[rowA] == tm.Memory.Span[rowB],
                    DecimalSeries dec => dec.Memory.Span[rowA] == dec.Memory.Span[rowB],
                    Int8Series i8 => i8.Memory.Span[rowA] == i8.Memory.Span[rowB],
                    Int16Series i16 => i16.Memory.Span[rowA] == i16.Memory.Span[rowB],
                    UInt8Series ui8 => ui8.Memory.Span[rowA] == ui8.Memory.Span[rowB],
                    UInt16Series ui16 => ui16.Memory.Span[rowA] == ui16.Memory.Span[rowB],
                    UInt32Series ui32 => ui32.Memory.Span[rowA] == ui32.Memory.Span[rowB],
                    UInt64Series ui64 => ui64.Memory.Span[rowA] == ui64.Memory.Span[rowB],
                    _ => ObjectValuesEqual(col.Get(rowA), col.Get(rowB))
                };
                if (!eq) return false;
            }
            return true;
        }

        private static bool ObjectValuesEqual(object? a, object? b)
        {
            if (ReferenceEquals(a, b)) return true;
            if (a == null || b == null) return false;
            if (a is object?[] arrA && b is object?[] arrB)
            {
                if (arrA.Length != arrB.Length) return false;
                for (int i = 0; i < arrA.Length; i++)
                {
                    if (!ObjectValuesEqual(arrA[i], arrB[i])) return false;
                }
                return true;
            }
            if (a is IDictionary<string, object?> dictA && b is IDictionary<string, object?> dictB)
            {
                if (dictA.Count != dictB.Count) return false;
                foreach (var kvp in dictA)
                {
                    if (!dictB.TryGetValue(kvp.Key, out var bVal) || !ObjectValuesEqual(kvp.Value, bVal))
                        return false;
                }
                return true;
            }
            if (a is byte[] bytesA && b is byte[] bytesB)
            {
                return bytesA.AsSpan().SequenceEqual(bytesB);
            }
            return Equals(a, b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long CanonicalizeDoubleBits(double val)
        {
            if (val == 0.0) return 0L;
            return double.IsNaN(val) ? 0x7FF8000000000000L : BitConverter.DoubleToInt64Bits(val);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CanonicalizeFloatBits(float val)
        {
            if (val == 0.0f) return 0;
            return float.IsNaN(val) ? 0x7FC00000 : BitConverter.SingleToInt32Bits(val);
        }

        /// <summary>Returns unique values in first-seen order across all series types, retaining the first null occurrence.</summary>
        public static ISeries Unique(ISeries series)
        {
            int len = series.Length;
            if (len == 0)
            {
                return series.CloneEmpty(0);
            }

            if (series is Int32Series i32 && !i32.ValidityMask.HasNulls)
            {
                var span = i32.Memory.Span;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                int mask = cap - 1;
                int[] entries = new int[cap];
                ref int entriesRef = ref MemoryMarshal.GetArrayDataReference(entries);

                int[] outBuffer = GC.AllocateUninitializedArray<int>(len);
                int count = 0;
                bool hasZero = false;

                for (int i = 0; i < len; i++)
                {
                    int val = span[i];
                    if (val == 0)
                    {
                        if (!hasZero)
                        {
                            hasZero = true;
                            outBuffer[count++] = 0;
                        }
                        continue;
                    }

                    uint hash = (uint)val * 2654435761u;
                    int pos = (int)(hash & (uint)mask);

                    while (true)
                    {
                        ref int slot = ref Unsafe.Add(ref entriesRef, pos);
                        int entry = slot;
                        if (entry == 0)
                        {
                            slot = val;
                            outBuffer[count++] = val;
                            break;
                        }
                        if (entry == val) break;
                        pos = (pos + 1) & mask;
                    }
                }

                if (count == len) return new Int32Series(series.Name, outBuffer);
                var arr = new Int32Series(series.Name, count);
                outBuffer.AsSpan(0, count).CopyTo(arr.Memory.Span);
                return arr;
            }

            if (series is Float64Series f64 && !f64.ValidityMask.HasNulls)
            {
                var span = f64.Memory.Span;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                int mask = cap - 1;
                long[] entries = new long[cap];
                ref long entriesRef = ref MemoryMarshal.GetArrayDataReference(entries);

                double[] outBuffer = GC.AllocateUninitializedArray<double>(len);
                int count = 0;
                bool hasZero = false;

                for (int i = 0; i < len; i++)
                {
                    double val = span[i];
                    long bits = CanonicalizeDoubleBits(val);

                    if (bits == 0L)
                    {
                        if (!hasZero)
                        {
                            hasZero = true;
                            outBuffer[count++] = 0.0;
                        }
                        continue;
                    }

                    ulong hash = (ulong)bits * 11400714819323198485ul;
                    int pos = (int)((hash >> 32) & (uint)mask);

                    while (true)
                    {
                        ref long slot = ref Unsafe.Add(ref entriesRef, pos);
                        long entry = slot;
                        if (entry == 0L)
                        {
                            slot = bits;
                            outBuffer[count++] = val;
                            break;
                        }
                        if (entry == bits) break;
                        pos = (pos + 1) & mask;
                    }
                }

                if (count == len) return new Float64Series(series.Name, outBuffer);
                var arr = new Float64Series(series.Name, count);
                outBuffer.AsSpan(0, count).CopyTo(arr.Memory.Span);
                return arr;
            }

            var indicesList = UniqueIndices(series);
            var indicesSpan = CollectionsMarshal.AsSpan(indicesList);
            var target = CreateSeriesForTake(series, indicesSpan);
            if (indicesSpan.Length > 0)
            {
                series.Take(target, indicesSpan);
            }
            return target;
        }

        /// <summary>Returns a BooleanSeries where true indicates the value appears more than once.</summary>
        public static Data.BooleanSeries IsDuplicated(ISeries series)
        {
            int len = series.Length;
            var result = new Data.BooleanSeries(series.Name + "_is_duplicated", len);
            var counts = new Dictionary<object, int>();
            for (int i = 0; i < len; i++)
            {
                if (series.ValidityMask.IsNull(i)) continue;
                var val = series.Get(i)!;
                counts.TryGetValue(val, out var c);
                counts[val] = c + 1;
            }
            for (int i = 0; i < len; i++)
            {
                if (series.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var val = series.Get(i)!;
                result.Memory.Span[i] = counts.TryGetValue(val, out var c) && c > 1;
            }
            return result;
        }

        /// <summary>Returns a BooleanSeries where true indicates the value appears exactly once.</summary>
        public static Data.BooleanSeries IsUnique(ISeries series)
        {
            int len = series.Length;
            var result = new Data.BooleanSeries(series.Name + "_is_unique", len);
            var counts = new Dictionary<object, int>();
            for (int i = 0; i < len; i++)
            {
                if (series.ValidityMask.IsNull(i)) continue;
                var val = series.Get(i)!;
                counts.TryGetValue(val, out var c);
                counts[val] = c + 1;
            }
            for (int i = 0; i < len; i++)
            {
                if (series.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var val = series.Get(i)!;
                result.Memory.Span[i] = counts.TryGetValue(val, out var c) && c == 1;
            }
            return result;
        }

        internal struct FastIntSet
        {
            private int[] _entries;
            private int _count;
            private int _mask;
            private bool _hasZero;

            public int Count => _count;

            public FastIntSet(int capacity)
            {
                int size = 16;
                while (size < capacity) size <<= 1;
                _entries = new int[size];
                _mask = size - 1;
                _count = 0;
                _hasZero = false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Add(int value)
            {
                if (value == 0)
                {
                    if (_hasZero) return false;
                    _hasZero = true;
                    _count++;
                    return true;
                }

                if (_count * 2 >= _entries.Length) Resize();
                uint hash = (uint)value * 2654435761u;
                int mask = _mask;
                int pos = (int)(hash & (uint)mask);
                ref int entriesRef = ref MemoryMarshal.GetArrayDataReference(_entries);
                while (true)
                {
                    ref int slot = ref Unsafe.Add(ref entriesRef, pos);
                    int entry = slot;
                    if (entry == 0)
                    {
                        slot = value;
                        _count++;
                        return true;
                    }
                    if (entry == value) return false;
                    pos = (pos + 1) & mask;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Contains(int value)
            {
                if (value == 0) return _hasZero;
                uint hash = (uint)value * 2654435761u;
                int mask = _mask;
                int pos = (int)(hash & (uint)mask);
                ref int entriesRef = ref MemoryMarshal.GetArrayDataReference(_entries);
                while (true)
                {
                    int entry = Unsafe.Add(ref entriesRef, pos);
                    if (entry == 0) return false;
                    if (entry == value) return true;
                    pos = (pos + 1) & mask;
                }
            }

            private void Resize()
            {
                int newSize = _entries.Length * 2;
                var newEntries = new int[newSize];
                int newMask = newSize - 1;

                for (int i = 0; i < _entries.Length; i++)
                {
                    int val = _entries[i];
                    if (val != 0)
                    {
                        uint hash = (uint)val * 2654435761u;
                        int pos = (int)(hash & (uint)newMask);
                        while (newEntries[pos] != 0) pos = (pos + 1) & newMask;
                        newEntries[pos] = val;
                    }
                }
                _entries = newEntries;
                _mask = newMask;
            }
        }

        internal struct FastDoubleSet
        {
            private long[] _entries;
            private int _count;
            private int _mask;
            private bool _hasZero;

            public int Count => _count;

            public FastDoubleSet(int capacity)
            {
                int size = 16;
                while (size < capacity) size <<= 1;
                _entries = new long[size];
                _mask = size - 1;
                _count = 0;
                _hasZero = false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Add(double value)
            {
                long bits = CanonicalizeDoubleBits(value);
                if (bits == 0L)
                {
                    if (_hasZero) return false;
                    _hasZero = true;
                    _count++;
                    return true;
                }

                if (_count * 2 >= _entries.Length) Resize();
                ulong hash = (ulong)bits * 11400714819323198485ul;
                int mask = _mask;
                int pos = (int)((hash >> 32) & (uint)mask);
                ref long entriesRef = ref MemoryMarshal.GetArrayDataReference(_entries);
                while (true)
                {
                    ref long slot = ref Unsafe.Add(ref entriesRef, pos);
                    long entry = slot;
                    if (entry == 0L)
                    {
                        slot = bits;
                        _count++;
                        return true;
                    }
                    if (entry == bits) return false;
                    pos = (pos + 1) & mask;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Contains(double value)
            {
                long bits = CanonicalizeDoubleBits(value);
                if (bits == 0L) return _hasZero;

                ulong hash = (ulong)bits * 11400714819323198485ul;
                int mask = _mask;
                int pos = (int)((hash >> 32) & (uint)mask);
                ref long entriesRef = ref MemoryMarshal.GetArrayDataReference(_entries);
                while (true)
                {
                    long entry = Unsafe.Add(ref entriesRef, pos);
                    if (entry == 0L) return false;
                    if (entry == bits) return true;
                    pos = (pos + 1) & mask;
                }
            }

            private void Resize()
            {
                int newSize = _entries.Length * 2;
                var newEntries = new long[newSize];
                int newMask = newSize - 1;

                for (int i = 0; i < _entries.Length; i++)
                {
                    long val = _entries[i];
                    if (val != 0L)
                    {
                        ulong hash = (ulong)val * 11400714819323198485ul;
                        int pos = (int)((hash >> 32) & (uint)newMask);
                        while (newEntries[pos] != 0L) pos = (pos + 1) & newMask;
                        newEntries[pos] = val;
                    }
                }
                _entries = newEntries;
                _mask = newMask;
            }
        }

        public static ISeries IsFirst(ISeries series)
        {
            var result = new Data.BooleanSeries(series.Name, series.Length);
            var resultSpan = result.Memory.Span;
            resultSpan.Clear();

            var firstIndices = UniqueIndices(series);
            foreach (int idx in firstIndices)
            {
                resultSpan[idx] = true;
            }

            return result;
        }

        public static DataFrame ValueCounts(ISeries series, bool sort, bool parallel)
        {
            ISeries keysCol;
            ISeries countsCol;
            int len = series.Length;

            if (series is Int32Series i32)
            {
                var dict = new Dictionary<int, int>();
                int nullCount = 0;
                var span = i32.Memory.Span;
                var mask = i32.ValidityMask;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i)) { nullCount++; continue; }
                    int val = span[i];
                    dict.TryGetValue(val, out int c);
                    dict[val] = c + 1;
                }

                var entries = dict.Select(kvp => (Key: (int?)kvp.Key, Count: kvp.Value)).ToList();
                if (nullCount > 0) entries.Add((null, nullCount));
                if (sort) entries.Sort((a, b) => b.Count.CompareTo(a.Count));

                keysCol = Int32Series.FromValues(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Count).ToArray());
            }
            else if (series is Float64Series f64)
            {
                var dict = new Dictionary<double, int>();
                int nullCount = 0;
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                for (int i = 0; i < len; i++)
                {
                    if (mask.IsNull(i)) { nullCount++; continue; }
                    double val = span[i];
                    dict.TryGetValue(val, out int c);
                    dict[val] = c + 1;
                }

                var entries = dict.Select(kvp => (Key: (double?)kvp.Key, Count: kvp.Value)).ToList();
                if (nullCount > 0) entries.Add((null, nullCount));
                if (sort) entries.Sort((a, b) => b.Count.CompareTo(a.Count));

                keysCol = Float64Series.FromValues(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Count).ToArray());
            }
            else if (series is Utf8StringSeries u8)
            {
                var dict = new Dictionary<string, int>();
                int nullCount = 0;
                for (int i = 0; i < len; i++)
                {
                    if (u8.ValidityMask.IsValid(i))
                    {
                        string val = u8.GetString(i)!;
                        dict.TryGetValue(val, out int c);
                        dict[val] = c + 1;
                    }
                    else
                    {
                        nullCount++;
                    }
                }

                var entries = dict.Select(kvp => (Key: (string?)kvp.Key, Count: kvp.Value)).ToList();
                if (nullCount > 0) entries.Add((null, nullCount));
                if (sort) entries.Sort((a, b) => b.Count.CompareTo(a.Count));

                keysCol = Utf8StringSeries.FromStrings(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Count).ToArray());
            }
            else
            {
                var dict = new Dictionary<object, int>();
                int nullCount = 0;
                for (int i = 0; i < len; i++)
                {
                    var val = series.Get(i);
                    if (val == null)
                    {
                        nullCount++;
                    }
                    else
                    {
                        dict.TryGetValue(val, out int c);
                        dict[val] = c + 1;
                    }
                }

                var entries = dict.Select(kvp => new KeyValuePair<object?, int>(kvp.Key, kvp.Value)).ToList();
                if (nullCount > 0)
                {
                    entries.Add(new KeyValuePair<object?, int>(null, nullCount));
                }

                if (sort) entries.Sort((a, b) => b.Value.CompareTo(a.Value));

                keysCol = new ObjectSeries(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Value).ToArray());
            }

            return new DataFrame(new List<ISeries> { keysCol, countsCol });
        }
    }
}
