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
        public static ISeries NUnique(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int len = i32.Length;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastIntSet(cap);
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    set.Add(span[i]);
                }
                var result = new Int32Series(series.Name + "_nunique", 1);
                result.Memory.Span[0] = set.Count;
                return result;
            }
            if (series is Float64Series f64)
            {
                int len = f64.Length;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var span = f64.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    set.Add(span[i]);
                }
                var result = new Int32Series(series.Name + "_nunique", 1);
                result.Memory.Span[0] = set.Count;
                return result;
            }
            if (series is Utf8StringSeries u8)
            {
                var set = new HashSet<string>();
                for (int i = 0; i < u8.Length; i++)
                    if (u8.ValidityMask.IsValid(i)) set.Add(u8.GetString(i)!);
                var result = new Int32Series(series.Name + "_nunique", 1);
                result.Memory.Span[0] = set.Count;
                return result;
            }
            var fallback = new Int32Series(series.Name + "_nunique", 1);
            fallback.Memory.Span[0] = 0;
            return fallback;
        }

        /// <summary>
        /// Returns the indices of unique elements, preserving first occurrence order.
        /// </summary>
        public static List<int> UniqueIndices(ISeries series)
        {
            int len = series.Length;
            if (len == 0) return new List<int>(0);

            if (series is Int32Series i32)
            {
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastIntSet(cap);
                var unique = new List<int>(Math.Min(len, 32768));
                var span = i32.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(i);
                }
                return unique;
            }
            if (series is Float64Series f64)
            {
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var unique = new List<int>(Math.Min(len, 32768));
                var span = f64.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(i);
                }
                return unique;
            }
            if (series is Utf8StringSeries u8)
            {
                var set = new HashSet<string>();
                var unique = new List<int>();
                for (int i = 0; i < u8.Length; i++)
                    if (u8.ValidityMask.IsValid(i) && set.Add(u8.GetString(i)!))
                        unique.Add(i);
                return unique;
            }
            return Enumerable.Range(0, series.Length).ToList();
        }

        /// <summary>Returns unique values using fast open addressing hash set for Int32/Float64, order-preserving.</summary>
        public static ISeries Unique(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int len = i32.Length;
                if (len == 0) return new Int32Series(series.Name, 0);

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

                if (count == len)
                {
                    return new Int32Series(series.Name, outBuffer);
                }
                var arr = new Int32Series(series.Name, count);
                outBuffer.AsSpan(0, count).CopyTo(arr.Memory.Span);
                return arr;
            }
            if (series is Float64Series f64)
            {
                int len = f64.Length;
                if (len == 0) return new Float64Series(series.Name, 0);

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
                    if (val == 0.0) val = 0.0;
                    long bits = double.IsNaN(val) ? 0x7FF8000000000000L : BitConverter.DoubleToInt64Bits(val);

                    if (bits == 0L)
                    {
                        if (!hasZero)
                        {
                            hasZero = true;
                            outBuffer[count++] = val;
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

                if (count == len)
                {
                    return new Float64Series(series.Name, outBuffer);
                }
                var arr = new Float64Series(series.Name, count);
                outBuffer.AsSpan(0, count).CopyTo(arr.Memory.Span);
                return arr;
            }
            // Fallback: HashSet for strings (not benchmarked heavily)
            if (series is Utf8StringSeries u8)
            {
                var set = new HashSet<string>();
                var list = new List<string>();
                for (int i = 0; i < u8.Length; i++)
                {
                    if (u8.ValidityMask.IsValid(i))
                    {
                        var s = u8.GetString(i);
                        if (set.Add(s!)) list.Add(s!);
                    }
                }
                return Utf8StringSeries.FromStrings(series.Name, list.ToArray());
            }
            return new NullSeries(series.Name, 0);
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
                var val = series.Get(i);
                counts.TryGetValue(val!, out var c);
                counts[val!] = c + 1;
            }
            for (int i = 0; i < len; i++)
            {
                if (series.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var val = series.Get(i);
                result.Memory.Span[i] = counts.TryGetValue(val!, out var c) && c > 1;
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
                var val = series.Get(i);
                counts.TryGetValue(val!, out var c);
                counts[val!] = c + 1;
            }
            for (int i = 0; i < len; i++)
            {
                if (series.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var val = series.Get(i);
                result.Memory.Span[i] = counts.TryGetValue(val!, out var c) && c == 1;
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
                if (value == 0.0) value = 0.0;
                long bits = double.IsNaN(value) ? 0x7FF8000000000000L : BitConverter.DoubleToInt64Bits(value);
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
                if (value == 0.0) value = 0.0;
                long bits = double.IsNaN(value) ? 0x7FF8000000000000L : BitConverter.DoubleToInt64Bits(value);
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

            if (series is Int32Series i32)
            {
                int len = i32.Length;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastIntSet(cap);
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                    resultSpan[i] = set.Add(span[i]);
            }
            else if (series is Float64Series f64)
            {
                int len = f64.Length;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var span = f64.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                    resultSpan[i] = set.Add(span[i]);
            }
            else if (series is Utf8StringSeries u8)
            {
                var set = new HashSet<string>();
                for (int i = 0; i < u8.Length; i++)
                {
                    if (u8.ValidityMask.IsValid(i))
                        resultSpan[i] = set.Add(u8.GetString(i)!);
                    else
                        resultSpan[i] = set.Add(" __NULL__ ");
                }
            }
            else
            {
                var set = new HashSet<object?>();
                for (int i = 0; i < series.Length; i++)
                    resultSpan[i] = set.Add(series.Get(i));
            }

            return result;
        }

        public static int ApproxNUnique(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int len = i32.Length;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastIntSet(cap);
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                    set.Add(span[i]);
                return set.Count;
            }
            if (series is Float64Series f64)
            {
                int len = f64.Length;
                int cap = Math.Max(16, (int)Math.Min(1 << 24, BitOperations.RoundUpToPowerOf2((uint)Math.Max(16, len * 2))));
                var set = new FastDoubleSet(cap);
                var span = f64.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                    set.Add(span[i]);
                return set.Count;
            }
            if (series is Utf8StringSeries u8)
            {
                var set = new HashSet<string>();
                for (int i = 0; i < u8.Length; i++)
                    if (u8.ValidityMask.IsValid(i)) set.Add(u8.GetString(i)!);
                return set.Count;
            }
            return 0;
        }

        public static DataFrame ValueCounts(ISeries series, bool sort, bool parallel)
        {
            ISeries keysCol;
            ISeries countsCol;

            if (series is Int32Series i32)
            {
                var dict = new Dictionary<int, int>();
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    int val = span[i];
                    dict.TryGetValue(val, out int c);
                    dict[val] = c + 1;
                }

                var entries = dict.ToList();
                if (sort) entries.Sort((a, b) => b.Value.CompareTo(a.Value));

                keysCol = new Int32Series(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Value).ToArray());
            }
            else if (series is Float64Series f64)
            {
                var dict = new Dictionary<double, int>();
                var span = f64.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    double val = span[i];
                    dict.TryGetValue(val, out int c);
                    dict[val] = c + 1;
                }

                var entries = dict.ToList();
                if (sort) entries.Sort((a, b) => b.Value.CompareTo(a.Value));

                keysCol = new Float64Series(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Value).ToArray());
            }
            else if (series is Utf8StringSeries u8)
            {
                var dict = new Dictionary<string, int>();
                for (int i = 0; i < u8.Length; i++)
                {
                    if (u8.ValidityMask.IsValid(i))
                    {
                        string val = u8.GetString(i)!;
                        dict.TryGetValue(val, out int c);
                        dict[val] = c + 1;
                    }
                    else
                    {
                        dict.TryGetValue("null", out int c);
                        dict["null"] = c + 1;
                    }
                }

                var entries = dict.ToList();
                if (sort) entries.Sort((a, b) => b.Value.CompareTo(a.Value));

                keysCol = new Utf8StringSeries(series.Name, entries.Select(e => e.Key).ToArray());
                countsCol = new Int32Series("count", entries.Select(e => e.Value).ToArray());
            }
            else
            {
                var dict = new Dictionary<object, int>();
                int nullCount = 0;
                for (int i = 0; i < series.Length; i++)
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
