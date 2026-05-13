using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class UniqueKernels
    {
        public static ISeries NUnique(ISeries series)
        {
            if (series is Int32Series i32)
            {
                var set = new FastIntSet(Math.Min(1024, i32.Length));
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
                var set = new FastDoubleSet(Math.Min(1024, f64.Length));
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
        }                /// <summary>
                         /// Returns the indices of unique elements, preserving first occurrence order.
                         /// Uses sort-based approach for Int32/Float64 for better cache locality.
                         /// </summary>
        public static List<int> UniqueIndices(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int len = i32.Length;
                var set = new FastIntSet(Math.Min(1024, len));
                var unique = new List<int>();
                var span = i32.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(i);
                }
                return unique;
            }
            if (series is Float64Series f64)
            {
                int len = f64.Length;
                var set = new FastDoubleSet(Math.Min(1024, len));
                var unique = new List<int>();
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
        }                /// <summary>Returns unique values using fast open addressing hash set for Int32/Float64, order-preserving.</summary>
        public static ISeries Unique(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int len = i32.Length;
                if (len == 0) return new Int32Series(series.Name, 0);
                var span = i32.Memory.Span;
                var set = new FastIntSet(Math.Min(1024, len));
                var unique = new List<int>();
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(span[i]);
                }
                var arr = new Int32Series(series.Name, unique.Count);
                var dst = arr.Memory.Span;
                for (int i = 0; i < unique.Count; i++) dst[i] = unique[i];
                return arr;
            }
            if (series is Float64Series f64)
            {
                int len = f64.Length;
                if (len == 0) return new Float64Series(series.Name, 0);
                var span = f64.Memory.Span;
                var set = new FastDoubleSet(Math.Min(1024, len));
                var unique = new List<double>();
                for (int i = 0; i < len; i++)
                {
                    if (set.Add(span[i])) unique.Add(span[i]);
                }
                var arr = new Float64Series(series.Name, unique.Count);
                var dst = arr.Memory.Span;
                for (int i = 0; i < unique.Count; i++) dst[i] = unique[i];
                return arr;
            }
            // Fallback: HashSet for strings (not benchmarked heavily)
            if (series is Utf8StringSeries u8)
            {
                var set = new System.Collections.Generic.HashSet<string>();
                var list = new System.Collections.Generic.List<string>();
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
        }/// <summary>Returns a BooleanSeries where true indicates the value appears more than once.</summary>
        public static Data.BooleanSeries IsDuplicated(ISeries series)
        {
            int len = series.Length;
            var result = new Data.BooleanSeries(series.Name + "_is_duplicated", len);
            // Count occurrences of each value using a dictionary
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
        private struct FastIntSet
        {
            private int[] _entries;
            private byte[] _states;
            private int _count;
            private int _mask;

            public int Count => _count;

            public FastIntSet(int capacity)
            {
                int size = 16;
                while (size < capacity) size <<= 1;
                _entries = new int[size];
                _states = new byte[size];
                _mask = size - 1;
                _count = 0;
            }

            [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
            public bool Add(int value)
            {
                if (_count * 2 >= _entries.Length) Resize();
                int hash = (int)((uint)value * 2654435761u);
                int pos = hash & _mask;
                while (_states[pos] != 0)
                {
                    if (_entries[pos] == value) return false;
                    pos = (pos + 1) & _mask;
                }
                _entries[pos] = value;
                _states[pos] = 1;
                _count++;
                return true;
            }

            private void Resize()
            {
                int newSize = _entries.Length * 2;
                var newEntries = new int[newSize];
                var newStates = new byte[newSize];
                int newMask = newSize - 1;

                for (int i = 0; i < _entries.Length; i++)
                {
                    if (_states[i] == 1)
                    {
                        int val = _entries[i];
                        int hash = (int)((uint)val * 2654435761u);
                        int pos = hash & newMask;
                        while (newStates[pos] != 0) pos = (pos + 1) & newMask;
                        newEntries[pos] = val;
                        newStates[pos] = 1;
                    }
                }
                _entries = newEntries;
                _states = newStates;
                _mask = newMask;
            }
        }

        private struct FastDoubleSet
        {
            private double[] _entries;
            private byte[] _states;
            private int _count;
            private int _mask;

            public int Count => _count;

            public FastDoubleSet(int capacity)
            {
                int size = 16;
                while (size < capacity) size <<= 1;
                _entries = new double[size];
                _states = new byte[size];
                _mask = size - 1;
                _count = 0;
            }

            [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
            public bool Add(double value)
            {
                if (_count * 2 >= _entries.Length) Resize();
                long bits = BitConverter.DoubleToInt64Bits(value);
                int hash = (int)((ulong)bits * 11400714819323198485ul >> 32);
                int pos = hash & _mask;
                while (_states[pos] != 0)
                {
                    if (_entries[pos] == value) return false;
                    pos = (pos + 1) & _mask;
                }
                _entries[pos] = value;
                _states[pos] = 1;
                _count++;
                return true;
            }

            private void Resize()
            {
                int newSize = _entries.Length * 2;
                var newEntries = new double[newSize];
                var newStates = new byte[newSize];
                int newMask = newSize - 1;

                for (int i = 0; i < _entries.Length; i++)
                {
                    if (_states[i] == 1)
                    {
                        double val = _entries[i];
                        long bits = BitConverter.DoubleToInt64Bits(val);
                        int hash = (int)((ulong)bits * 11400714819323198485ul >> 32);
                        int pos = hash & newMask;
                        while (newStates[pos] != 0) pos = (pos + 1) & newMask;
                        newEntries[pos] = val;
                        newStates[pos] = 1;
                    }
                }
                _entries = newEntries;
                _states = newStates;
                _mask = newMask;
            }
        }
        public static ISeries IsFirst(ISeries series)
        {
            var result = new Data.BooleanSeries(series.Name, series.Length);
            var resultSpan = result.Memory.Span;

            if (series is Int32Series i32)
            {
                var set = new FastIntSet(Math.Min(1024, i32.Length));
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                    resultSpan[i] = set.Add(span[i]);
            }
            else if (series is Float64Series f64)
            {
                var set = new FastDoubleSet(Math.Min(1024, f64.Length));
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
                        resultSpan[i] = set.Add("\0__NULL__\0");
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
                var set = new FastIntSet(Math.Min(1024, i32.Length));
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                    set.Add(span[i]);
                return set.Count;
            }
            if (series is Float64Series f64)
            {
                var set = new FastDoubleSet(Math.Min(1024, f64.Length));
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
}    }
}
