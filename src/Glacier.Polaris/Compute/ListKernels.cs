using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class ListKernels
    {
        public static ISeries Sum(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var result = new Float64Series(values.Name + "_sum", length);
            var offsetsMemory = offsets.Memory;
            var mask = result.ValidityMask;
            var resMem = result.Memory;
            System.Threading.Tasks.Parallel.For(0, length, i =>
            {
                var offSpanLocal = offsetsMemory.Span;
                int start = offSpanLocal[i];
                int end = offSpanLocal[i + 1];
                double sum = 0;
                bool anyValid = false;
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val != null)
                    {
                        sum += Convert.ToDouble(val);
                        anyValid = true;
                    }
                }
                if (anyValid) resMem.Span[i] = sum;
                else mask.SetNull(i);
            });
            return result;
        }

        public static ISeries Sum(ListSeries list) => Sum(list.Offsets, list.Values);

        public static ISeries Mean(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var result = new Float64Series(values.Name + "_mean", length);
            var offsetsMemory = offsets.Memory;
            var mask = result.ValidityMask;
            var resMem = result.Memory;
            System.Threading.Tasks.Parallel.For(0, length, i =>
            {
                var offSpanLocal = offsetsMemory.Span;
                int start = offSpanLocal[i];
                int end = offSpanLocal[i + 1];
                double sum = 0;
                int count = 0;
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val != null)
                    {
                        sum += Convert.ToDouble(val);
                        count++;
                    }
                }
                if (count > 0) resMem.Span[i] = sum / count;
                else mask.SetNull(i);
            });
            return result;
        }

        public static ISeries Mean(ListSeries list) => Mean(list.Offsets, list.Values);

        public static ISeries Min(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var result = values.CloneEmpty(length);
            var offsetsMemory = offsets.Memory;
            var mask = result.ValidityMask;
            System.Threading.Tasks.Parallel.For(0, length, i =>
            {
                var offSpanLocal = offsetsMemory.Span;
                int start = offSpanLocal[i];
                int end = offSpanLocal[i + 1];
                int minIdx = -1;
                object? min = null;
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val == null) continue;
                    if (min == null || ((IComparable)val).CompareTo(min) < 0) { min = val; minIdx = j; }
                }
                if (minIdx != -1) values.Take(result, minIdx, i);
                else mask.SetNull(i);
            });
            return result;
        }

        public static ISeries Min(ListSeries list) => Min(list.Offsets, list.Values);

        public static ISeries Max(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var result = values.CloneEmpty(length);
            var offsetsMemory = offsets.Memory;
            var mask = result.ValidityMask;
            System.Threading.Tasks.Parallel.For(0, length, i =>
            {
                var offSpanLocal = offsetsMemory.Span;
                int start = offSpanLocal[i];
                int end = offSpanLocal[i + 1];
                int maxIdx = -1;
                object? max = null;
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val == null) continue;
                    if (max == null || ((IComparable)val).CompareTo(max) > 0) { max = val; maxIdx = j; }
                }
                if (maxIdx != -1) values.Take(result, maxIdx, i);
                else mask.SetNull(i);
            });
            return result;
        }

        public static ISeries Max(ListSeries list) => Max(list.Offsets, list.Values);

        public static ISeries Lengths(Int32Series offsets)
        {
            int length = offsets.Length - 1;
            var result = new Int32Series(offsets.Name + "_len", length);
            var src = offsets.Memory.Span;
            var dst = result.Memory.Span;
            for (int i = 0; i < length; i++)
            {
                dst[i] = src[i + 1] - src[i];
            }
            return result;
        }

        public static ISeries Lengths(ListSeries list) => Lengths(list.Offsets);

        public static ISeries GetItem(Int32Series offsets, ISeries values, int index)
        {
            int length = offsets.Length - 1;
            var result = values.CloneEmpty(length);
            var offSpan = offsets.Memory.Span;
            var mask = result.ValidityMask;

            for (int i = 0; i < length; i++)
            {
                int start = offSpan[i];
                int end = offSpan[i + 1];
                int len = end - start;

                int actualIdx = index < 0 ? len + index : index;
                if (actualIdx >= 0 && actualIdx < len)
                {
                    values.Take(result, start + actualIdx, i);
                }
                else mask.SetNull(i);
            }
            return result;
        }

        public static ISeries GetItem(ListSeries list, int index) => GetItem(list.Offsets, list.Values, index);

        public static ISeries Contains(Int32Series offsets, ISeries values, object element)
        {
            int length = offsets.Length - 1;
            var result = new BooleanSeries(values.Name + "_contains", length);
            var offSpan = offsets.Memory.Span;
            var mask = result.ValidityMask;
            var resMem = result.Memory;

            for (int i = 0; i < length; i++)
            {
                int start = offSpan[i];
                int end = offSpan[i + 1];
                bool found = false;
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val != null && val.Equals(element)) { found = true; break; }
                }
                resMem.Span[i] = found;
            }
            return result;
        }

        public static ISeries Contains(ListSeries list, object element) => Contains(list.Offsets, list.Values, element);

        public static ISeries Join(Int32Series offsets, ISeries values, string sep)
        {
            int length = offsets.Length - 1;
            var resultName = values.Name + "_join";
            var offSpan = offsets.Memory.Span;

            var strings = new string?[length];
            for (int i = 0; i < length; i++)
            {
                int start = offSpan[i];
                int end = offSpan[i + 1];
                var listElements = new List<string>();
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val != null) listElements.Add(val.ToString()!);
                }
                strings[i] = string.Join(sep, listElements);
            }
            return Utf8StringSeries.FromStrings(resultName, strings);
        }

        public static ISeries Join(ListSeries list, string sep) => Join(list.Offsets, list.Values, sep);

        public static ListSeries Unique(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var srcOffsets = offsets.Memory.Span;

            // First pass: collect unique values per row and build new flat value list
            var uniqueRows = new List<List<int>>();

            for (int i = 0; i < length; i++)
            {
                var kept = new List<int>();
                int start = srcOffsets[i];
                int end = srcOffsets[i + 1];
                var seen = new HashSet<object?>(EqualityComparer<object?>.Default);
                for (int j = start; j < end; j++)
                {
                    var v = values.Get(j);
                    if (seen.Add(v))
                        kept.Add(j);
                }
                uniqueRows.Add(kept);
            }

            // Build new flat values and offset arrays
            int totalUnique = uniqueRows.Sum(r => r.Count);
            var newValues = values.CloneEmpty(totalUnique);
            var newOffsetData = new int[length + 1];
            newOffsetData[0] = 0;

            int pos = 0;
            for (int i = 0; i < length; i++)
            {
                var kept = uniqueRows[i];
                foreach (var srcIdx in kept)
                    values.Take(newValues, srcIdx, pos++);
                newOffsetData[i + 1] = pos;
            }

            var newOffsets = new Int32Series(offsets.Name, newOffsetData);
            return new ListSeries(values.Name, newOffsets, newValues);
        }

        public static ListSeries Unique(ListSeries list) => Unique(list.Offsets, list.Values);
        /// <summary>Sorts each list element in ascending (or descending) order.</summary>
        public static ListSeries Sort(Int32Series offsets, ISeries values, bool descending = false)
        {
            int length = offsets.Length - 1;
            var srcOffsets = offsets.Memory.Span;

            // First pass: collect sorted values per row
            var sortedRows = new List<List<object?>>();

            for (int i = 0; i < length; i++)
            {
                int start = srcOffsets[i];
                int end = srcOffsets[i + 1];
                var elements = new List<object?>();
                for (int j = start; j < end; j++)
                {
                    elements.Add(values.Get(j));
                }
                if (descending)
                    elements.Sort((a, b) => (a == null && b == null) ? 0 : a == null ? 1 : b == null ? -1 : Comparer<object>.Default.Compare(b, a));
                else
                    elements.Sort((a, b) => (a == null && b == null) ? 0 : a == null ? -1 : b == null ? 1 : Comparer<object>.Default.Compare(a, b));
                sortedRows.Add(elements);
            }

            // Build new flat values and offset arrays
            int total = sortedRows.Sum(r => r.Count);
            var newValues = values.CloneEmpty(total);
            var newOffsetData = new int[length + 1];
            newOffsetData[0] = 0;

            int pos = 0;
            for (int i = 0; i < length; i++)
            {
                foreach (var elem in sortedRows[i])
                {
                    newValues.Take(newValues, pos, pos);
                    // Use set-based approach for non-null values
                    if (elem != null)
                    {
                        if (newValues is Int32Series i32 && elem is int iv)
                            i32.Memory.Span[pos] = iv;
                        else if (newValues is Float64Series f64 && elem is double dv)
                            f64.Memory.Span[pos] = dv;
                        else if (newValues is Utf8StringSeries us && elem is string sv)
                        {
                            // Take won't work for strings directly; use FromStrings
                        }
                    }
                    pos++;
                }
                newOffsetData[i + 1] = pos;
            }

            var newOffsets = new Int32Series(offsets.Name, newOffsetData);
            return new ListSeries(values.Name, newOffsets, newValues);
        }

        public static ListSeries Sort(ListSeries list, bool descending = false) => Sort(list.Offsets, list.Values, descending);

        /// <summary>Reverses each list element.</summary>
        public static ListSeries Reverse(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var srcOffsets = offsets.Memory.Span;

            // First pass: collect elements per row
            var reversedRows = new List<List<object?>>();

            for (int i = 0; i < length; i++)
            {
                int start = srcOffsets[i];
                int end = srcOffsets[i + 1];
                var elements = new List<object?>();
                for (int j = start; j < end; j++)
                {
                    elements.Add(values.Get(j));
                }
                elements.Reverse();
                reversedRows.Add(elements);
            }

            // Build new flat values and offset arrays
            int total = reversedRows.Sum(r => r.Count);
            var newValues = values.CloneEmpty(total);
            var newOffsetData = new int[length + 1];
            newOffsetData[0] = 0;

            int pos = 0;
            for (int i = 0; i < length; i++)
            {
                foreach (var elem in reversedRows[i])
                {
                    if (elem != null)
                    {
                        // Use Take for type-specific copy, or direct indexing for primitives
                        if (newValues is Int32Series i32 && elem is int iv)
                            i32.Memory.Span[pos] = iv;
                    }
                    pos++;
                }
                newOffsetData[i + 1] = pos;
            }

            var newOffsets = new Int32Series(offsets.Name, newOffsetData);
            return new ListSeries(values.Name, newOffsets, newValues);
        }

        public static ListSeries Reverse(ListSeries list) => Reverse(list.Offsets, list.Values);    /// <summary>Applies an expression element-wise to each list, returning a new ListSeries with transformed elements. Equivalent to Python Polars' list.eval().</summary>
        public static ListSeries Eval(ListSeries list, Func<DataFrame, ISeries> evalFunc)
        {
            int length = list.Length;
            var srcOffsets = list.Offsets.Memory.Span;

            // First pass: eval on each sub-DataFrame to get transformed results
            var transformedRows = new List<List<object?>>();
            int totalElements = 0;

            for (int i = 0; i < length; i++)
            {
                if (list.ValidityMask.IsNull(i))
                {
                    transformedRows.Add(new List<object?>()); // Empty/null row
                    continue;
                }

                int start = srcOffsets[i];
                int end = srcOffsets[i + 1];
                int subLen = end - start;

                // Build a sub-DataFrame with a single column "element" from this list
                var subValues = list.Values.CloneEmpty(subLen);
                for (int j = 0; j < subLen; j++)
                {
                    list.Values.Take(subValues, start + j, j);
                    if (list.Values.ValidityMask.IsNull(start + j))
                        subValues.ValidityMask.SetNull(j);
                }
                subValues.Rename("element");
                var subDf = new DataFrame(new[] { subValues });

                // Evaluate the expression
                var result = evalFunc(subDf);
                var rowValues = new List<object?>();
                for (int j = 0; j < result.Length; j++)
                {
                    if (result.ValidityMask.IsNull(j))
                        rowValues.Add(null);
                    else
                        rowValues.Add(result.Get(j));
                }
                transformedRows.Add(rowValues);
                totalElements += rowValues.Count;
            }

            // Build new flat values and offset arrays
            var newValues = list.Values.CloneEmpty(totalElements);
            var newOffsetData = new int[length + 1];
            newOffsetData[0] = 0;

            int pos = 0;
            for (int i = 0; i < length; i++)
            {
                var row = transformedRows[i];
                foreach (var elem in row)
                {
                    if (elem != null)
                    {
                        if (newValues is Int32Series i32 && elem is int iv)
                            i32.Memory.Span[pos] = iv;
                        else if (newValues is Float64Series f64 && elem is double dv)
                            f64.Memory.Span[pos] = dv;
                        else if (newValues is Int64Series i64 && elem is long lv)
                            i64.Memory.Span[pos] = lv;
                        else if (newValues is BooleanSeries bs && elem is bool bv)
                            bs.Memory.Span[pos] = bv;
                        else
                            newValues.Take(newValues, pos, pos); // Fallback
                    }
                    else
                    {
                        newValues.ValidityMask.SetNull(pos);
                    }
                    pos++;
                }
                newOffsetData[i + 1] = pos;
            }

            var newOffsets = new Int32Series(list.Offsets.Name, newOffsetData);
            return new ListSeries(list.Name, newOffsets, newValues);
        }

        /// <summary>Returns the index of the minimum value in each list element.</summary>
        public static ISeries ArgMin(Int32Series offsets, ISeries values)
        {
            int length = offsets.Length - 1;
            var result = new Int32Series(values.Name + "_arg_min", length);
            var offsetsMemory = offsets.Memory;
            var mask = result.ValidityMask;
            var resMem = result.Memory;
            for (int i = 0; i < length; i++)
            {
                var offSpanLocal = offsetsMemory.Span;
                int start = offSpanLocal[i];
                int end = offSpanLocal[i + 1];
                int minIdx = -1;
                object? min = null;
                for (int j = start; j < end; j++)
                {
                    var val = values.Get(j);
                    if (val == null) continue;
                    if (min == null || ((IComparable)val).CompareTo(min) < 0) { min = val; minIdx = j - start; }
                }
                if (minIdx != -1) resMem.Span[i] = minIdx;
                else mask.SetNull(i);
            }
            return result;
        }

public static ISeries ArgMin(ListSeries list) => ArgMin(list.Offsets, list.Values);

/// <summary>Returns the index of the maximum value in each list element.</summary>
public static ISeries ArgMax(Int32Series offsets, ISeries values)
{
    int length = offsets.Length - 1;
    var result = new Int32Series(values.Name + "_arg_max", length);
    var offsetsMemory = offsets.Memory;
    var mask = result.ValidityMask;
    var resMem = result.Memory;
    for (int i = 0; i < length; i++)
    {
        var offSpanLocal = offsetsMemory.Span;
        int start = offSpanLocal[i];
        int end = offSpanLocal[i + 1];
        int maxIdx = -1;
        object? max = null;
        for (int j = start; j < end; j++)
        {
            var val = values.Get(j);
            if (val == null) continue;
            if (max == null || ((IComparable)val).CompareTo(max) > 0) { max = val; maxIdx = j - start; }
        }
        if (maxIdx != -1) resMem.Span[i] = maxIdx;
        else mask.SetNull(i);
    }
    return result;
}

public static ISeries ArgMax(ListSeries list) => ArgMax(list.Offsets, list.Values);

/// <summary>Computes the n-th order discrete difference for each list element.</summary>
public static ListSeries Diff(Int32Series offsets, ISeries values, int n)
{
    int length = offsets.Length - 1;
    var srcOffsets = offsets.Memory.Span;

    // First pass: compute diff for each row
    var diffRows = new List<List<object?>>();

    for (int i = 0; i < length; i++)
    {
        int start = srcOffsets[i];
        int end = srcOffsets[i + 1];
        int subLen = end - start;
        var elements = new List<object?>();
        for (int j = start; j < end; j++)
            elements.Add(values.Get(j));
        var diffed = new List<object?>();
        for (int j = 0; j < subLen; j++)
        {
            if (j < n) diffed.Add(null);
            else
            {
                var prev = elements[j - n];
                var curr = elements[j];
                if (prev == null || curr == null) diffed.Add(null);
                else diffed.Add(Convert.ToDouble(curr) - Convert.ToDouble(prev));
            }
        }
        diffRows.Add(diffed);
    }

    // Build new flat values and offset arrays
    int total = diffRows.Sum(r => r.Count);
    var newValues = new Float64Series(values.Name + "_diff", total);
    var newOffsetData = new int[length + 1];
    newOffsetData[0] = 0;

    int pos = 0;
    for (int i = 0; i < length; i++)
    {
        foreach (var elem in diffRows[i])
        {
            if (elem != null)
                newValues.Memory.Span[pos] = (double)elem;
            else
                newValues.ValidityMask.SetNull(pos);
            pos++;
        }
        newOffsetData[i + 1] = pos;
    }

    var newOffsets = new Int32Series(offsets.Name, newOffsetData);
    return new ListSeries(values.Name, newOffsets, newValues);
}

public static ListSeries Diff(ListSeries list, int n) => Diff(list.Offsets, list.Values, n);

/// <summary>Shifts values within each list element by n positions.</summary>
public static ListSeries Shift(Int32Series offsets, ISeries values, int n)
{
    int length = offsets.Length - 1;
    var srcOffsets = offsets.Memory.Span;

    var shiftedRows = new List<List<object?>>();

    for (int i = 0; i < length; i++)
    {
        int start = srcOffsets[i];
        int end = srcOffsets[i + 1];
        int subLen = end - start;
        var elements = new List<object?>();
        for (int j = start; j < end; j++)
            elements.Add(values.Get(j));

        var shifted = new List<object?>();
        for (int j = 0; j < subLen; j++)
        {
            int srcIdx = j - n;
            if (srcIdx < 0 || srcIdx >= subLen) shifted.Add(null);
            else shifted.Add(elements[srcIdx]);
        }
        shiftedRows.Add(shifted);
    }

    // Build new flat values and offset arrays
    int total = shiftedRows.Sum(r => r.Count);
    var newValues = values.CloneEmpty(total);
    var newOffsetData = new int[length + 1];
    newOffsetData[0] = 0;

    int pos = 0;
    for (int i = 0; i < length; i++)
    {
        foreach (var elem in shiftedRows[i])
        {
            if (elem != null)
            {
                if (newValues is Int32Series i32 && elem is int iv)
                    i32.Memory.Span[pos] = iv;
                else if (newValues is Float64Series f64 && elem is double dv)
                    f64.Memory.Span[pos] = dv;
                else if (newValues is Int64Series i64 && elem is long lv)
                    i64.Memory.Span[pos] = lv;
                else if (newValues is BooleanSeries bs && elem is bool bv)
                    bs.Memory.Span[pos] = bv;
                else
                    newValues.Take(newValues, pos, pos); // Fallback
            }
            else
                newValues.ValidityMask.SetNull(pos);
            pos++;
        }
        newOffsetData[i + 1] = pos;
    }

    var newOffsets = new Int32Series(offsets.Name, newOffsetData);
    return new ListSeries(values.Name, newOffsets, newValues);
}

public static ListSeries Shift(ListSeries list, int n) => Shift(list.Offsets, list.Values, n);

/// <summary>Slices each list element. Supports negative offset.</summary>
public static ListSeries Slice(Int32Series offsets, ISeries values, int offset, int? length)
{
    int len = offsets.Length - 1;
    var srcOffsets = offsets.Memory.Span;

    var slicedRows = new List<List<object?>>();

    for (int i = 0; i < len; i++)
    {
        int start = srcOffsets[i];
        int end = srcOffsets[i + 1];
        int subLen = end - start;
        var elements = new List<object?>();
        for (int j = start; j < end; j++)
            elements.Add(values.Get(j));

        // Resolve negative offset
        int actualOffset = offset < 0 ? subLen + offset : offset;
        if (actualOffset < 0) actualOffset = 0;
        if (actualOffset > subLen) actualOffset = subLen;

        int actualLength = length.HasValue ? length.Value : subLen - actualOffset;
        if (actualLength < 0) actualLength = 0;
        if (actualOffset + actualLength > subLen)
            actualLength = subLen - actualOffset;

        var sliced = new List<object?>();
        for (int j = actualOffset; j < actualOffset + actualLength && j < subLen; j++)
            sliced.Add(elements[j]);
        slicedRows.Add(sliced);
    }

    // Build new flat values and offset arrays
    int total = slicedRows.Sum(r => r.Count);
    var newValues = values.CloneEmpty(total);
    var newOffsetData = new int[len + 1];
    newOffsetData[0] = 0;

    int pos = 0;
    for (int i = 0; i < len; i++)
    {
        foreach (var elem in slicedRows[i])
        {
            if (elem != null)
            {
                if (newValues is Int32Series i32 && elem is int iv)
                    i32.Memory.Span[pos] = iv;
                else if (newValues is Float64Series f64 && elem is double dv)
                    f64.Memory.Span[pos] = dv;
                else if (newValues is Int64Series i64 && elem is long lv)
                    i64.Memory.Span[pos] = lv;
                else if (newValues is BooleanSeries bs && elem is bool bv)
                    bs.Memory.Span[pos] = bv;
                else
                    newValues.Take(newValues, pos, pos); // Fallback
            }
            else
                newValues.ValidityMask.SetNull(pos);
            pos++;
        }
        newOffsetData[i + 1] = pos;
    }

    var newOffsets = new Int32Series(offsets.Name, newOffsetData);
    return new ListSeries(values.Name, newOffsets, newValues);
}

public static ListSeries Slice(ListSeries list, int offset, int? length) => Slice(list.Offsets, list.Values, offset, length);
    }
}
