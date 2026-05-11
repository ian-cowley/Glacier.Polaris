using System;
using System.Collections.Generic;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class ArrayKernels
    {
        public static ISeries Shift(ISeries source, int n)
        {
            int length = source.Length;
            var resultType = source.DataType;

            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, length);
                var srcSpan = i32.Memory.Span;
                var dstSpan = result.Memory.Span;
                int limit = n >= 0 ? length - n : length + n;
                if (n >= 0)
                {
                    for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = n; i < length; i++)
                    {
                        dstSpan[i] = srcSpan[i - n];
                        if (!i32.ValidityMask.IsValid(i - n))
                            result.ValidityMask.SetNull(i);
                    }
                }
                else
                {
                    int shiftRight = -n;
                    for (int i = length - shiftRight; i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = 0; i < length - shiftRight; i++)
                    {
                        dstSpan[i] = srcSpan[i + shiftRight];
                        if (!i32.ValidityMask.IsValid(i + shiftRight))
                            result.ValidityMask.SetNull(i);
                    }
                }
                return result;
            }
            else if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, length);
                var srcSpan = i64.Memory.Span;
                var dstSpan = result.Memory.Span;
                int limit = n >= 0 ? length - n : length + n;
                if (n >= 0)
                {
                    for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = n; i < length; i++)
                    {
                        dstSpan[i] = srcSpan[i - n];
                        if (!i64.ValidityMask.IsValid(i - n))
                            result.ValidityMask.SetNull(i);
                    }
                }
                else
                {
                    int shiftRight = -n;
                    for (int i = length - shiftRight; i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = 0; i < length - shiftRight; i++)
                    {
                        dstSpan[i] = srcSpan[i + shiftRight];
                        if (!i64.ValidityMask.IsValid(i + shiftRight))
                            result.ValidityMask.SetNull(i);
                    }
                }
                return result;
            }
            else if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                var srcSpan = f64.Memory.Span;
                var dstSpan = result.Memory.Span;
                if (n >= 0)
                {
                    for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = n; i < length; i++)
                    {
                        dstSpan[i] = srcSpan[i - n];
                        if (!f64.ValidityMask.IsValid(i - n))
                            result.ValidityMask.SetNull(i);
                    }
                }
                else
                {
                    int shiftRight = -n;
                    for (int i = length - shiftRight; i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = 0; i < length - shiftRight; i++)
                    {
                        dstSpan[i] = srcSpan[i + shiftRight];
                        if (!f64.ValidityMask.IsValid(i + shiftRight))
                            result.ValidityMask.SetNull(i);
                    }
                }
                return result;
            }
            else if (source is Float32Series f32)
            {
                var result = new Float32Series(source.Name, length);
                var srcSpan = f32.Memory.Span;
                var dstSpan = result.Memory.Span;
                if (n >= 0)
                {
                    for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = n; i < length; i++)
                    {
                        dstSpan[i] = srcSpan[i - n];
                        if (!f32.ValidityMask.IsValid(i - n))
                            result.ValidityMask.SetNull(i);
                    }
                }
                else
                {
                    int shiftRight = -n;
                    for (int i = length - shiftRight; i < length; i++) result.ValidityMask.SetNull(i);
                    for (int i = 0; i < length - shiftRight; i++)
                    {
                        dstSpan[i] = srcSpan[i + shiftRight];
                        if (!f32.ValidityMask.IsValid(i + shiftRight))
                            result.ValidityMask.SetNull(i);
                    }
                }
                return result;
            }
            else if (source is Int16Series i16)
            {
                var result = new Int16Series(source.Name, length);
                var srcSpan = i16.Memory.Span;
                var dstSpan = result.Memory.Span;
                if (n >= 0) { for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i); for (int i = n; i < length; i++) { dstSpan[i] = srcSpan[i - n]; if (!i16.ValidityMask.IsValid(i - n)) result.ValidityMask.SetNull(i); } }
                else { int sr = -n; for (int i = length - sr; i < length; i++) result.ValidityMask.SetNull(i); for (int i = 0; i < length - sr; i++) { dstSpan[i] = srcSpan[i + sr]; if (!i16.ValidityMask.IsValid(i + sr)) result.ValidityMask.SetNull(i); } }
                return result;
            }
            else if (source is UInt32Series u32)
            {
                var result = new UInt32Series(source.Name, length);
                var srcSpan = u32.Memory.Span;
                var dstSpan = result.Memory.Span;
                if (n >= 0) { for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i); for (int i = n; i < length; i++) { dstSpan[i] = srcSpan[i - n]; if (!u32.ValidityMask.IsValid(i - n)) result.ValidityMask.SetNull(i); } }
                else { int sr = -n; for (int i = length - sr; i < length; i++) result.ValidityMask.SetNull(i); for (int i = 0; i < length - sr; i++) { dstSpan[i] = srcSpan[i + sr]; if (!u32.ValidityMask.IsValid(i + sr)) result.ValidityMask.SetNull(i); } }
                return result;
            }
            else if (source is UInt64Series u64)
            {
                var result = new UInt64Series(source.Name, length);
                var srcSpan = u64.Memory.Span;
                var dstSpan = result.Memory.Span;
                if (n >= 0) { for (int i = 0; i < n && i < length; i++) result.ValidityMask.SetNull(i); for (int i = n; i < length; i++) { dstSpan[i] = srcSpan[i - n]; if (!u64.ValidityMask.IsValid(i - n)) result.ValidityMask.SetNull(i); } }
                else { int sr = -n; for (int i = length - sr; i < length; i++) result.ValidityMask.SetNull(i); for (int i = 0; i < length - sr; i++) { dstSpan[i] = srcSpan[i + sr]; if (!u64.ValidityMask.IsValid(i + sr)) result.ValidityMask.SetNull(i); } }
                return result;
            }
            throw new NotSupportedException($"Shift not supported for {source.GetType().Name}");
        }

        public static ISeries Diff(ISeries source, int n)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);

            if (n <= 0 || n >= length) return result; // all nulls

            for (int i = 0; i < n; i++) result.ValidityMask.SetNull(i);

            if (source is Int32Series i32)
            {
                var span = i32.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i) && i32.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else if (source is Int64Series i64)
            {
                var span = i64.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i) && i64.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else if (source is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i) && f64.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else if (source is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i) && f32.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else if (source is Int16Series i16)
            {
                var span = i16.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (i16.ValidityMask.IsValid(i) && i16.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else if (source is UInt32Series u32)
            {
                var span = u32.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (u32.ValidityMask.IsValid(i) && u32.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else if (source is UInt64Series u64)
            {
                var span = u64.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (u64.ValidityMask.IsValid(i) && u64.ValidityMask.IsValid(i - n))
                        result.Memory.Span[i] = span[i] - span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
            }
            else
            {
                throw new NotSupportedException($"Diff not supported for {source.GetType().Name}");
            }

            return result;
        }

        public static ISeries Abs(ISeries source)
        {
            int length = source.Length;

            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, length);
                var src = i32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                    {
                        int v = src[i];
                        dst[i] = v < 0 ? -v : v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, length);
                var src = i64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                    {
                        long v = src[i];
                        dst[i] = v < 0 ? -v : v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                var src = f64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        dst[i] = Math.Abs(src[i]);
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Float32Series f32)
            {
                var result = new Float32Series(source.Name, length);
                var src = f32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        dst[i] = Math.Abs(src[i]);
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Int16Series i16)
            {
                var result = new Int16Series(source.Name, length);
                var src = i16.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (i16.ValidityMask.IsValid(i))
                    {
                        short v = src[i];
                        dst[i] = (short)(v < 0 ? -v : v);
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Int8Series i8)
            {
                var result = new Int8Series(source.Name, length);
                var src = i8.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (i8.ValidityMask.IsValid(i))
                    {
                        sbyte v = src[i];
                        dst[i] = (sbyte)(v < 0 ? -v : v);
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            return source; // unsigned types: Abs is identity
        }

        public static ISeries Clip(ISeries source, double min, double max)
        {
            int length = source.Length;

            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, length);
                var src = i32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                    {
                        int v = src[i];
                        if (v < min) dst[i] = (int)min;
                        else if (v > max) dst[i] = (int)max;
                        else dst[i] = v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, length);
                var src = i64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                    {
                        long v = src[i];
                        if (v < min) dst[i] = (long)min;
                        else if (v > max) dst[i] = (long)max;
                        else dst[i] = v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                var src = f64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                    {
                        double v = src[i];
                        dst[i] = v < min ? min : v > max ? max : v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is Float32Series f32)
            {
                var result = new Float32Series(source.Name, length);
                var src = f32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                    {
                        float v = src[i];
                        dst[i] = v < (float)min ? (float)min : v > (float)max ? (float)max : v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is UInt32Series u32)
            {
                var result = new UInt32Series(source.Name, length);
                var src = u32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (u32.ValidityMask.IsValid(i))
                    {
                        uint v = src[i];
                        if (v < (uint)min) dst[i] = (uint)min;
                        else if (v > (uint)max) dst[i] = (uint)max;
                        else dst[i] = v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            else if (source is UInt64Series u64)
            {
                var result = new UInt64Series(source.Name, length);
                var src = u64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (u64.ValidityMask.IsValid(i))
                    {
                        ulong v = src[i];
                        if (v < (ulong)min) dst[i] = (ulong)min;
                        else if (v > (ulong)max) dst[i] = (ulong)max;
                        else dst[i] = v;
                    }
                    else result.ValidityMask.SetNull(i);
                }
                return result;
            }
            throw new NotSupportedException($"Clip not supported for {source.GetType().Name}");
        }

        public static ISeries DropNulls(ISeries source)
        {
            var mask = source.ValidityMask;
            int length = source.Length;
            var validIndices = new List<int>(length);
            for (int i = 0; i < length; i++)
            {
                if (mask.IsValid(i))
                    validIndices.Add(i);
            }

            int validCount = validIndices.Count;
            if (validCount == length) return source; // no nulls to drop

            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, validCount);
                var src = i32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, validCount);
                var src = i64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, validCount);
                var src = f64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is Float32Series f32)
            {
                var result = new Float32Series(source.Name, validCount);
                var src = f32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is Int16Series i16)
            {
                var result = new Int16Series(source.Name, validCount);
                var src = i16.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is UInt32Series u32)
            {
                var result = new UInt32Series(source.Name, validCount);
                var src = u32.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is UInt64Series u64)
            {
                var result = new UInt64Series(source.Name, validCount);
                var src = u64.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is BooleanSeries bs)
            {
                var result = new BooleanSeries(source.Name, validCount);
                var src = bs.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < validCount; i++)
                    dst[i] = src[validIndices[i]];
                return result;
            }
            else if (source is Utf8StringSeries u8)
            {
                var srcOffsets = u8.Offsets.Span;
                var srcData = u8.DataBytes.Span;
                int totalBytes = 0;
                for (int i = 0; i < validCount; i++)
                {
                    int idx = validIndices[i];
                    totalBytes += srcOffsets[idx + 1] - srcOffsets[idx];
                }
                var result = new Utf8StringSeries(source.Name, validCount, totalBytes);
                var dstOffsets = result.Offsets.Span;
                var dstData = result.DataBytes.Span;
                int bytePos = 0;
                for (int i = 0; i < validCount; i++)
                {
                    int idx = validIndices[i];
                    int start = srcOffsets[idx];
                    int end = srcOffsets[idx + 1];
                    int len = end - start;
                    srcData.Slice(start, len).CopyTo(dstData.Slice(bytePos));
                    dstOffsets[i] = bytePos;
                    bytePos += len;
                }
                dstOffsets[validCount] = bytePos;
                return result;
            }
            throw new NotSupportedException($"DropNulls not supported for {source.GetType().Name}");
        }
        /// <summary>Gather every n-th element starting at offset. Like Python Polars' gather_every.</summary>
        public static ISeries GatherEvery(ISeries source, int n, int offset)
        {
            int length = source.Length;
            if (n <= 0) throw new ArgumentException("n must be positive", nameof(n));
            if (offset < 0) offset = Math.Max(0, length + offset);
            if (offset >= length) return source.CloneEmpty(0);

            // Count how many elements will be gathered
            int resultCount = 0;
            for (int i = offset; i < length; i += n) resultCount++;

            if (resultCount == 0) return source.CloneEmpty(0);

            int[] indices = new int[resultCount];
            int idx = 0;
            for (int i = offset; i < length; i += n) indices[idx++] = i;

            return ComputeKernels.Take(source, indices);
        }

        /// <summary>Binary search: for each value in searchValues, find insertion index in sorted source.</summary>
        public static ISeries SearchSorted(ISeries source, ISeries searchValues)
        {
            int sourceLen = source.Length;
            int searchLen = searchValues.Length;
            var result = new Int32Series(source.Name + "_search_sorted", searchLen);

            if (source is Int32Series i32)
            {
                var srcSpan = i32.Memory.Span;
                var resSpan = result.Memory.Span;
                for (int i = 0; i < searchLen; i++)
                {
                    if (searchValues is Int32Series sv32)
                    {
                        int val = sv32.Memory.Span[i];
                        resSpan[i] = Array.BinarySearch(srcSpan.ToArray(), val);
                        if (resSpan[i] < 0) resSpan[i] = ~resSpan[i];
                    }
                    else
                    {
                        resSpan[i] = 0;
                    }
                }
            }
            else if (source is Float64Series f64)
            {
                var srcSpan = f64.Memory.Span;
                var resSpan = result.Memory.Span;
                for (int i = 0; i < searchLen; i++)
                {
                    if (searchValues is Float64Series sv64)
                    {
                        double val = sv64.Memory.Span[i];
                        resSpan[i] = Array.BinarySearch(srcSpan.ToArray(), val);
                        if (resSpan[i] < 0) resSpan[i] = ~resSpan[i];
                    }
                    else
                    {
                        resSpan[i] = 0;
                    }
                }
            }
            else if (source is Int64Series i64)
            {
                var srcSpan = i64.Memory.Span;
                var resSpan = result.Memory.Span;
                for (int i = 0; i < searchLen; i++)
                {
                    if (searchValues is Int64Series sv64)
                    {
                        long val = sv64.Memory.Span[i];
                        resSpan[i] = Array.BinarySearch(srcSpan.ToArray(), val);
                        if (resSpan[i] < 0) resSpan[i] = ~resSpan[i];
                    }
                    else
                    {
                        resSpan[i] = 0;
                    }
                }
            }
            else if (source is Utf8StringSeries u8)
            {
                var resSpan = result.Memory.Span;
                var srcStrings = new string[sourceLen];
                for (int s = 0; s < sourceLen; s++) srcStrings[s] = u8.GetString(s);
                Array.Sort(srcStrings);

                for (int i = 0; i < searchLen; i++)
                {
                    string val = searchValues is Utf8StringSeries svU8 ? svU8.GetString(i) : "";
                    resSpan[i] = Array.BinarySearch(srcStrings, val);
                    if (resSpan[i] < 0) resSpan[i] = ~resSpan[i];
                }
            }
            else
            {
                // Generic approach via Get() — slower but works for any type
                var resSpan = result.Memory.Span;
                var srcValues = new IComparable?[sourceLen];
                for (int s = 0; s < sourceLen; s++) srcValues[s] = (IComparable)source.Get(s);
                Array.Sort(srcValues);

                for (int i = 0; i < searchLen; i++)
                {
                    var val = (IComparable)searchValues.Get(i);
                    resSpan[i] = Array.BinarySearch(srcValues, val);
                    if (resSpan[i] < 0) resSpan[i] = ~resSpan[i];
                }
            }

            return result;
        }

        /// <summary>Extract a contiguous slice of a series. Supports negative offset.</summary>
        public static ISeries SliceSeries(ISeries source, int offset, int? length = null)
        {
            int srcLen = source.Length;
            if (offset < 0) offset = Math.Max(0, srcLen + offset);
            if (offset >= srcLen) return source.CloneEmpty(0);

            int actualLength = length.HasValue ? Math.Min(length.Value, srcLen - offset) : srcLen - offset;
            if (actualLength <= 0) return source.CloneEmpty(0);

            int[] indices = new int[actualLength];
            for (int i = 0; i < actualLength; i++) indices[i] = offset + i;

            return ComputeKernels.Take(source, indices);
        }
        /// <summary>Return the top k values (largest) from a series.</summary>
        public static ISeries TopKSeries(ISeries source, int k)
        {
            if (k <= 0) return source.CloneEmpty(0);
            int length = source.Length;
            if (k >= length)
            {
                // Clone to avoid mutating the original series when an alias is applied later
                var result = source.CloneEmpty(length);
                source.CopyTo(result, 0);
                return result;
            }

            int[] sortedIndices;
            if (source is Int32Series i32)
            {
                sortedIndices = SortKernels.ArgSort(i32.Memory.Span);
                Array.Reverse(sortedIndices); // descending
            }
            else if (source is Float64Series f64)
            {
                sortedIndices = SortKernels.ArgSort(f64.Memory.Span);
                Array.Reverse(sortedIndices);
            }
            else
            {
                // Use DataFrame-based sort
                var df = new DataFrame(new[] { source });
                sortedIndices = SortKernels.MultiColumnSort(df, new[] { source.Name }, new[] { true });
            }

            var topIndices = sortedIndices.Take(k).ToArray();
            return ComputeKernels.Take(source, topIndices);
        }
        /// <summary>Return the bottom k values (smallest) from a series.</summary>
        public static ISeries BottomKSeries(ISeries source, int k)
        {
            if (k <= 0) return source.CloneEmpty(0);
            int length = source.Length;
            if (k >= length)
            {
                // Clone to avoid mutating the original series when an alias is applied later
                var result = source.CloneEmpty(length);
                source.CopyTo(result, 0);
                return result;
            }

            int[] sortedIndices;
            if (source is Int32Series i32)
            {
                sortedIndices = SortKernels.ArgSort(i32.Memory.Span);
            }
            else if (source is Float64Series f64)
            {
                sortedIndices = SortKernels.ArgSort(f64.Memory.Span);
            }
            else
            {
                var df = new DataFrame(new[] { source });
                sortedIndices = SortKernels.MultiColumnSort(df, new[] { source.Name }, new[] { false });
            }

            var bottomIndices = sortedIndices.Take(k).ToArray();
            return ComputeKernels.Take(source, bottomIndices);
        }
        /// <summary>
        /// Bit-reinterpret a numeric series to another numeric type without value conversion.
        /// Supported pairs: Int64 ↔ Float64, Int32 ↔ Float32, UInt64 → Float64, UInt32 → Float32.
        /// Matches Python Polars Series.reinterpret() / cast() with reinterpret=True semantics.
        /// </summary>
        public static ISeries Reinterpret(ISeries source, Type targetType)
        {
            int n = source.Length;
            string name = source.Name;

            // Int64 → Float64
            if (source is Data.Int64Series i64 && targetType == typeof(double))
            {
                var result = new Data.Float64Series(name, n);
                var src = i64.Memory.Span;
                var dst = result.Memory.Span;
                System.Runtime.InteropServices.MemoryMarshal.Cast<long, double>(src).CopyTo(dst);
                for (int i = 0; i < n; i++)
                    if (!i64.ValidityMask.IsValid(i)) result.ValidityMask.SetNull(i);
                return result;
            }
            // Float64 → Int64
            if (source is Data.Float64Series f64 && targetType == typeof(long))
            {
                var result = new Data.Int64Series(name, n);
                var src = f64.Memory.Span;
                var dst = result.Memory.Span;
                System.Runtime.InteropServices.MemoryMarshal.Cast<double, long>(src).CopyTo(dst);
                for (int i = 0; i < n; i++)
                    if (!f64.ValidityMask.IsValid(i)) result.ValidityMask.SetNull(i);
                return result;
            }
            // Int32 → Float32
            if (source is Data.Int32Series i32 && targetType == typeof(float))
            {
                var result = new Data.Float32Series(name, n);
                var src = i32.Memory.Span;
                var dst = result.Memory.Span;
                System.Runtime.InteropServices.MemoryMarshal.Cast<int, float>(src).CopyTo(dst);
                for (int i = 0; i < n; i++)
                    if (!i32.ValidityMask.IsValid(i)) result.ValidityMask.SetNull(i);
                return result;
            }
            // Float32 → Int32
            if (source is Data.Float32Series f32 && targetType == typeof(int))
            {
                var result = new Data.Int32Series(name, n);
                var src = f32.Memory.Span;
                var dst = result.Memory.Span;
                System.Runtime.InteropServices.MemoryMarshal.Cast<float, int>(src).CopyTo(dst);
                for (int i = 0; i < n; i++)
                    if (!f32.ValidityMask.IsValid(i)) result.ValidityMask.SetNull(i);
                return result;
            }
            // UInt64 → Float64 (Polars signed=False)
            if (source is Data.UInt64Series u64 && targetType == typeof(double))
            {
                var result = new Data.Float64Series(name, n);
                var src = u64.Memory.Span;
                var dst = result.Memory.Span;
                System.Runtime.InteropServices.MemoryMarshal.Cast<ulong, double>(src).CopyTo(dst);
                for (int i = 0; i < n; i++)
                    if (!u64.ValidityMask.IsValid(i)) result.ValidityMask.SetNull(i);
                return result;
            }

            throw new NotSupportedException(
                $"Reinterpret from {source.GetType().Name} to {targetType.Name} is not supported.");
        }
    }
}
