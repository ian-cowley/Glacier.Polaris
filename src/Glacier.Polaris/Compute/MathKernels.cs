using System;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class MathKernels
    {
        public static ISeries Sqrt(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series i8)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i8.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(i8.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int16Series i16)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i16.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(i16.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is UInt8Series u8)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (u8.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(u8.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is UInt16Series u16)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (u16.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(u16.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is UInt32Series u32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (u32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(u32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is UInt64Series u64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (u64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sqrt(u64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            throw new NotSupportedException($"Sqrt not supported for {source.GetType().Name}");
        }

        public static ISeries Log(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series)
            {
                // Promote to Float64 first
                return Log(PromoteToFloat64(source));
            }
            throw new NotSupportedException($"Log not supported for {source.GetType().Name}");
        }

        public static ISeries Log10(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log10(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log10(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log10(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Log10(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series)
            {
                return Log10(PromoteToFloat64(source));
            }
            throw new NotSupportedException($"Log10 not supported for {source.GetType().Name}");
        }

        public static ISeries Exp(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Exp(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Exp(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Exp(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Exp(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series || source is UInt64Series)
            {
                return Exp(PromoteToFloat64(source));
            }
            throw new NotSupportedException($"Exp not supported for {source.GetType().Name}");
        }

        public static ISeries Sin(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sin(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sin(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sin(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Sin(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series || source is UInt64Series)
            {
                return Sin(PromoteToFloat64(source));
            }
            throw new NotSupportedException($"Sin not supported for {source.GetType().Name}");
        }

        public static ISeries Cos(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Cos(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Cos(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Cos(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Cos(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series || source is UInt64Series)
            {
                return Cos(PromoteToFloat64(source));
            }
            throw new NotSupportedException($"Cos not supported for {source.GetType().Name}");
        }

        public static ISeries Tan(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Tan(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Tan(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Tan(i32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Tan(i64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series || source is UInt64Series)
            {
                return Tan(PromoteToFloat64(source));
            }
            throw new NotSupportedException($"Tan not supported for {source.GetType().Name}");
        }

        public static ISeries PctChange(ISeries source, int n = 1)
        {
            // pct_change(n) = (x_i - x_{i-n}) / x_{i-n}
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            for (int i = 0; i < n && i < length; i++)
                result.ValidityMask.SetNull(i);

            if (source is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i) && f64.ValidityMask.IsValid(i - n) && span[i - n] != 0)
                        result.Memory.Span[i] = (span[i] - span[i - n]) / span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i) && f32.ValidityMask.IsValid(i - n) && span[i - n] != 0)
                        result.Memory.Span[i] = (span[i] - span[i - n]) / span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var span = i32.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (i32.ValidityMask.IsValid(i) && i32.ValidityMask.IsValid(i - n) && span[i - n] != 0)
                        result.Memory.Span[i] = (double)(span[i] - span[i - n]) / span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int64Series i64)
            {
                var span = i64.Memory.Span;
                for (int i = n; i < length; i++)
                {
                    if (i64.ValidityMask.IsValid(i) && i64.ValidityMask.IsValid(i - n) && span[i - n] != 0)
                        result.Memory.Span[i] = (double)(span[i] - span[i - n]) / span[i - n];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int8Series || source is Int16Series || source is UInt8Series || source is UInt16Series || source is UInt32Series)
            {
                return PctChange(PromoteToFloat64(source), n);
            }
            throw new NotSupportedException($"PctChange not supported for {source.GetType().Name}");
        }
        /// <summary>Compute average rank (matching Polars default rank behavior).</summary>
        public static ISeries Rank(ISeries source, bool descending = false)
        {
            int length = source.Length;
            var indices = new int[length];
            for (int i = 0; i < length; i++) indices[i] = i;

            // Get values as doubles for comparison
            var values = new double[length];
            bool[] isNull = new bool[length];
            for (int i = 0; i < length; i++)
            {
                if (source.ValidityMask.IsValid(i))
                    values[i] = Convert.ToDouble(source.Get(i));
                else
                    isNull[i] = true;
            }

            // If descending, negate values so that sort gives descending order
            if (descending)
            {
                for (int i = 0; i < length; i++)
                    if (!isNull[i]) values[i] = -values[i];
            }

            // Sort indices by value
            Array.Sort(values, indices);

            // Compute average rank (1-based, tied values get the average of their positions)
            var ranks = new double[length];
            for (int i = 0; i < length;)
            {
                if (isNull[indices[i]]) { i++; continue; }
                int j = i;
                while (j < length && !isNull[indices[j]] && values[j] == values[i])
                    j++;
                // Average rank for this group
                double avgRank = (i + 1 + j) / 2.0;
                for (int k = i; k < j; k++)
                    ranks[indices[k]] = avgRank;
                i = j;
            }

            var result = new Float64Series(source.Name + "_rank", length);
            for (int i = 0; i < length; i++)
            {
                if (isNull[i])
                    result.ValidityMask.SetNull(i);
                else
                    result.Memory.Span[i] = ranks[i];
            }
            return result;
        }
        private static Float64Series PromoteToFloat64(ISeries source)
        {
            var result = new Float64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                result.Memory.Span[i] = Convert.ToDouble(source.Get(i));
            }
            return result;
        }
        public static ISeries Floor(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Floor(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Floor(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                // Already integer, identity
                var result = new Int32Series(source.Name, length);
                i32.Memory.Span.CopyTo(result.Memory.Span);
                result.ValidityMask.CopyFrom(i32.ValidityMask);
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, length);
                i64.Memory.Span.CopyTo(result.Memory.Span);
                result.ValidityMask.CopyFrom(i64.ValidityMask);
                return result;
            }
            throw new NotSupportedException($"Floor not supported for {source.GetType().Name}");
        }

        public static ISeries Ceil(ISeries source)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Ceiling(f64.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Ceiling(f32.Memory.Span[i]);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, length);
                i32.Memory.Span.CopyTo(result.Memory.Span);
                result.ValidityMask.CopyFrom(i32.ValidityMask);
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, length);
                i64.Memory.Span.CopyTo(result.Memory.Span);
                result.ValidityMask.CopyFrom(i64.ValidityMask);
                return result;
            }
            throw new NotSupportedException($"Ceil not supported for {source.GetType().Name}");
        }

        public static ISeries Round(ISeries source)
        {
            return Round(source, 0);
        }

        public static ISeries Round(ISeries source, int decimals)
        {
            int length = source.Length;
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Round(f64.Memory.Span[i], decimals, MidpointRounding.ToEven);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Float32Series f32)
            {
                var result = new Float64Series(source.Name, length);
                for (int i = 0; i < length; i++)
                {
                    if (f32.ValidityMask.IsValid(i))
                        result.Memory.Span[i] = Math.Round(f32.Memory.Span[i], decimals, MidpointRounding.ToEven);
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, length);
                i32.Memory.Span.CopyTo(result.Memory.Span);
                result.ValidityMask.CopyFrom(i32.ValidityMask);
                return result;
            }
            if (source is Int64Series i64)
            {
                var result = new Int64Series(source.Name, length);
                i64.Memory.Span.CopyTo(result.Memory.Span);
                result.ValidityMask.CopyFrom(i64.ValidityMask);
                return result;
            }
            throw new NotSupportedException($"Round not supported for {source.GetType().Name}");
        }
    }
}
