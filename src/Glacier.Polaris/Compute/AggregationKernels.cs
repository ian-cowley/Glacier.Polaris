using System;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class AggregationKernels
    {
        public static ISeries Min(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int min = int.MaxValue;
                bool found = false;
                var span = i32.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        min = Math.Min(min, span[i]);
                        found = true;
                    }
                }
                var result = new Int32Series(series.Name + "_min", 1);
                if (found) result.Memory.Span[0] = min;
                else result.ValidityMask.SetNull(0);
                return result;
            }
            if (series is Float64Series f64)
            {
                double min = double.MaxValue;
                bool found = false;
                var span = f64.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        min = Math.Min(min, span[i]);
                        found = true;
                    }
                }
                var result = new Float64Series(series.Name + "_min", 1);
                if (found) result.Memory.Span[0] = min;
                else result.ValidityMask.SetNull(0);
                return result;
            }
            return new NullSeries(series.Name + "_min", 1);
        }

        public static ISeries Max(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int max = int.MinValue;
                bool found = false;
                var span = i32.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        max = Math.Max(max, span[i]);
                        found = true;
                    }
                }
                var result = new Int32Series(series.Name + "_max", 1);
                if (found) result.Memory.Span[0] = max;
                else result.ValidityMask.SetNull(0);
                return result;
            }
            if (series is Float64Series f64)
            {
                double max = double.MinValue;
                bool found = false;
                var span = f64.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        max = Math.Max(max, span[i]);
                        found = true;
                    }
                }
                var result = new Float64Series(series.Name + "_max", 1);
                if (found) result.Memory.Span[0] = max;
                else result.ValidityMask.SetNull(0);
                return result;
            }
            return new NullSeries(series.Name + "_max", 1);
        }

        public static BooleanSeries All(ISeries series)
        {
            var result = new BooleanSeries(series.Name + "_all", 1);
            if (series.Length == 0)
            {
                result.Memory.Span[0] = true;
                return result;
            }

            if (series is BooleanSeries bs)
            {
                var span = bs.Memory.Span;
                var mask = bs.ValidityMask;
                for (int i = 0; i < series.Length; i++)
                {
                    if (mask.IsValid(i) && !span[i])
                    {
                        result.Memory.Span[0] = false;
                        return result;
                    }
                }
                result.Memory.Span[0] = true;
                return result;
            }

            if (series is Int32Series i32)
            {
                var span = i32.Memory.Span;
                var mask = i32.ValidityMask;
                for (int i = 0; i < series.Length; i++)
                {
                    if (mask.IsValid(i) && span[i] == 0)
                    {
                        result.Memory.Span[0] = false;
                        return result;
                    }
                }
                result.Memory.Span[0] = true;
                return result;
            }

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                for (int i = 0; i < series.Length; i++)
                {
                    if (mask.IsValid(i) && (span[i] == 0.0 || double.IsNaN(span[i])))
                    {
                        result.Memory.Span[0] = false;
                        return result;
                    }
                }
                result.Memory.Span[0] = true;
                return result;
            }

            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    var val = series.Get(i);
                    bool truthy = val switch
                    {
                        bool b => b,
                        int n => n != 0,
                        long l => l != 0,
                        double d => d != 0.0 && !double.IsNaN(d),
                        string s => !string.IsNullOrEmpty(s),
                        _ => val != null
                    };
                    if (!truthy)
                    {
                        result.Memory.Span[0] = false;
                        return result;
                    }
                }
            }
            result.Memory.Span[0] = true;
            return result;
        }

        public static BooleanSeries Any(ISeries series)
        {
            var result = new BooleanSeries(series.Name + "_any", 1);
            if (series.Length == 0)
            {
                result.Memory.Span[0] = false;
                return result;
            }

            if (series is BooleanSeries bs)
            {
                var span = bs.Memory.Span;
                var mask = bs.ValidityMask;
                for (int i = 0; i < series.Length; i++)
                {
                    if (mask.IsValid(i) && span[i])
                    {
                        result.Memory.Span[0] = true;
                        return result;
                    }
                }
                result.Memory.Span[0] = false;
                return result;
            }

            if (series is Int32Series i32)
            {
                var span = i32.Memory.Span;
                var mask = i32.ValidityMask;
                for (int i = 0; i < series.Length; i++)
                {
                    if (mask.IsValid(i) && span[i] != 0)
                    {
                        result.Memory.Span[0] = true;
                        return result;
                    }
                }
                result.Memory.Span[0] = false;
                return result;
            }

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                for (int i = 0; i < series.Length; i++)
                {
                    if (mask.IsValid(i) && span[i] != 0.0 && !double.IsNaN(span[i]))
                    {
                        result.Memory.Span[0] = true;
                        return result;
                    }
                }
                result.Memory.Span[0] = false;
                return result;
            }

            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    var val = series.Get(i);
                    bool truthy = val switch
                    {
                        bool b => b,
                        int n => n != 0,
                        long l => l != 0,
                        double d => d != 0.0 && !double.IsNaN(d),
                        string s => !string.IsNullOrEmpty(s),
                        _ => val != null
                    };
                    if (truthy)
                    {
                        result.Memory.Span[0] = true;
                        return result;
                    }
                }
            }
            result.Memory.Span[0] = false;
            return result;
        }

        public static ISeries Count(ISeries series)
        {
            int count = 0;
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsValid(i)) count++;
            }
            var result = new Int32Series(series.Name + "_count", 1);
            result.Memory.Span[0] = count;
            return result;
        }

        /// <summary>Returns the first non-null value.</summary>
        public static ISeries First(ISeries series)
        {
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    var result = series.CloneEmpty(1);
                    series.Take(result, i, 0);
                    return result;
                }
            }
            return new NullSeries(series.Name + "_first", 1);
        }

        /// <summary>Returns the last non-null value.</summary>
        public static ISeries Last(ISeries series)
        {
            for (int i = series.Length - 1; i >= 0; i--)
            {
                if (series.ValidityMask.IsValid(i))
                {
                    var result = series.CloneEmpty(1);
                    series.Take(result, i, 0);
                    return result;
                }
            }
            return new NullSeries(series.Name + "_last", 1);
        }

        public static ISeries NullCount(ISeries series)
        {
            // Count nulls by scanning the validity mask
            int nullCount = 0;
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i))
                    nullCount++;
            }
            return new Data.Int32Series("null_count", 1) { [0] = nullCount };
        }

        /// <summary>
        /// Returns the index of the first occurrence of the minimum value.
        /// If all values are null, returns a null series.
        /// </summary>
        public static ISeries ArgMin(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int min = int.MaxValue;
                int argMin = -1;
                var span = i32.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i) && span[i] < min)
                    {
                        min = span[i];
                        argMin = i;
                    }
                }
                var result = new Int32Series(series.Name + "_arg_min", 1);
                if (argMin >= 0)
                    result.Memory.Span[0] = argMin;
                else
                    result.ValidityMask.SetNull(0);
                return result;
            }
            if (series is Float64Series f64)
            {
                double min = double.MaxValue;
                int argMin = -1;
                var span = f64.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i) && span[i] < min)
                    {
                        min = span[i];
                        argMin = i;
                    }
                }
                var result = new Int32Series(series.Name + "_arg_min", 1);
                if (argMin >= 0)
                    result.Memory.Span[0] = argMin;
                else
                    result.ValidityMask.SetNull(0);
                return result;
            }
            return new NullSeries(series.Name + "_arg_min", 1);
        }

        /// <summary>
        /// Returns the index of the first occurrence of the maximum value.
        /// If all values are null, returns a null series.
        /// </summary>
        public static ISeries ArgMax(ISeries series)
        {
            if (series is Int32Series i32)
            {
                int max = int.MinValue;
                int argMax = -1;
                var span = i32.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i) && span[i] > max)
                    {
                        max = span[i];
                        argMax = i;
                    }
                }
                var result = new Int32Series(series.Name + "_arg_max", 1);
                if (argMax >= 0)
                    result.Memory.Span[0] = argMax;
                else
                    result.ValidityMask.SetNull(0);
                return result;
            }
            if (series is Float64Series f64)
            {
                double max = double.MinValue;
                int argMax = -1;
                var span = f64.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i) && span[i] > max)
                    {
                        max = span[i];
                        argMax = i;
                    }
                }
                var result = new Int32Series(series.Name + "_arg_max", 1);
                if (argMax >= 0)
                    result.Memory.Span[0] = argMax;
                else
                    result.ValidityMask.SetNull(0);
                return result;
            }
            return new NullSeries(series.Name + "_arg_max", 1);
        }
    }
}
