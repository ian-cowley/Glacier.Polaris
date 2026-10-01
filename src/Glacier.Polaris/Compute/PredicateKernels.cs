using System;
using System.Collections;
using System.Collections.Generic;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class PredicateKernels
    {
        public static BooleanSeries IsIn(ISeries series, IEnumerable values)
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isin", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            if (series is Int32Series i32)
            {
                var set = new HashSet<int>();
                foreach (var v in values)
                {
                    if (v is int valInt) set.Add(valInt);
                    else if (v != null && int.TryParse(v.ToString(), out int parsed)) set.Add(parsed);
                }

                var span = i32.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        resSpan[i] = set.Contains(span[i]);
                    }
                }
                return result;
            }

            if (series is Int64Series i64)
            {
                var set = new HashSet<long>();
                foreach (var v in values)
                {
                    if (v is long valLong) set.Add(valLong);
                    else if (v != null && long.TryParse(v.ToString(), out long parsed)) set.Add(parsed);
                }

                var span = i64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        resSpan[i] = set.Contains(span[i]);
                    }
                }
                return result;
            }

            if (series is Float64Series f64)
            {
                var set = new HashSet<double>();
                foreach (var v in values)
                {
                    if (v is double valDbl) set.Add(valDbl);
                    else if (v != null && double.TryParse(v.ToString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double parsed)) set.Add(parsed);
                }

                var span = f64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        resSpan[i] = set.Contains(span[i]);
                    }
                }
                return result;
            }

            if (series is Utf8StringSeries u8)
            {
                var set = new HashSet<string>(StringComparer.Ordinal);
                foreach (var v in values)
                {
                    if (v != null) set.Add(v.ToString()!);
                }

                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        resSpan[i] = set.Contains(u8.GetString(i));
                    }
                }
                return result;
            }

            // Generic fallback
            var objSet = new HashSet<object>();
            foreach (var v in values)
            {
                if (v != null) objSet.Add(v);
            }

            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i))
                {
                    resMask.SetNull(i);
                }
                else
                {
                    var val = series.Get(i);
                    resSpan[i] = val != null && objSet.Contains(val);
                }
            }
            return result;
        }

        public static BooleanSeries IsIn(ISeries series, ISeries targetSeries)
        {
            var list = new List<object?>(targetSeries.Length);
            for (int i = 0; i < targetSeries.Length; i++)
            {
                if (targetSeries.ValidityMask.IsValid(i))
                    list.Add(targetSeries.Get(i));
            }
            return IsIn(series, list);
        }

        public static BooleanSeries IsBetween(ISeries series, object lower, object upper, string closed = "both")
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isbetween", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            double lowDbl = Convert.ToDouble(lower, System.Globalization.CultureInfo.InvariantCulture);
            double highDbl = Convert.ToDouble(upper, System.Globalization.CultureInfo.InvariantCulture);

            bool incLow = closed.Equals("both", StringComparison.OrdinalIgnoreCase) || closed.Equals("left", StringComparison.OrdinalIgnoreCase);
            bool incHigh = closed.Equals("both", StringComparison.OrdinalIgnoreCase) || closed.Equals("right", StringComparison.OrdinalIgnoreCase);

            if (series is Int32Series i32)
            {
                var span = i32.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        double v = span[i];
                        bool geLow = incLow ? v >= lowDbl : v > lowDbl;
                        bool leHigh = incHigh ? v <= highDbl : v < highDbl;
                        resSpan[i] = geLow && leHigh;
                    }
                }
                return result;
            }

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        double v = span[i];
                        bool geLow = incLow ? v >= lowDbl : v > lowDbl;
                        bool leHigh = incHigh ? v <= highDbl : v < highDbl;
                        resSpan[i] = geLow && leHigh;
                    }
                }
                return result;
            }

            if (series is Int64Series i64)
            {
                var span = i64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i))
                    {
                        resMask.SetNull(i);
                    }
                    else
                    {
                        double v = span[i];
                        bool geLow = incLow ? v >= lowDbl : v > lowDbl;
                        bool leHigh = incHigh ? v <= highDbl : v < highDbl;
                        resSpan[i] = geLow && leHigh;
                    }
                }
                return result;
            }

            // General numeric / comparable fallback
            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i))
                {
                    resMask.SetNull(i);
                }
                else
                {
                    double v = Convert.ToDouble(series.Get(i), System.Globalization.CultureInfo.InvariantCulture);
                    bool geLow = incLow ? v >= lowDbl : v > lowDbl;
                    bool leHigh = incHigh ? v <= highDbl : v < highDbl;
                    resSpan[i] = geLow && leHigh;
                }
            }
            return result;
        }

        public static BooleanSeries IsBetween(ISeries series, ISeries lower, ISeries upper, string closed = "both")
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isbetween", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            bool incLow = closed.Equals("both", StringComparison.OrdinalIgnoreCase) || closed.Equals("left", StringComparison.OrdinalIgnoreCase);
            bool incHigh = closed.Equals("both", StringComparison.OrdinalIgnoreCase) || closed.Equals("right", StringComparison.OrdinalIgnoreCase);

            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i) || lower.ValidityMask.IsNull(i) || upper.ValidityMask.IsNull(i))
                {
                    resMask.SetNull(i);
                }
                else
                {
                    double v = Convert.ToDouble(series.Get(i), System.Globalization.CultureInfo.InvariantCulture);
                    double low = Convert.ToDouble(lower.Get(i), System.Globalization.CultureInfo.InvariantCulture);
                    double high = Convert.ToDouble(upper.Get(i), System.Globalization.CultureInfo.InvariantCulture);
                    bool geLow = incLow ? v >= low : v > low;
                    bool leHigh = incHigh ? v <= high : v < high;
                    resSpan[i] = geLow && leHigh;
                }
            }
            return result;
        }

        public static BooleanSeries IsNan(ISeries series)
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isnan", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else resSpan[i] = double.IsNaN(span[i]);
                }
                return result;
            }

            if (series is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else resSpan[i] = float.IsNaN(span[i]);
                }
                return result;
            }

            // Non-float types cannot be NaN
            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i)) resMask.SetNull(i);
                else resSpan[i] = false;
            }
            return result;
        }

        public static BooleanSeries IsNotNan(ISeries series)
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isnotnan", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else resSpan[i] = !double.IsNaN(span[i]);
                }
                return result;
            }

            if (series is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else resSpan[i] = !float.IsNaN(span[i]);
                }
                return result;
            }

            // Non-float types are never NaN
            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i)) resMask.SetNull(i);
                else resSpan[i] = true;
            }
            return result;
        }

        public static BooleanSeries IsFinite(ISeries series)
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isfinite", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else
                    {
                        double v = span[i];
                        resSpan[i] = !double.IsNaN(v) && !double.IsInfinity(v);
                    }
                }
                return result;
            }

            if (series is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else
                    {
                        float v = span[i];
                        resSpan[i] = !float.IsNaN(v) && !float.IsInfinity(v);
                    }
                }
                return result;
            }

            // Non-float types are always finite
            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i)) resMask.SetNull(i);
                else resSpan[i] = true;
            }
            return result;
        }

        public static BooleanSeries IsInfinite(ISeries series)
        {
            int length = series.Length;
            var result = new BooleanSeries(series.Name + "_isinfinite", length);
            var resSpan = result.Memory.Span;
            var mask = series.ValidityMask;
            var resMask = result.ValidityMask;

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else resSpan[i] = double.IsInfinity(span[i]);
                }
                return result;
            }

            if (series is Float32Series f32)
            {
                var span = f32.Memory.Span;
                for (int i = 0; i < length; i++)
                {
                    if (mask.IsNull(i)) resMask.SetNull(i);
                    else resSpan[i] = float.IsInfinity(span[i]);
                }
                return result;
            }

            // Non-float types are never infinite
            for (int i = 0; i < length; i++)
            {
                if (mask.IsNull(i)) resMask.SetNull(i);
                else resSpan[i] = false;
            }
            return result;
        }
    }
}
