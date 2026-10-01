using System;
using System.Linq;
using Glacier.Polaris.Data;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris
{
    public static class SeriesExtensions
    {
        public static ISeries Min(this ISeries series) => AggregationKernels.Min(series);
        public static ISeries Max(this ISeries series) => AggregationKernels.Max(series);
        public static ISeries Sum(this ISeries series) => AggregationKernels.Sum(series);
        public static ISeries Mean(this ISeries series) => AggregationKernels.Mean(series);
        public static ISeries Std(this ISeries series) => AggregationKernels.Std(series);
        public static ISeries Var(this ISeries series) => AggregationKernels.Var(series);
        public static ISeries Median(this ISeries series) => AggregationKernels.Median(series);
        public static ISeries Count(this ISeries series) => AggregationKernels.Count(series);
        public static ISeries NUnique(this ISeries series) => Compute.UniqueKernels.NUnique(series);
        public static ISeries Unique(this ISeries series) => Compute.UniqueKernels.Unique(series);
        public static ISeries Quantile(this ISeries series, double quantile) => AggregationKernels.Quantile(series, quantile);
        public static ISeries All(this ISeries series) => AggregationKernels.All(series);
        public static ISeries Any(this ISeries series) => AggregationKernels.Any(series);
        public static ISeries IsIn(this ISeries series, System.Collections.IEnumerable values) => PredicateKernels.IsIn(series, values);
        public static ISeries IsIn(this ISeries series, ISeries targetSeries) => PredicateKernels.IsIn(series, targetSeries);
        public static ISeries IsBetween(this ISeries series, object lower, object upper, string closed = "both") => PredicateKernels.IsBetween(series, lower, upper, closed);
        public static ISeries IsBetween(this ISeries series, ISeries lower, ISeries upper, string closed = "both") => PredicateKernels.IsBetween(series, lower, upper, closed);
        public static ISeries IsNan(this ISeries series) => PredicateKernels.IsNan(series);
        public static ISeries IsNotNan(this ISeries series) => PredicateKernels.IsNotNan(series);
        public static ISeries IsFinite(this ISeries series) => PredicateKernels.IsFinite(series);
        public static ISeries IsInfinite(this ISeries series) => PredicateKernels.IsInfinite(series);

        // Cumulative Reductions
        public static ISeries CumSum(this ISeries series) => WindowKernels.ExpandingSum(series);
        public static ISeries CumMean(this ISeries series) => WindowKernels.ExpandingMean(series);
        public static ISeries CumMin(this ISeries series) => WindowKernels.ExpandingMin(series);
        public static ISeries CumMax(this ISeries series) => WindowKernels.ExpandingMax(series);
        public static ISeries CumProd(this ISeries series, bool reverse = false) => WindowKernels.ExpandingProd(series, reverse);
        public static ISeries CumCount(this ISeries series, bool reverse = false) => WindowKernels.ExpandingCount(series, reverse);

        // Array & Math Ergonomics
        public static ISeries Shift(this ISeries series, int n) => ArrayKernels.Shift(series, n);
        public static ISeries Diff(this ISeries series, int n = 1) => ArrayKernels.Diff(series, n);
        public static ISeries PctChange(this ISeries series, int n = 1) => MathKernels.PctChange(series, n);
        public static ISeries Rank(this ISeries series, bool descending = false) => MathKernels.Rank(series, descending);
        public static ISeries Abs(this ISeries series) => ArrayKernels.Abs(series);
        public static ISeries Clip(this ISeries series, double min, double max) => ArrayKernels.Clip(series, min, max);
        public static ISeries Round(this ISeries series, int decimals = 0) => MathKernels.Round(series, decimals);
        public static ISeries Sign(this ISeries series) => MathKernels.Sign(series);
        public static ISeries Pow(this ISeries series, double exponent) => MathKernels.Pow(series, exponent);
        public static ISeries Pow(this ISeries series, ISeries exponent) => MathKernels.Pow(series, exponent);
        public static ISeries Log1p(this ISeries series) => MathKernels.Log1p(series);
        public static ISeries Cbrt(this ISeries series) => MathKernels.Cbrt(series);
        public static ISeries Dot(this ISeries series, ISeries other) => MathKernels.Dot(series, other);
        public static ISeries Head(this ISeries series, int n = 5) => ArrayKernels.SliceSeries(series, 0, Math.Min(n, series.Length));
        public static ISeries Tail(this ISeries series, int n = 5)
        {
            int start = Math.Max(0, series.Length - n);
            int len = series.Length - start;
            return ArrayKernels.SliceSeries(series, start, len);
        }

        public static ISeries Clone(this ISeries col)
        {
            ISeries newCol;
            if (col is Utf8StringSeries u8)
            {
                newCol = new Utf8StringSeries(col.Name, col.Length, u8.DataBytes.Length);
                u8.Offsets.Span.CopyTo(((Utf8StringSeries)newCol).Offsets.Span);
                u8.DataBytes.Span.CopyTo(((Utf8StringSeries)newCol).DataBytes.Span);
            }
            else
            {
                newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, col.Length)!;
                for (int i = 0; i < col.Length; i++) col.Take(newCol, i, i);
            }
            newCol.ValidityMask.CopyFrom(col.ValidityMask);
            return newCol;
        }

        public static ISeries FillNull(this ISeries series, FillStrategy strategy) => FillNullKernels.FillNull(series, strategy);
        public static ISeries FillNull(this ISeries series, object value)
        {
            if (value is ISeries valSeries) return FillNullKernels.FillWithValue(series, valSeries);

            // Handle literals by creating a 1-length series and broadcasting (simplified)
            ISeries literalSeries;
            if (value is int i) literalSeries = new Int32Series("literal", 1) { [0] = i };
            else if (value is double d) literalSeries = new Float64Series("literal", 1) { [0] = d };
            else if (value is string s) literalSeries = Utf8StringSeries.FromStrings("literal", new[] { s });
            else throw new NotSupportedException($"Literal type {value.GetType().Name} not supported in FillNull.");

            return FillNullKernels.FillWithValue(series, literalSeries);
        }
        public static ISeries NullCount(this ISeries series) => AggregationKernels.NullCount(series);
public static ISeries ArgMin(this ISeries series) => AggregationKernels.ArgMin(series);
public static ISeries ArgMax(this ISeries series) => AggregationKernels.ArgMax(series);
        public static ISeries Cast(this ISeries series, Type targetType)
        {
            // Simplified cast logic
            if (series.DataType == targetType) return series;

            if (targetType == typeof(double))
            {
                if (series is Int32Series i32)
                {
                    var result = new Float64Series(series.Name, series.Length);
                    for (int i = 0; i < series.Length; i++)
                    {
                        if (series.ValidityMask.IsValid(i)) result.Memory.Span[i] = (double)i32.Memory.Span[i];
                        else result.ValidityMask.SetNull(i);
                    }
                    return result;
                }
            }
            // Add more as needed
            throw new NotSupportedException($"Casting {series.DataType.Name} to {targetType.Name} not implemented.");
        }

        public static ISeries StartsWith(this ISeries series, string prefix)
        {
            if (series is Utf8StringSeries u8)
            {
                var tmp = new int[series.Length];
                StringKernels.StartsWith(u8.DataBytes.Span, u8.Offsets.Span, prefix, tmp);
                var result = new BooleanSeries(series.Name + "_starts_with", series.Length);
                for (int i = 0; i < series.Length; i++) result.Memory.Span[i] = tmp[i] != 0;
                result.ValidityMask.CopyFrom(u8.ValidityMask);
                return result;
            }
            throw new InvalidOperationException("StartsWith only supported for Utf8StringSeries.");
        }

        public static ISeries EndsWith(this ISeries series, string suffix)
        {
            if (series is Utf8StringSeries u8)
            {
                var tmp = new int[series.Length];
                StringKernels.EndsWith(u8.DataBytes.Span, u8.Offsets.Span, suffix, tmp);
                var result = new BooleanSeries(series.Name + "_ends_with", series.Length);
                for (int i = 0; i < series.Length; i++) result.Memory.Span[i] = tmp[i] != 0;
                result.ValidityMask.CopyFrom(u8.ValidityMask);
                return result;
            }
            throw new InvalidOperationException("EndsWith only supported for Utf8StringSeries.");
        }

        /// <summary>Compute a histogram of the series with the specified number of bins.</summary>
        public static DataFrame Hist(this ISeries series, int bins)
        {
            return Compute.AnalyticalKernels.Histogram(series, bins);
        }

        /// <summary>Compute Kernel Density Estimation (KDE) of the series.</summary>
        public static DataFrame Kde(this ISeries series, double bandwidth, int gridPoints = 100)
        {
            return Compute.AnalyticalKernels.Kde(series, bandwidth, gridPoints);
        }
    }
}
