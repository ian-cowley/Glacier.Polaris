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
    }
}
