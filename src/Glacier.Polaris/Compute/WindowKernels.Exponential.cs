using System;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class WindowKernels
    {
        public static Float64Series EWMMean(ISeries source, double alpha)
        {
            if (source is Float64Series f64)
                return EWMMeanF64(f64, alpha);
            if (source is Int32Series i32)
                return EWMMeanI32(i32, alpha);

            var result = new Float64Series(source.Name, source.Length);
            double lastVal = GetValueAsDouble(source, 0);
            result.Memory.Span[0] = lastVal;
            for (int i = 1; i < source.Length; i++)
            {
                double current = GetValueAsDouble(source, i);
                lastVal = (alpha * current) + (1 - alpha) * lastVal;
                result.Memory.Span[i] = lastVal;
            }
            return result;
        }

        private static Float64Series EWMMeanF64(Float64Series source, double alpha)
        {
            var result = new Float64Series(source.Name, source.Length);
            var span = source.Memory.Span;
            var res = result.Memory.Span;
            double lastVal = span[0];
            res[0] = lastVal;
            for (int i = 1; i < source.Length; i++)
            {
                lastVal = (alpha * span[i]) + (1 - alpha) * lastVal;
                res[i] = lastVal;
            }
            return result;
        }

        private static Float64Series EWMMeanI32(Int32Series source, double alpha)
        {
            var result = new Float64Series(source.Name, source.Length);
            var span = source.Memory.Span;
            var res = result.Memory.Span;
            double lastVal = span[0];
            res[0] = lastVal;
            for (int i = 1; i < source.Length; i++)
            {
                lastVal = (alpha * span[i]) + (1 - alpha) * lastVal;
                res[i] = lastVal;
            }
            return result;
        }

        private static double GetValueAsDouble(ISeries s, int i)
        {
            if (s is Int32Series i32) return i32.Memory.Span[i];
            if (s is Float64Series f64) return f64.Memory.Span[i];
            var val = s.Get(i);
            return val != null ? Convert.ToDouble(val) : 0;
        }

        /// <summary>Exponentially weighted moving standard deviation.</summary>
        public static Data.Float64Series EWMStd(ISeries source, double alpha)
        {
            var mean = EWMMean(source, alpha);
            int len = source.Length;
            var result = new Data.Float64Series(source.Name + "_ewm_std", len);
            var res = result.Memory.Span;

            if (source is Data.Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (f64.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    double si = 0;
                    double wSumi = 0;
                    double wSqSumi = 0;
                    for (int j = 0; j <= i; j++)
                    {
                        if (f64.ValidityMask.IsNull(j)) continue;
                        double w = (j == 0) ? Math.Pow(1 - alpha, i) : alpha * Math.Pow(1 - alpha, i - j);
                        si += w * span[j] * span[j];
                        wSumi += w;
                        wSqSumi += w * w;
                    }
                    if (wSumi * wSumi > wSqSumi)
                    {
                        double eX2i = si / wSumi;
                        double eXi = mean.Memory.Span[i];
                        double variance = eX2i - eXi * eXi;
                        double factor = (wSumi * wSumi) / (wSumi * wSumi - wSqSumi);
                        res[i] = Math.Sqrt(Math.Max(0, variance * factor));
                    }
                }
            }
            else if (source is Data.Int32Series i32)
            {
                var span = i32.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (i32.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    double si = 0;
                    double wSumi = 0;
                    double wSqSumi = 0;
                    for (int j = 0; j <= i; j++)
                    {
                        if (i32.ValidityMask.IsNull(j)) continue;
                        double w = (j == 0) ? Math.Pow(1 - alpha, i) : alpha * Math.Pow(1 - alpha, i - j);
                        si += w * span[j] * span[j];
                        wSumi += w;
                        wSqSumi += w * w;
                    }
                    if (wSumi * wSumi > wSqSumi)
                    {
                        double eX2i = si / wSumi;
                        double eXi = mean.Memory.Span[i];
                        double variance = eX2i - eXi * eXi;
                        double factor = (wSumi * wSumi) / (wSumi * wSumi - wSqSumi);
                        res[i] = Math.Sqrt(Math.Max(0, variance * factor));
                    }
                }
            }
            else if (source is Data.Int64Series i64)
            {
                var span = i64.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (i64.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    double si = 0;
                    double wSumi = 0;
                    double wSqSumi = 0;
                    for (int j = 0; j <= i; j++)
                    {
                        if (i64.ValidityMask.IsNull(j)) continue;
                        double w = (j == 0) ? Math.Pow(1 - alpha, i) : alpha * Math.Pow(1 - alpha, i - j);
                        si += w * span[j] * span[j];
                        wSumi += w;
                        wSqSumi += w * w;
                    }
                    if (wSumi * wSumi > wSqSumi)
                    {
                        double eX2i = si / wSumi;
                        double eXi = mean.Memory.Span[i];
                        double variance = eX2i - eXi * eXi;
                        double factor = (wSumi * wSumi) / (wSumi * wSumi - wSqSumi);
                        res[i] = Math.Sqrt(Math.Max(0, variance * factor));
                    }
                }
            }
            else
            {
                throw new NotSupportedException($"EWMStd not supported for {source.DataType.Name}");
            }
            return result;
        }
    }
}
