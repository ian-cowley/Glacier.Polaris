using System;
using System.Collections.Generic;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class WindowKernels
    {
        public static ISeries BroadcastOver(ISeries aggregatedValues, List<List<int>> groups, int totalLength)
        {
            if (aggregatedValues is Int32Series i32)
            {
                var result = new Int32Series(i32.Name, totalLength);
                var resSpan = result.Memory.Span;
                var aggSpan = i32.Memory.Span;
                for (int i = 0; i < groups.Count; i++)
                {
                    int val = aggSpan[i];
                    foreach (int idx in groups[i]) resSpan[idx] = val;
                }
                return result;
            }
            else if (aggregatedValues is Float64Series f64)
            {
                var result = new Float64Series(f64.Name, totalLength);
                var resSpan = result.Memory.Span;
                var aggSpan = f64.Memory.Span;
                for (int i = 0; i < groups.Count; i++)
                {
                    double val = aggSpan[i];
                    foreach (int idx in groups[i]) resSpan[idx] = val;
                }
                return result;
            }
            throw new NotSupportedException();
        }

        public static Float64Series RollingMean(ISeries source, int windowSize)
        {
            if (source is Float64Series f64)
                return RollingMeanF64(f64, windowSize);
            if (source is Int32Series i32)
                return RollingMeanI32(i32, windowSize);
            throw new NotSupportedException();
        }

        private static Float64Series RollingMeanF64(Float64Series source, int windowSize)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            var resSpan = result.Memory.Span;
            var srcSpan = source.Memory.Span;
            var mask = result.ValidityMask;

            int partialEnd = Math.Min(windowSize - 1, length);
            for (int i = 0; i < partialEnd; i++)
                mask.SetNull(i);

            if (length < windowSize) return result;

            double runningSum = 0;
            for (int i = 0; i < windowSize; i++)
                runningSum += srcSpan[i];
            resSpan[windowSize - 1] = runningSum / windowSize;

            for (int i = windowSize; i < length; i++)
            {
                runningSum += srcSpan[i];
                runningSum -= srcSpan[i - windowSize];
                resSpan[i] = runningSum / windowSize;
            }
            return result;
        }

        private static Float64Series RollingMeanI32(Int32Series source, int windowSize)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            var resSpan = result.Memory.Span;
            var srcSpan = source.Memory.Span;
            var mask = result.ValidityMask;

            int partialEnd = Math.Min(windowSize - 1, length);
            for (int i = 0; i < partialEnd; i++)
                mask.SetNull(i);

            if (length < windowSize) return result;

            double runningSum = 0;
            for (int i = 0; i < windowSize; i++)
                runningSum += srcSpan[i];
            resSpan[windowSize - 1] = runningSum / windowSize;

            for (int i = windowSize; i < length; i++)
            {
                runningSum += srcSpan[i];
                runningSum -= srcSpan[i - windowSize];
                resSpan[i] = runningSum / windowSize;
            }
            return result;
        }

        public static ISeries RollingSum(ISeries source, int windowSize)
        {
            if (source is Int32Series i32)
            {
                int length = source.Length;
                var result = new Int32Series(source.Name, length);
                var resSpan = result.Memory.Span;
                var mask = result.ValidityMask;
                var srcSpan = i32.Memory.Span;

                for (int i = 0; i < Math.Min(windowSize - 1, length); i++)
                    mask.SetNull(i);

                if (length < windowSize) return result;

                int runningSum = 0;
                for (int i = 0; i < windowSize; i++)
                    runningSum += srcSpan[i];
                resSpan[windowSize - 1] = runningSum;

                for (int i = windowSize; i < length; i++)
                {
                    runningSum += srcSpan[i];
                    runningSum -= srcSpan[i - windowSize];
                    resSpan[i] = runningSum;
                }
                return result;
            }
            if (source is Float64Series f64)
            {
                int length = source.Length;
                var result = new Float64Series(source.Name, length);
                var resSpan = result.Memory.Span;
                var mask = result.ValidityMask;
                var srcSpan = f64.Memory.Span;

                for (int i = 0; i < Math.Min(windowSize - 1, length); i++)
                    mask.SetNull(i);

                if (length < windowSize) return result;

                double runningSum = 0;
                for (int i = 0; i < windowSize; i++)
                    runningSum += srcSpan[i];
                resSpan[windowSize - 1] = runningSum;

                for (int i = windowSize; i < length; i++)
                {
                    runningSum += srcSpan[i];
                    runningSum -= srcSpan[i - windowSize];
                    resSpan[i] = runningSum;
                }
                return result;
            }
            throw new NotSupportedException();
        }


        public static Float64Series RollingStd(ISeries source, int windowSize)
        {
            if (source is Float64Series f64)
                return RollingStdF64(f64, windowSize);
            if (source is Int32Series i32)
                return RollingStdI32(i32, windowSize);
            throw new NotSupportedException();
        }

        private static Float64Series RollingStdF64(Float64Series source, int windowSize)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            var resSpan = result.Memory.Span;
            var srcSpan = source.Memory.Span;
            var mask = result.ValidityMask;
            int partial = Math.Min(windowSize - 1, length);
            for (int i = 0; i < partial; i++) mask.SetNull(i);
            if (length < windowSize) return result;

            // O(n) sliding window: maintain sum and sumSq
            double sum = 0, sumSq = 0;
            for (int i = 0; i < windowSize; i++)
            {
                double v = srcSpan[i];
                sum += v;
                sumSq += v * v;
            }
            double variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
            resSpan[windowSize - 1] = Math.Sqrt(Math.Max(0, variance));

            for (int i = windowSize; i < length; i++)
            {
                double add = srcSpan[i];
                double remove = srcSpan[i - windowSize];
                sum += add - remove;
                sumSq += add * add - remove * remove;
                variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
                resSpan[i] = Math.Sqrt(Math.Max(0, variance));
            }
            return result;
        }

        private static Float64Series RollingStdI32(Int32Series source, int windowSize)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            var resSpan = result.Memory.Span;
            var srcSpan = source.Memory.Span;
            var mask = result.ValidityMask;
            int partial = Math.Min(windowSize - 1, length);
            for (int i = 0; i < partial; i++) mask.SetNull(i);
            if (length < windowSize) return result;

            double sum = 0, sumSq = 0;
            for (int i = 0; i < windowSize; i++)
            {
                double v = srcSpan[i];
                sum += v;
                sumSq += v * v;
            }
            double variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
            resSpan[windowSize - 1] = Math.Sqrt(Math.Max(0, variance));

            for (int i = windowSize; i < length; i++)
            {
                double add = srcSpan[i];
                double remove = srcSpan[i - windowSize];
                sum += add - remove;
                sumSq += add * add - remove * remove;
                variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
                resSpan[i] = Math.Sqrt(Math.Max(0, variance));
            }
            return result;
        }



        public static ISeries RollingMin(ISeries source, int windowSize)
        {
            int length = source.Length;
            var result = source.CloneEmpty(length);
            var mask = result.ValidityMask;
            for (int i = 0; i < Math.Min(windowSize - 1, length); i++) mask.SetNull(i);
            if (length < windowSize) return result;

            for (int i = windowSize - 1; i < length; i++)
            {
                int minIdx = -1;
                object? min = null;
                for (int j = i - windowSize + 1; j <= i; j++)
                {
                    var val = source.Get(j);
                    if (val != null && (min == null || ((IComparable)val).CompareTo(min) < 0)) { min = val; minIdx = j; }
                }
                if (minIdx != -1) source.Take(result, minIdx, i);
                else mask.SetNull(i);
            }
            return result;
        }

        public static ISeries RollingMax(ISeries source, int windowSize)
        {
            int length = source.Length;
            var result = source.CloneEmpty(length);
            var mask = result.ValidityMask;
            for (int i = 0; i < Math.Min(windowSize - 1, length); i++) mask.SetNull(i);
            if (length < windowSize) return result;

            for (int i = windowSize - 1; i < length; i++)
            {
                int maxIdx = -1;
                object? max = null;
                for (int j = i - windowSize + 1; j <= i; j++)
                {
                    var val = source.Get(j);
                    if (val != null && (max == null || ((IComparable)val).CompareTo(max) > 0)) { max = val; maxIdx = j; }
                }
                if (maxIdx != -1) source.Take(result, maxIdx, i);
                else mask.SetNull(i);
            }
            return result;
        }

        public static ISeries ExpandingSum(ISeries source)
        {
            if (source is Int32Series i32)
            {
                var result = new Int32Series(source.Name, source.Length);
                int sum = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    sum += i32.Memory.Span[i];
                    result.Memory.Span[i] = sum;
                }
                return result;
            }
            if (source is Float64Series f64)
            {
                var result = new Float64Series(source.Name, source.Length);
                double sum = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    sum += f64.Memory.Span[i];
                    result.Memory.Span[i] = sum;
                }
                return result;
            }
            throw new NotSupportedException();
        }

        public static Float64Series ExpandingMean(ISeries source)
        {
            var result = new Float64Series(source.Name, source.Length);
            double sum = 0;
            for (int i = 0; i < source.Length; i++)
            {
                sum += GetValueAsDouble(source, i);
                result.Memory.Span[i] = sum / (i + 1);
            }
            return result;
        }

        public static ISeries ExpandingMin(ISeries source)
        {
            var result = source.CloneEmpty(source.Length);
            int minIdx = -1;
            object? min = null;
            for (int i = 0; i < source.Length; i++)
            {
                var val = source.Get(i);
                if (val != null && (min == null || ((IComparable)val).CompareTo(min) < 0)) { min = val; minIdx = i; }
                if (minIdx != -1) source.Take(result, minIdx, i);
                else result.ValidityMask.SetNull(i);
            }
            return result;
        }

        public static ISeries ExpandingMax(ISeries source)
        {
            var result = source.CloneEmpty(source.Length);
            int maxIdx = -1;
            object? max = null;
            for (int i = 0; i < source.Length; i++)
            {
                var val = source.Get(i);
                if (val != null && (max == null || ((IComparable)val).CompareTo(max) > 0)) { max = val; maxIdx = i; }
                if (maxIdx != -1) source.Take(result, maxIdx, i);
                else result.ValidityMask.SetNull(i);
            }
            return result;
        }

        public static Float64Series ExpandingStd(ISeries source)
        {
            if (source is Float64Series f64)
                return ExpandingStdF64(f64);
            if (source is Int32Series i32)
                return ExpandingStdI32(i32);

            var result = new Float64Series(source.Name, source.Length);
            double sum = 0;
            double sumSq = 0;
            for (int i = 0; i < source.Length; i++)
            {
                double val = GetValueAsDouble(source, i);
                sum += val;
                sumSq += val * val;
                if (i == 0) result.ValidityMask.SetNull(i);
                else
                {
                    double n = i + 1;
                    double variance = (sumSq - (sum * sum) / n) / (n - 1);
                    result.Memory.Span[i] = Math.Sqrt(Math.Max(0, variance));
                }
            }
            return result;
        }

        private static Float64Series ExpandingStdF64(Float64Series source)
        {
            var result = new Float64Series(source.Name, source.Length);
            var span = source.Memory.Span;
            var res = result.Memory.Span;
            double sum = 0;
            double sumSq = 0;
            for (int i = 0; i < source.Length; i++)
            {
                double val = span[i];
                sum += val;
                sumSq += val * val;
                if (i == 0) result.ValidityMask.SetNull(i);
                else
                {
                    double n = i + 1;
                    double variance = (sumSq - (sum * sum) / n) / (n - 1);
                    res[i] = Math.Sqrt(Math.Max(0, variance));
                }
            }
            return result;
        }

        private static Float64Series ExpandingStdI32(Int32Series source)
        {
            var result = new Float64Series(source.Name, source.Length);
            var span = source.Memory.Span;
            var res = result.Memory.Span;
            long sum = 0;
            long sumSq = 0;
            for (int i = 0; i < source.Length; i++)
            {
                int val = span[i];
                sum += val;
                sumSq += (long)val * val;
                if (i == 0) result.ValidityMask.SetNull(i);
                else
                {
                    double n = i + 1;
                    double variance = (sumSq - (double)(sum * sum) / n) / (n - 1);
                    res[i] = Math.Sqrt(Math.Max(0, variance));
                }
            }
            return result;
        }

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
        /// <summary>Cumulative count of non-null elements (O(n)).</summary>
        public static ISeries ExpandingCount(ISeries source, bool reverse = false)
        {
            var result = new Int32Series(source.Name + "_cum_count", source.Length);
            if (reverse)
            {
                int count = 0;
                for (int i = source.Length - 1; i >= 0; i--)
                {
                    if (source.ValidityMask.IsValid(i))
                        count++;
                    result.Memory.Span[i] = count;
                }
            }
            else
            {
                int count = 0;
                for (int i = 0; i < source.Length; i++)
                {
                    if (source.ValidityMask.IsValid(i))
                        count++;
                    result.Memory.Span[i] = count;
                }
            }
            return result;
        }
        public static ISeries ExpandingProd(ISeries source, bool reverse = false)
        {
            if (reverse)
            {
                if (source is Int32Series i32)
                {
                    var result = new Int64Series(source.Name + "_cum_prod", source.Length);
                    long prod = 1;
                    bool hasPrev = false;
                    for (int i = source.Length - 1; i >= 0; i--)
                    {
                        if (i32.ValidityMask.IsValid(i))
                        {
                            if (hasPrev) { prod *= i32.Memory.Span[i]; }
                            else { prod = i32.Memory.Span[i]; hasPrev = true; }
                            result.Memory.Span[i] = prod;
                        }
                        else
                        {
                            result.ValidityMask.SetNull(i);
                            // Do NOT reset prod — carry through nulls like Polars
                        }
                    }
                    return result;
                }
                if (source is Int64Series i64)
                {
                    var result = new Int64Series(source.Name + "_cum_prod", source.Length);
                    long prod = 1;
                    bool hasPrev = false;
                    for (int i = source.Length - 1; i >= 0; i--)
                    {
                        if (i64.ValidityMask.IsValid(i))
                        {
                            if (hasPrev) { prod *= i64.Memory.Span[i]; }
                            else { prod = i64.Memory.Span[i]; hasPrev = true; }
                            result.Memory.Span[i] = prod;
                        }
                        else
                        {
                            result.ValidityMask.SetNull(i);
                        }
                    }
                    return result;
                }
                if (source is Float64Series f64)
                {
                    var result = new Float64Series(source.Name + "_cum_prod", source.Length);
                    double prod = 1.0;
                    bool hasPrev = false;
                    for (int i = source.Length - 1; i >= 0; i--)
                    {
                        if (f64.ValidityMask.IsValid(i))
                        {
                            if (hasPrev) { prod *= f64.Memory.Span[i]; }
                            else { prod = f64.Memory.Span[i]; hasPrev = true; }
                            result.Memory.Span[i] = prod;
                        }
                        else
                        {
                            result.ValidityMask.SetNull(i);
                        }
                    }
                    return result;
                }
                throw new NotSupportedException($"ExpandingProd reverse not supported for {source.GetType().Name}");
            }
            // Forward
            if (source is Int32Series i32f)
            {
                var result = new Int64Series(source.Name + "_cum_prod", source.Length);
                long prod = 1;
                bool hasPrev = false;
                for (int i = 0; i < source.Length; i++)
                {
                    if (i32f.ValidityMask.IsValid(i))
                    {
                        if (hasPrev) { prod *= i32f.Memory.Span[i]; }
                        else { prod = i32f.Memory.Span[i]; hasPrev = true; }
                        result.Memory.Span[i] = prod;
                    }
                    else
                    {
                        result.ValidityMask.SetNull(i);
                        // Do NOT reset prod — carry through nulls like Polars
                    }
                }
                return result;
            }
            if (source is Int64Series i64f)
            {
                var result = new Int64Series(source.Name + "_cum_prod", source.Length);
                long prod = 1;
                bool hasPrev = false;
                for (int i = 0; i < source.Length; i++)
                {
                    if (i64f.ValidityMask.IsValid(i))
                    {
                        if (hasPrev) { prod *= i64f.Memory.Span[i]; }
                        else { prod = i64f.Memory.Span[i]; hasPrev = true; }
                        result.Memory.Span[i] = prod;
                    }
                    else
                    {
                        result.ValidityMask.SetNull(i);
                    }
                }
                return result;
            }
            if (source is Float64Series f64f)
            {
                var result = new Float64Series(source.Name + "_cum_prod", source.Length);
                double prod = 1.0;
                bool hasPrev = false;
                for (int i = 0; i < source.Length; i++)
                {
                    if (f64f.ValidityMask.IsValid(i))
                    {
                        if (hasPrev) { prod *= f64f.Memory.Span[i]; }
                        else { prod = f64f.Memory.Span[i]; hasPrev = true; }
                        result.Memory.Span[i] = prod;
                    }
                    else
                    {
                        result.ValidityMask.SetNull(i);
                    }
                }
                return result;
            }
            throw new NotSupportedException($"ExpandingProd not supported for {source.GetType().Name}");
        }
    }
}
