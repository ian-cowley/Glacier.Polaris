using System;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class AggregationKernels
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
        /// <summary>
        /// SIMD-accelerated Sum. Uses Vector256 to process 8 Int32s or 4 Float64s per instruction.
        /// Falls back to scalar for non-numeric types.
        /// </summary>
        public static ISeries Sum(ISeries series)
        {
            if (series is Int32Series i32)
            {
                var span = i32.Memory.Span;
                long sum = 0;
                int i = 0;

                if (Vector256.IsHardwareAccelerated && span.Length >= Vector256<int>.Count)
                {
                    int step = Vector256<int>.Count; // 8
                    var vSum = Vector256<long>.Zero;
                    for (; i <= span.Length - step; i += step)
                    {
                        var vData = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                        var (widenLo, widenHi) = Vector256.Widen(vData);
                        vSum = Vector256.Add(vSum, widenLo);
                        vSum = Vector256.Add(vSum, widenHi);
                    }
                    // Horizontal add: extract and sum all 4 longs
                    long simdSum = 0;
                    for (int j = 0; j < Vector256<long>.Count; j++)
                        simdSum += vSum[j];
                    sum = simdSum;
                }

                // Scalar tail with null check
                for (; i < span.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i)) sum += span[i];
                }

                var result = new Int32Series(series.Name + "_sum", 1);
                result.Memory.Span[0] = (int)sum;
                return result;
            }
            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                double sum = 0;
                int i = 0;

                if (Vector256.IsHardwareAccelerated && span.Length >= Vector256<double>.Count)
                {
                    int step = Vector256<double>.Count; // 4
                    var vSum = Vector256<double>.Zero;
                    for (; i <= span.Length - step; i += step)
                    {
                        var vData = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                        vSum = Vector256.Add(vSum, vData);
                    }
                    double simdSum = 0;
                    for (int j = 0; j < Vector256<double>.Count; j++)
                        simdSum += vSum[j];
                    sum = simdSum;
                }

                for (; i < span.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i)) sum += span[i];
                }

                var result = new Float64Series(series.Name + "_sum", 1);
                result.Memory.Span[0] = sum;
                return result;
            }
            return new NullSeries(series.Name + "_sum", 1);
        }
        /// <summary>
        /// SIMD-accelerated Mean. For Float64, uses Vector256 to sum 4 values per instruction,
        /// with a scalar count. For Int32, widens to long and sums via SIMD.
        /// </summary>
        public static ISeries Mean(ISeries series)
        {
            if (series is Int32Series i32)
            {
                long sum = 0;
                int count = 0;
                var span = i32.Memory.Span;
                int i = 0;

                if (Vector256.IsHardwareAccelerated && span.Length >= Vector256<int>.Count)
                {
                    int step = Vector256<int>.Count; // 8
                    var vSum = Vector256<long>.Zero;
                    for (; i <= span.Length - step; i += step)
                    {
                        var vData = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                        var (widenLo, widenHi) = Vector256.Widen(vData);
                        vSum = Vector256.Add(vSum, widenLo);
                        vSum = Vector256.Add(vSum, widenHi);
                    }
                    long simdSum = 0;
                    for (int j = 0; j < Vector256<long>.Count; j++)
                        simdSum += vSum[j];
                    sum = simdSum;
                    count = (i / step) * step;
                }

                for (; i < span.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i)) { sum += span[i]; count++; }
                }

                var result = new Float64Series(series.Name + "_mean", 1);
                if (count > 0) result.Memory.Span[0] = (double)sum / count;
                else result.ValidityMask.SetNull(0);
                return result;
            }
            if (series is Float64Series f64)
            {
                double sum = 0;
                int count = 0;
                var span = f64.Memory.Span;
                int i = 0;

                if (Vector256.IsHardwareAccelerated && span.Length >= Vector256<double>.Count)
                {
                    int step = Vector256<double>.Count; // 4
                    var vSum = Vector256<double>.Zero;
                    for (; i <= span.Length - step; i += step)
                    {
                        var vData = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                        vSum = Vector256.Add(vSum, vData);
                    }
                    double simdSum = 0;
                    for (int j = 0; j < Vector256<double>.Count; j++)
                        simdSum += vSum[j];
                    sum = simdSum;
                    count = i; // No nulls in SIMD path (typical case)
                }

                for (; i < span.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i)) { sum += span[i]; count++; }
                }

                var result = new Float64Series(series.Name + "_mean", 1);
                if (count > 0) result.Memory.Span[0] = sum / count;
                else result.ValidityMask.SetNull(0);
                return result;
            }
            return new NullSeries(series.Name + "_mean", 1);
        }
        public static ISeries Std(ISeries series)
        {
            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                int n = span.Length;

                if (!mask.HasNulls)
                {
                    // No-null fast path: SIMD sum then SIMD variance
                    int i = 0;
                    double sum = 0;

                    if (Vector256.IsHardwareAccelerated && n >= Vector256<double>.Count)
                    {
                        int simdStep = Vector256<double>.Count;
                        var vSum = Vector256<double>.Zero;
                        for (; i <= n - simdStep; i += simdStep)
                            vSum += Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                        sum = vSum[0] + vSum[1] + vSum[2] + vSum[3];
                    }
                    for (; i < n; i++)
                        sum += span[i];

                    double mean = sum / n;

                    i = 0;
                    double sqSum = 0;

                    if (Vector256.IsHardwareAccelerated && n >= Vector256<double>.Count)
                    {
                        int simdStep = Vector256<double>.Count;
                        var vMean = Vector256.Create(mean);
                        var vSq = Vector256<double>.Zero;
                        for (; i <= n - simdStep; i += simdStep)
                        {
                            var v = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                            var diff = v - vMean;
                            vSq += diff * diff;
                        }
                        sqSum = vSq[0] + vSq[1] + vSq[2] + vSq[3];
                    }
                    for (; i < n; i++)
                    {
                        double d = span[i] - mean;
                        sqSum += d * d;
                    }

                    var result = new Float64Series(series.Name + "_std", 1);
                    if (n < 2) result[0] = double.NaN;
                    else result[0] = Math.Sqrt(sqSum / (n - 1));
                    return result;
                }

                // Has-nulls: single-pass Welford
                double wMean = 0;
                double m2 = 0;
                int count = 0;

                for (int i = 0; i < n; i++)
                {
                    if (mask.IsValid(i))
                    {
                        count++;
                        double val = span[i];
                        double delta = val - wMean;
                        wMean += delta / count;
                        m2 += delta * (val - wMean);
                    }
                }

                var result2 = new Float64Series(series.Name + "_std", 1);
                if (count < 2) result2[0] = double.NaN;
                else result2[0] = Math.Sqrt(m2 / (count - 1));
                return result2;
            }

            if (series is Int32Series i32)
            {
                double mean = 0;
                double m2 = 0;
                int count = 0;
                var span = i32.Memory.Span;
                var mask = i32.ValidityMask;
                for (int i = 0; i < span.Length; i++)
                {
                    if (mask.IsValid(i))
                    {
                        count++;
                        double val = span[i];
                        double delta = val - mean;
                        mean += delta / count;
                        m2 += delta * (val - mean);
                    }
                }
                var result = new Float64Series(series.Name + "_std", 1);
                if (count < 2) result[0] = double.NaN;
                else result[0] = Math.Sqrt(m2 / (count - 1));
                return result;
            }
            return new NullSeries(series.Name + "_std", 1);
        }/// <summary>SIMD-accelerated single-pass Welford Var — Float64 uses Vector256 load.</summary>
        public static ISeries Var(ISeries series)
        {
            if (series is Int32Series i32)
            {
                double mean = 0;
                double m2 = 0;
                int count = 0;
                var span = i32.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        count++;
                        double val = span[i];
                        double delta = val - mean;
                        mean += delta / count;
                        m2 += delta * (val - mean);
                    }
                }
                var result = new Float64Series(series.Name + "_var", 1);
                if (count < 2) result[0] = double.NaN;
                else result[0] = m2 / (count - 1);
                return result;
            }
            if (series is Float64Series f64)
            {
                double mean = 0;
                double m2 = 0;
                int count = 0;
                var span = f64.Memory.Span;
                int i = 0;

                if (Vector256.IsHardwareAccelerated && span.Length >= Vector256<double>.Count)
                {
                    int simdStep = Vector256<double>.Count;
                    for (; i <= span.Length - simdStep; i += simdStep)
                    {
                        var v = Vector256.LoadUnsafe(ref System.Runtime.InteropServices.MemoryMarshal.GetReference(span.Slice(i)));
                        for (int j = 0; j < simdStep; j++)
                        {
                            if (series.ValidityMask.IsValid(i + j))
                            {
                                count++;
                                double val = v[j];
                                double delta = val - mean;
                                mean += delta / count;
                                m2 += delta * (val - mean);
                            }
                        }
                    }
                }
                for (; i < span.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        count++;
                        double val = span[i];
                        double delta = val - mean;
                        mean += delta / count;
                        m2 += delta * (val - mean);
                    }
                }

                var result = new Float64Series(series.Name + "_var", 1);
                if (count < 2) result[0] = double.NaN;
                else result[0] = m2 / (count - 1);
                return result;
            }
            return new NullSeries(series.Name + "_var", 1);
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

        public static ISeries Median(ISeries series)
        {
            return Quantile(series, 0.5);
        }

        public static ISeries Quantile(ISeries series, double quantile)
        {
            if (series is Int32Series i32)
            {
                var list = new System.Collections.Generic.List<int>();
                for (int i = 0; i < series.Length; i++) if (i32.ValidityMask.IsValid(i)) list.Add(i32[i]);
                if (list.Count == 0)
                {
                    var res = new Float64Series(series.Name + "_quantile", 1);
                    res.ValidityMask.SetNull(0);
                    return res;
                }
                list.Sort();
                double idx = (list.Count - 1) * quantile;
                int lower = (int)Math.Floor(idx);
                int upper = (int)Math.Ceiling(idx);
                double val = list[lower] + (list[upper] - list[lower]) * (idx - lower);
                var result = new Float64Series(series.Name + "_quantile", 1);
                result.Memory.Span[0] = val;
                return result;
            }
            if (series is Float64Series f64)
            {
                var list = new System.Collections.Generic.List<double>();
                for (int i = 0; i < series.Length; i++) if (f64.ValidityMask.IsValid(i)) list.Add(f64[i]);
                if (list.Count == 0)
                {
                    var res = new Float64Series(series.Name + "_quantile", 1);
                    res.ValidityMask.SetNull(0);
                    return res;
                }
                list.Sort();
                double idx = (list.Count - 1) * quantile;
                int lower = (int)Math.Floor(idx);
                int upper = (int)Math.Ceiling(idx);
                double val = list[lower] + (list[upper] - list[lower]) * (idx - lower);
                var result = new Float64Series(series.Name + "_quantile", 1);
                result.Memory.Span[0] = val;
                return result;
            }
            return new NullSeries(series.Name + "_quantile", 1);
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
