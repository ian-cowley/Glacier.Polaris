using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class WindowKernels
    {
        public static unsafe ISeries ExpandingSum(ISeries source)
        {
            if (source is Int32Series i32)
            {
                int length = i32.Length;
                var result = new Int32Series(source.Name, length);
                var resSpan = result.Memory.Span;
                var srcSpan = i32.Memory.Span;

                if (length >= 100_000)
                {
                    int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / 32768));
                    int chunkSize = (length + numChunks - 1) / numChunks;
                    int[] chunkSums = new int[numChunks];

                    fixed (int* pSrc = srcSpan)
                    fixed (int* pRes = resSpan)
                    {
                        int* src = pSrc;
                        int* res = pRes;

                        Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(length, start + chunkSize);
                            int sum = 0;
                            for (int i = start; i < end; i++)
                            {
                                sum += src[i];
                                res[i] = sum;
                            }
                            chunkSums[c] = sum;
                        });

                        int running = 0;
                        int[] chunkOffsets = new int[numChunks];
                        for (int c = 0; c < numChunks; c++)
                        {
                            chunkOffsets[c] = running;
                            running += chunkSums[c];
                        }

                        Parallel.For(1, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(length, start + chunkSize);
                            int offset = chunkOffsets[c];
                            int i = start;
                            int count = end - start;

                            if (Vector256.IsHardwareAccelerated && count >= 8)
                            {
                                var vOff = Vector256.Create(offset);
                                int limit = end - 8;
                                for (; i <= limit; i += 8)
                                {
                                    var v = Vector256.Load(res + i);
                                    v = Vector256.Add(v, vOff);
                                    v.Store(res + i);
                                }
                            }
                            for (; i < end; i++)
                            {
                                res[i] += offset;
                            }
                        });
                    }
                    return result;
                }

                int s = 0;
                for (int i = 0; i < length; i++)
                {
                    s += srcSpan[i];
                    resSpan[i] = s;
                }
                return result;
            }

            if (source is Float64Series f64)
            {
                int length = f64.Length;
                var result = new Float64Series(source.Name, length);
                var resSpan = result.Memory.Span;
                var srcSpan = f64.Memory.Span;

                if (length >= 100_000)
                {
                    int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / 32768));
                    int chunkSize = (length + numChunks - 1) / numChunks;
                    double[] chunkSums = new double[numChunks];

                    fixed (double* pSrc = srcSpan)
                    fixed (double* pRes = resSpan)
                    {
                        double* src = pSrc;
                        double* res = pRes;

                        Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(length, start + chunkSize);
                            double sum = 0;
                            for (int i = start; i < end; i++)
                            {
                                sum += src[i];
                                res[i] = sum;
                            }
                            chunkSums[c] = sum;
                        });

                        double running = 0;
                        double[] chunkOffsets = new double[numChunks];
                        for (int c = 0; c < numChunks; c++)
                        {
                            chunkOffsets[c] = running;
                            running += chunkSums[c];
                        }

                        Parallel.For(1, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(length, start + chunkSize);
                            double offset = chunkOffsets[c];
                            int i = start;
                            int count = end - start;

                            if (Vector512.IsHardwareAccelerated && count >= 8)
                            {
                                var vOff = Vector512.Create(offset);
                                int limit = end - 8;
                                for (; i <= limit; i += 8)
                                {
                                    var v = Vector512.Load(res + i);
                                    v = Vector512.Add(v, vOff);
                                    v.Store(res + i);
                                }
                            }
                            else if (Vector256.IsHardwareAccelerated && count >= 4)
                            {
                                var vOff = Vector256.Create(offset);
                                int limit = end - 4;
                                for (; i <= limit; i += 4)
                                {
                                    var v = Vector256.Load(res + i);
                                    v = Vector256.Add(v, vOff);
                                    v.Store(res + i);
                                }
                            }
                            for (; i < end; i++)
                            {
                                res[i] += offset;
                            }
                        });
                    }
                    return result;
                }

                double sumVal = 0;
                for (int i = 0; i < length; i++)
                {
                    sumVal += srcSpan[i];
                    resSpan[i] = sumVal;
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

        private static unsafe Float64Series ExpandingStdF64(Float64Series source)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            var span = source.Memory.Span;
            var res = result.Memory.Span;
            if (length == 0) return result;
            result.ValidityMask.SetNull(0);

            if (length >= 100_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / 32768));
                int chunkSize = (length + numChunks - 1) / numChunks;
                double[] chunkSums = new double[numChunks];
                double[] chunkSqSums = new double[numChunks];

                fixed (double* pSrc = span)
                fixed (double* pRes = res)
                {
                    double* src = pSrc;
                    double* resPtr = pRes;

                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int end = Math.Min(length, start + chunkSize);
                        double sum = 0, sqSum = 0;
                        for (int i = start; i < end; i++)
                        {
                            double v = src[i];
                            sum += v;
                            sqSum += v * v;
                        }
                        chunkSums[c] = sum;
                        chunkSqSums[c] = sqSum;
                    });

                    double runningSum = 0;
                    double runningSq = 0;
                    double[] chunkSumOffsets = new double[numChunks];
                    double[] chunkSqOffsets = new double[numChunks];
                    for (int c = 0; c < numChunks; c++)
                    {
                        chunkSumOffsets[c] = runningSum;
                        chunkSqOffsets[c] = runningSq;
                        runningSum += chunkSums[c];
                        runningSq += chunkSqSums[c];
                    }

                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int end = Math.Min(length, start + chunkSize);
                        double sum = chunkSumOffsets[c];
                        double sqSum = chunkSqOffsets[c];

                        int i = start;
                        if (c == 0 && i == 0)
                        {
                            double v = src[0];
                            sum += v;
                            sqSum += v * v;
                            i = 1;
                        }

                        for (; i < end; i++)
                        {
                            double v = src[i];
                            sum += v;
                            sqSum += v * v;
                            double n = i + 1;
                            double variance = (sqSum - (sum * sum) / n) / (n - 1);
                            resPtr[i] = Math.Sqrt(Math.Max(0, variance));
                        }
                    });
                }
                return result;
            }

            double s = span[0];
            double sq = s * s;
            for (int i = 1; i < length; i++)
            {
                double val = span[i];
                s += val;
                sq += val * val;
                double n = i + 1;
                double variance = (sq - (s * s) / n) / (n - 1);
                res[i] = Math.Sqrt(Math.Max(0, variance));
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
