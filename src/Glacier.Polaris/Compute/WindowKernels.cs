using System;
using System.Collections.Generic;
using Glacier.Polaris.Data;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    public static partial class WindowKernels
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

        private static unsafe Float64Series RollingMeanF64(Float64Series source, int windowSize)
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

            if (length >= 100_000 && windowSize < 1000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / (windowSize * 2)));
                int chunkSize = (length + numChunks - 1) / numChunks;

                fixed (double* pSrc = srcSpan)
                fixed (double* pRes = resSpan)
                {
                    double* src = pSrc;
                    double* res = pRes;

                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int end = Math.Min(length, start + chunkSize);
                        if (start >= end) return;

                        if (c == 0)
                        {
                            double runningSum = 0;
                            for (int i = 0; i < windowSize; i++)
                                runningSum += src[i];
                            res[windowSize - 1] = runningSum / windowSize;

                            for (int i = windowSize; i < end; i++)
                            {
                                runningSum += src[i] - src[i - windowSize];
                                res[i] = runningSum / windowSize;
                            }
                        }
                        else
                        {
                            double runningSum = 0;
                            for (int k = start - windowSize + 1; k <= start; k++)
                                runningSum += src[k];
                            res[start] = runningSum / windowSize;

                            for (int i = start + 1; i < end; i++)
                            {
                                runningSum += src[i] - src[i - windowSize];
                                res[i] = runningSum / windowSize;
                            }
                        }
                    });
                }
                return result;
            }

            double rSum = 0;
            for (int i = 0; i < windowSize; i++)
                rSum += srcSpan[i];
            resSpan[windowSize - 1] = rSum / windowSize;

            for (int i = windowSize; i < length; i++)
            {
                rSum += srcSpan[i] - srcSpan[i - windowSize];
                resSpan[i] = rSum / windowSize;
            }
            return result;
        }

        private static unsafe Float64Series RollingMeanI32(Int32Series source, int windowSize)
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

            if (length >= 100_000 && windowSize < 1000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / (windowSize * 2)));
                int chunkSize = (length + numChunks - 1) / numChunks;

                fixed (int* pSrc = srcSpan)
                fixed (double* pRes = resSpan)
                {
                    int* src = pSrc;
                    double* res = pRes;

                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int end = Math.Min(length, start + chunkSize);
                        if (start >= end) return;

                        if (c == 0)
                        {
                            double runningSum = 0;
                            for (int i = 0; i < windowSize; i++)
                                runningSum += src[i];
                            res[windowSize - 1] = runningSum / windowSize;

                            for (int i = windowSize; i < end; i++)
                            {
                                runningSum += src[i] - src[i - windowSize];
                                res[i] = runningSum / windowSize;
                            }
                        }
                        else
                        {
                            double runningSum = 0;
                            for (int k = start - windowSize + 1; k <= start; k++)
                                runningSum += src[k];
                            res[start] = runningSum / windowSize;

                            for (int i = start + 1; i < end; i++)
                            {
                                runningSum += src[i] - src[i - windowSize];
                                res[i] = runningSum / windowSize;
                            }
                        }
                    });
                }
                return result;
            }

            double rSum = 0;
            for (int i = 0; i < windowSize; i++)
                rSum += srcSpan[i];
            resSpan[windowSize - 1] = rSum / windowSize;

            for (int i = windowSize; i < length; i++)
            {
                rSum += srcSpan[i] - srcSpan[i - windowSize];
                resSpan[i] = rSum / windowSize;
            }
            return result;
        }

        public static unsafe ISeries RollingSum(ISeries source, int windowSize)
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

                if (length >= 100_000 && windowSize < 1000)
                {
                    int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / (windowSize * 2)));
                    int chunkSize = (length + numChunks - 1) / numChunks;

                    fixed (int* pSrc = srcSpan)
                    fixed (int* pRes = resSpan)
                    {
                        int* src = pSrc;
                        int* res = pRes;

                        Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(length, start + chunkSize);
                            if (start >= end) return;

                            if (c == 0)
                            {
                                int runningSum = 0;
                                for (int i = 0; i < windowSize; i++)
                                    runningSum += src[i];
                                res[windowSize - 1] = runningSum;

                                for (int i = windowSize; i < end; i++)
                                {
                                    runningSum += src[i] - src[i - windowSize];
                                    res[i] = runningSum;
                                }
                            }
                            else
                            {
                                int runningSum = 0;
                                for (int k = start - windowSize + 1; k <= start; k++)
                                    runningSum += src[k];
                                res[start] = runningSum;

                                for (int i = start + 1; i < end; i++)
                                {
                                    runningSum += src[i] - src[i - windowSize];
                                    res[i] = runningSum;
                                }
                            }
                        });
                    }
                    return result;
                }

                int rSum = 0;
                for (int i = 0; i < windowSize; i++)
                    rSum += srcSpan[i];
                resSpan[windowSize - 1] = rSum;

                for (int i = windowSize; i < length; i++)
                {
                    rSum += srcSpan[i] - srcSpan[i - windowSize];
                    resSpan[i] = rSum;
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

                if (length >= 100_000 && windowSize < 1000)
                {
                    int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / (windowSize * 2)));
                    int chunkSize = (length + numChunks - 1) / numChunks;

                    fixed (double* pSrc = srcSpan)
                    fixed (double* pRes = resSpan)
                    {
                        double* src = pSrc;
                        double* res = pRes;

                        Parallel.For(0, numChunks, c =>
                        {
                            int start = c * chunkSize;
                            int end = Math.Min(length, start + chunkSize);
                            if (start >= end) return;

                            if (c == 0)
                            {
                                double runningSum = 0;
                                for (int i = 0; i < windowSize; i++)
                                    runningSum += src[i];
                                res[windowSize - 1] = runningSum;

                                for (int i = windowSize; i < end; i++)
                                {
                                    runningSum += src[i] - src[i - windowSize];
                                    res[i] = runningSum;
                                }
                            }
                            else
                            {
                                double runningSum = 0;
                                for (int k = start - windowSize + 1; k <= start; k++)
                                    runningSum += src[k];
                                res[start] = runningSum;

                                for (int i = start + 1; i < end; i++)
                                {
                                    runningSum += src[i] - src[i - windowSize];
                                    res[i] = runningSum;
                                }
                            }
                        });
                    }
                    return result;
                }

                double rSum = 0;
                for (int i = 0; i < windowSize; i++)
                    rSum += srcSpan[i];
                resSpan[windowSize - 1] = rSum;

                for (int i = windowSize; i < length; i++)
                {
                    rSum += srcSpan[i] - srcSpan[i - windowSize];
                    resSpan[i] = rSum;
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

        private static unsafe Float64Series RollingStdF64(Float64Series source, int windowSize)
        {
            int length = source.Length;
            var result = new Float64Series(source.Name, length);
            var resSpan = result.Memory.Span;
            var srcSpan = source.Memory.Span;
            var mask = result.ValidityMask;
            int partial = Math.Min(windowSize - 1, length);
            for (int i = 0; i < partial; i++) mask.SetNull(i);
            if (length < windowSize) return result;

            if (length >= 100_000 && windowSize < 1000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, length / (windowSize * 2)));
                int chunkSize = (length + numChunks - 1) / numChunks;

                fixed (double* pSrc = srcSpan)
                fixed (double* pRes = resSpan)
                {
                    double* src = pSrc;
                    double* res = pRes;

                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int end = Math.Min(length, start + chunkSize);
                        if (start >= end) return;

                        if (c == 0)
                        {
                            double sum = 0, sumSq = 0;
                            for (int i = 0; i < windowSize; i++)
                            {
                                double v = src[i];
                                sum += v;
                                sumSq += v * v;
                            }
                            double variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
                            res[windowSize - 1] = Math.Sqrt(Math.Max(0, variance));

                            for (int i = windowSize; i < end; i++)
                            {
                                double add = src[i];
                                double remove = src[i - windowSize];
                                sum += add - remove;
                                sumSq += add * add - remove * remove;
                                variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
                                res[i] = Math.Sqrt(Math.Max(0, variance));
                            }
                        }
                        else
                        {
                            double sum = 0, sumSq = 0;
                            for (int k = start - windowSize + 1; k <= start; k++)
                            {
                                double v = src[k];
                                sum += v;
                                sumSq += v * v;
                            }
                            double variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
                            res[start] = Math.Sqrt(Math.Max(0, variance));

                            for (int i = start + 1; i < end; i++)
                            {
                                double add = src[i];
                                double remove = src[i - windowSize];
                                sum += add - remove;
                                sumSq += add * add - remove * remove;
                                variance = (sumSq - (sum * sum) / windowSize) / (windowSize - 1);
                                res[i] = Math.Sqrt(Math.Max(0, variance));
                            }
                        }
                    });
                }
                return result;
            }

            // O(n) sliding window: maintain sum and sumSq
            double sSum = 0, sSumSq = 0;
            for (int i = 0; i < windowSize; i++)
            {
                double v = srcSpan[i];
                sSum += v;
                sSumSq += v * v;
            }
            double sVariance = (sSumSq - (sSum * sSum) / windowSize) / (windowSize - 1);
            resSpan[windowSize - 1] = Math.Sqrt(Math.Max(0, sVariance));

            for (int i = windowSize; i < length; i++)
            {
                double add = srcSpan[i];
                double remove = srcSpan[i - windowSize];
                sSum += add - remove;
                sSumSq += add * add - remove * remove;
                sVariance = (sSumSq - (sSum * sSum) / windowSize) / (windowSize - 1);
                resSpan[i] = Math.Sqrt(Math.Max(0, sVariance));
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
    }
}
