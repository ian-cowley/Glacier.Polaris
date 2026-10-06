using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    public static partial class AggregationKernels
    {
        private static readonly Vector256<int>[] s_int32MaskLut = PrecomputeInt32MaskLut();
        private static readonly Vector256<long>[] s_float64MaskLut = PrecomputeFloat64MaskLut();

        private static Vector256<int>[] PrecomputeInt32MaskLut()
        {
            var lut = new Vector256<int>[256];
            Span<int> lanes = stackalloc int[8];
            for (int b = 0; b < 256; b++)
            {
                for (int k = 0; k < 8; k++)
                    lanes[k] = ((b & (1 << k)) != 0) ? -1 : 0;
                lut[b] = Vector256.Create(lanes);
            }
            return lut;
        }

        private static Vector256<long>[] PrecomputeFloat64MaskLut()
        {
            var lut = new Vector256<long>[16];
            Span<long> lanes = stackalloc long[4];
            for (int b = 0; b < 16; b++)
            {
                for (int k = 0; k < 4; k++)
                    lanes[k] = ((b & (1 << k)) != 0) ? -1L : 0L;
                lut[b] = Vector256.Create(lanes);
            }
            return lut;
        }

        /// <summary>
        /// SIMD-accelerated Sum with 64-element block bitwise validity mask filtering.
        /// Uses Vector256 to process 8 Int32s or 4 Float64s per instruction, zeroing null lanes.
        /// Falls back to scalar for non-numeric types.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        internal static unsafe double SumFloat64(ReadOnlySpan<double> span)
        {
            int n = span.Length;
            if (n >= 250_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, n / 32768));
                int chunkSize = (n + numChunks - 1) / numChunks;
                double[] partialSums = new double[numChunks];
                fixed (double* p = span)
                {
                    double* ptr = p;
                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int length = Math.Min(chunkSize, n - start);
                        if (length > 0)
                            partialSums[c] = SumFloat64Ptr(ptr + start, length);
                    });
                }
                double total = 0;
                for (int c = 0; c < numChunks; c++) total += partialSums[c];
                return total;
            }
            fixed (double* p = span)
            {
                return SumFloat64Ptr(p, n);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private static unsafe double SumFloat64Ptr(double* ptr, int len)
        {
            if (len == 0) return 0;
            int i = 0;

            if (Vector512.IsHardwareAccelerated && len >= 32)
            {
                var s0 = Vector512<double>.Zero;
                var s1 = Vector512<double>.Zero;
                var s2 = Vector512<double>.Zero;
                var s3 = Vector512<double>.Zero;
                int limit = len - 32;
                for (; i <= limit; i += 32)
                {
                    s0 += Vector512.Load(ptr + i);
                    s1 += Vector512.Load(ptr + i + 8);
                    s2 += Vector512.Load(ptr + i + 16);
                    s3 += Vector512.Load(ptr + i + 24);
                }
                var sTot = (s0 + s1) + (s2 + s3);
                double acc = Vector512.Sum(sTot);
                for (; i < len; i++) acc += ptr[i];
                return acc;
            }

            if (Vector256.IsHardwareAccelerated && len >= 16)
            {
                var s0 = Vector256<double>.Zero;
                var s1 = Vector256<double>.Zero;
                var s2 = Vector256<double>.Zero;
                var s3 = Vector256<double>.Zero;
                int limit = len - 16;
                for (; i <= limit; i += 16)
                {
                    s0 += Vector256.Load(ptr + i);
                    s1 += Vector256.Load(ptr + i + 4);
                    s2 += Vector256.Load(ptr + i + 8);
                    s3 += Vector256.Load(ptr + i + 12);
                }
                var sTot = (s0 + s1) + (s2 + s3);
                double acc = Vector256.Sum(sTot);
                for (; i < len; i++) acc += ptr[i];
                return acc;
            }

            double sum = 0;
            for (; i < len; i++) sum += ptr[i];
            return sum;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        internal static unsafe long SumInt32(ReadOnlySpan<int> span)
        {
            int n = span.Length;
            if (n >= 250_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, n / 32768));
                int chunkSize = (n + numChunks - 1) / numChunks;
                long[] partialSums = new long[numChunks];
                fixed (int* p = span)
                {
                    int* ptr = p;
                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int length = Math.Min(chunkSize, n - start);
                        if (length > 0)
                            partialSums[c] = SumInt32Ptr(ptr + start, length);
                    });
                }
                long total = 0;
                for (int c = 0; c < numChunks; c++) total += partialSums[c];
                return total;
            }
            fixed (int* p = span)
            {
                return SumInt32Ptr(p, n);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private static unsafe long SumInt32Ptr(int* ptr, int len)
        {
            if (len == 0) return 0;
            int i = 0;

            if (Vector256.IsHardwareAccelerated && len >= 16)
            {
                var s0 = Vector256<long>.Zero;
                var s1 = Vector256<long>.Zero;
                var s2 = Vector256<long>.Zero;
                var s3 = Vector256<long>.Zero;
                int limit = len - 16;
                for (; i <= limit; i += 16)
                {
                    var v0 = Vector256.Load(ptr + i);
                    var (w0Lo, w0Hi) = Vector256.Widen(v0);
                    s0 += w0Lo;
                    s1 += w0Hi;
                    var v1 = Vector256.Load(ptr + i + 8);
                    var (w1Lo, w1Hi) = Vector256.Widen(v1);
                    s2 += w1Lo;
                    s3 += w1Hi;
                }
                var sTot = (s0 + s1) + (s2 + s3);
                long acc = Vector256.Sum(sTot);
                for (; i < len; i++) acc += ptr[i];
                return acc;
            }

            long sum = 0;
            for (; i < len; i++) sum += ptr[i];
            return sum;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        internal static unsafe double SqSumFloat64(ReadOnlySpan<double> span, double mean)
        {
            int n = span.Length;
            if (n >= 250_000)
            {
                int numChunks = Math.Min(Environment.ProcessorCount, Math.Max(1, n / 32768));
                int chunkSize = (n + numChunks - 1) / numChunks;
                double[] partialSq = new double[numChunks];
                fixed (double* p = span)
                {
                    double* ptr = p;
                    Parallel.For(0, numChunks, c =>
                    {
                        int start = c * chunkSize;
                        int length = Math.Min(chunkSize, n - start);
                        if (length > 0)
                            partialSq[c] = SqSumFloat64Ptr(ptr + start, length, mean);
                    });
                }
                double total = 0;
                for (int c = 0; c < numChunks; c++) total += partialSq[c];
                return total;
            }
            fixed (double* p = span)
            {
                return SqSumFloat64Ptr(p, n, mean);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private static unsafe double SqSumFloat64Ptr(double* ptr, int len, double mean)
        {
            if (len == 0) return 0;
            int i = 0;

            if (Vector512.IsHardwareAccelerated && len >= 32)
            {
                var vMean = Vector512.Create(mean);
                var s0 = Vector512<double>.Zero;
                var s1 = Vector512<double>.Zero;
                var s2 = Vector512<double>.Zero;
                var s3 = Vector512<double>.Zero;
                int limit = len - 32;
                for (; i <= limit; i += 32)
                {
                    var d0 = Vector512.Load(ptr + i) - vMean;
                    s0 += d0 * d0;
                    var d1 = Vector512.Load(ptr + i + 8) - vMean;
                    s1 += d1 * d1;
                    var d2 = Vector512.Load(ptr + i + 16) - vMean;
                    s2 += d2 * d2;
                    var d3 = Vector512.Load(ptr + i + 24) - vMean;
                    s3 += d3 * d3;
                }
                var sTot = (s0 + s1) + (s2 + s3);
                double acc = Vector512.Sum(sTot);
                for (; i < len; i++)
                {
                    double d = ptr[i] - mean;
                    acc += d * d;
                }
                return acc;
            }

            if (Vector256.IsHardwareAccelerated && len >= 16)
            {
                var vMean = Vector256.Create(mean);
                var s0 = Vector256<double>.Zero;
                var s1 = Vector256<double>.Zero;
                var s2 = Vector256<double>.Zero;
                var s3 = Vector256<double>.Zero;
                int limit = len - 16;
                for (; i <= limit; i += 16)
                {
                    var d0 = Vector256.Load(ptr + i) - vMean;
                    s0 += d0 * d0;
                    var d1 = Vector256.Load(ptr + i + 4) - vMean;
                    s1 += d1 * d1;
                    var d2 = Vector256.Load(ptr + i + 8) - vMean;
                    s2 += d2 * d2;
                    var d3 = Vector256.Load(ptr + i + 12) - vMean;
                    s3 += d3 * d3;
                }
                var sTot = (s0 + s1) + (s2 + s3);
                double acc = Vector256.Sum(sTot);
                for (; i < len; i++)
                {
                    double d = ptr[i] - mean;
                    acc += d * d;
                }
                return acc;
            }

            double sqSum = 0;
            for (; i < len; i++)
            {
                double d = ptr[i] - mean;
                sqSum += d * d;
            }
            return sqSum;
        }
    }
}
