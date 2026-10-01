using System;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
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

        public static ISeries Sum(ISeries series)
        {
            if (series is Int32Series i32)
            {
                var span = i32.Memory.Span;
                var mask = i32.ValidityMask;
                if (!mask.HasNulls)
                {
                    var res = new Int32Series(series.Name + "_sum", 1);
                    res.Memory.Span[0] = (int)SumInt32(span);
                    return res;
                }

                long sum = 0;
                int fullWords = span.Length / 64;
                var vSum = Vector256<long>.Zero;
                ref int basePtr = ref MemoryMarshal.GetReference(span);

                for (int w = 0; w < fullWords; w++)
                {
                    ulong word = mask.GetWord(w);
                    int baseIdx = w * 64;

                    if (word == ulong.MaxValue)
                    {
                        for (int v = 0; v < 8; v++)
                        {
                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 8));
                            var (wLo, wHi) = Vector256.Widen(vData);
                            vSum = Vector256.Add(vSum, wLo);
                            vSum = Vector256.Add(vSum, wHi);
                        }
                    }
                    else if (word == 0UL)
                    {
                        continue;
                    }
                    else
                    {
                        for (int v = 0; v < 8; v++)
                        {
                            byte b = (byte)((word >> (v * 8)) & 0xFF);
                            if (b == 0x00) continue;

                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 8));
                            if (b != 0xFF)
                                vData = Vector256.BitwiseAnd(vData, s_int32MaskLut[b]);

                            var (wLo, wHi) = Vector256.Widen(vData);
                            vSum = Vector256.Add(vSum, wLo);
                            vSum = Vector256.Add(vSum, wHi);
                        }
                    }
                }

                int i = fullWords * 64;
                int rem = span.Length - i;
                int remVectors = rem / 8;
                if (remVectors > 0)
                {
                    ulong remWord = mask.GetWord(fullWords);
                    for (int v = 0; v < remVectors; v++)
                    {
                        byte b = (byte)((remWord >> (v * 8)) & 0xFF);
                        if (b == 0x00) continue;

                        var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, i + v * 8));
                        if (b != 0xFF)
                            vData = Vector256.BitwiseAnd(vData, s_int32MaskLut[b]);

                        var (wLo, wHi) = Vector256.Widen(vData);
                        vSum = Vector256.Add(vSum, wLo);
                        vSum = Vector256.Add(vSum, wHi);
                    }
                    i += remVectors * 8;
                }

                sum = Vector256.Sum(vSum);
                for (; i < span.Length; i++)
                {
                    if (mask.IsValid(i)) sum += span[i];
                }

                var result = new Int32Series(series.Name + "_sum", 1);
                result.Memory.Span[0] = (int)sum;
                return result;
            }

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                if (!mask.HasNulls)
                {
                    var res = new Float64Series(series.Name + "_sum", 1);
                    res.Memory.Span[0] = SumFloat64(span);
                    return res;
                }

                double sum = 0;
                int fullWords = span.Length / 64;
                var vSum = Vector256<double>.Zero;
                ref double basePtr = ref MemoryMarshal.GetReference(span);

                for (int w = 0; w < fullWords; w++)
                {
                    ulong word = mask.GetWord(w);
                    int baseIdx = w * 64;

                    if (word == ulong.MaxValue)
                    {
                        for (int v = 0; v < 16; v++)
                        {
                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 4));
                            vSum = Vector256.Add(vSum, vData);
                        }
                    }
                    else if (word == 0UL)
                    {
                        continue;
                    }
                    else
                    {
                        for (int v = 0; v < 16; v++)
                        {
                            int nibble = (int)((word >> (v * 4)) & 0x0F);
                            if (nibble == 0x00) continue;

                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 4));
                            if (nibble != 0x0F)
                                vData = Vector256.BitwiseAnd(vData.AsInt64(), s_float64MaskLut[nibble]).AsDouble();

                            vSum = Vector256.Add(vSum, vData);
                        }
                    }
                }

                int i = fullWords * 64;
                int rem = span.Length - i;
                int remVectors = rem / 4;
                if (remVectors > 0)
                {
                    ulong remWord = mask.GetWord(fullWords);
                    for (int v = 0; v < remVectors; v++)
                    {
                        int nibble = (int)((remWord >> (v * 4)) & 0x0F);
                        if (nibble == 0x00) continue;

                        var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, i + v * 4));
                        if (nibble != 0x0F)
                            vData = Vector256.BitwiseAnd(vData.AsInt64(), s_float64MaskLut[nibble]).AsDouble();

                        vSum = Vector256.Add(vSum, vData);
                    }
                    i += remVectors * 4;
                }

                sum = Vector256.Sum(vSum);
                for (; i < span.Length; i++)
                {
                    if (mask.IsValid(i)) sum += span[i];
                }

                var result = new Float64Series(series.Name + "_sum", 1);
                result.Memory.Span[0] = sum;
                return result;
            }

            return new NullSeries(series.Name + "_sum", 1);
        }

        public static ISeries Mean(ISeries series)
        {
            if (series is Int32Series i32)
            {
                var span = i32.Memory.Span;
                var mask = i32.ValidityMask;
                if (!mask.HasNulls)
                {
                    var result = new Float64Series(series.Name + "_mean", 1);
                    if (span.Length > 0) result.Memory.Span[0] = (double)SumInt32(span) / span.Length;
                    else result.ValidityMask.SetNull(0);
                    return result;
                }

                long sum = 0;
                int count = 0;
                int fullWords = span.Length / 64;
                var vSum = Vector256<long>.Zero;
                ref int basePtr = ref MemoryMarshal.GetReference(span);

                for (int w = 0; w < fullWords; w++)
                {
                    ulong word = mask.GetWord(w);
                    int baseIdx = w * 64;

                    if (word == ulong.MaxValue)
                    {
                        for (int v = 0; v < 8; v++)
                        {
                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 8));
                            var (wLo, wHi) = Vector256.Widen(vData);
                            vSum = Vector256.Add(vSum, wLo);
                            vSum = Vector256.Add(vSum, wHi);
                        }
                        count += 64;
                    }
                    else if (word == 0UL)
                    {
                        continue;
                    }
                    else
                    {
                        for (int v = 0; v < 8; v++)
                        {
                            byte b = (byte)((word >> (v * 8)) & 0xFF);
                            if (b == 0x00) continue;

                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 8));
                            if (b != 0xFF)
                                vData = Vector256.BitwiseAnd(vData, s_int32MaskLut[b]);

                            var (wLo, wHi) = Vector256.Widen(vData);
                            vSum = Vector256.Add(vSum, wLo);
                            vSum = Vector256.Add(vSum, wHi);
                            count += System.Numerics.BitOperations.PopCount(b);
                        }
                    }
                }

                int i = fullWords * 64;
                int rem = span.Length - i;
                int remVectors = rem / 8;
                if (remVectors > 0)
                {
                    ulong remWord = mask.GetWord(fullWords);
                    for (int v = 0; v < remVectors; v++)
                    {
                        byte b = (byte)((remWord >> (v * 8)) & 0xFF);
                        if (b == 0x00) continue;

                        var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, i + v * 8));
                        if (b != 0xFF)
                            vData = Vector256.BitwiseAnd(vData, s_int32MaskLut[b]);

                        var (wLo, wHi) = Vector256.Widen(vData);
                        vSum = Vector256.Add(vSum, wLo);
                        vSum = Vector256.Add(vSum, wHi);
                        count += System.Numerics.BitOperations.PopCount(b);
                    }
                    i += remVectors * 8;
                }

                sum = Vector256.Sum(vSum);
                for (; i < span.Length; i++)
                {
                    if (mask.IsValid(i)) { sum += span[i]; count++; }
                }

                var res = new Float64Series(series.Name + "_mean", 1);
                if (count > 0) res.Memory.Span[0] = (double)sum / count;
                else res.ValidityMask.SetNull(0);
                return res;
            }

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                var mask = f64.ValidityMask;
                if (!mask.HasNulls)
                {
                    var result = new Float64Series(series.Name + "_mean", 1);
                    if (span.Length > 0) result.Memory.Span[0] = SumFloat64(span) / span.Length;
                    else result.ValidityMask.SetNull(0);
                    return result;
                }

                double sum = 0;
                int count = 0;
                int fullWords = span.Length / 64;
                var vSum = Vector256<double>.Zero;
                ref double basePtr = ref MemoryMarshal.GetReference(span);

                for (int w = 0; w < fullWords; w++)
                {
                    ulong word = mask.GetWord(w);
                    int baseIdx = w * 64;

                    if (word == ulong.MaxValue)
                    {
                        for (int v = 0; v < 16; v++)
                        {
                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 4));
                            vSum = Vector256.Add(vSum, vData);
                        }
                        count += 64;
                    }
                    else if (word == 0UL)
                    {
                        continue;
                    }
                    else
                    {
                        for (int v = 0; v < 16; v++)
                        {
                            int nibble = (int)((word >> (v * 4)) & 0x0F);
                            if (nibble == 0x00) continue;

                            var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, baseIdx + v * 4));
                            if (nibble != 0x0F)
                                vData = Vector256.BitwiseAnd(vData.AsInt64(), s_float64MaskLut[nibble]).AsDouble();

                            vSum = Vector256.Add(vSum, vData);
                            count += System.Numerics.BitOperations.PopCount((uint)nibble);
                        }
                    }
                }

                int i = fullWords * 64;
                int rem = span.Length - i;
                int remVectors = rem / 4;
                if (remVectors > 0)
                {
                    ulong remWord = mask.GetWord(fullWords);
                    for (int v = 0; v < remVectors; v++)
                    {
                        int nibble = (int)((remWord >> (v * 4)) & 0x0F);
                        if (nibble == 0x00) continue;

                        var vData = Vector256.LoadUnsafe(ref Unsafe.Add(ref basePtr, i + v * 4));
                        if (nibble != 0x0F)
                            vData = Vector256.BitwiseAnd(vData.AsInt64(), s_float64MaskLut[nibble]).AsDouble();

                        vSum = Vector256.Add(vSum, vData);
                        count += System.Numerics.BitOperations.PopCount((uint)nibble);
                    }
                    i += remVectors * 4;
                }

                sum = Vector256.Sum(vSum);
                for (; i < span.Length; i++)
                {
                    if (mask.IsValid(i)) { sum += span[i]; count++; }
                }

                var res = new Float64Series(series.Name + "_mean", 1);
                if (count > 0) res.Memory.Span[0] = sum / count;
                else res.ValidityMask.SetNull(0);
                return res;
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
                    var result = new Float64Series(series.Name + "_std", 1);
                    if (n < 2)
                    {
                        result[0] = double.NaN;
                        return result;
                    }

                    double mean = SumFloat64(span) / n;
                    double sqSum = SqSumFloat64(span, mean);
                    result[0] = Math.Sqrt(sqSum / (n - 1));
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
        /// <summary>Computes Shannon entropy of the series (using natural log, base e).</summary>
        public static double Entropy(ISeries series)
        {
            var frequencies = new Dictionary<double, int>();
            int validCount = 0;

            if (series is Float64Series f64)
            {
                var span = f64.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    if (f64.ValidityMask.IsValid(i))
                    {
                        double val = span[i];
                        frequencies.TryGetValue(val, out int c);
                        frequencies[val] = c + 1;
                        validCount++;
                    }
                }
            }
            else
            {
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsValid(i))
                    {
                        double val = Convert.ToDouble(series.Get(i));
                        frequencies.TryGetValue(val, out int c);
                        frequencies[val] = c + 1;
                        validCount++;
                    }
                }
            }

            double entropy = 0.0;
            foreach (var kvp in frequencies)
            {
                double p = (double)kvp.Value / validCount;
                entropy -= p * Math.Log(p);
            }

            return entropy;
        }
    }
}
