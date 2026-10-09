using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class AggregationKernels
    {
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

            if (series is Int64Series i64)
            {
                var span = i64.Memory.Span;
                var mask = i64.ValidityMask;
                if (!mask.HasNulls)
                {
                    var res = new Int64Series(series.Name + "_sum", 1);
                    res.Memory.Span[0] = SumInt64(span);
                    return res;
                }

                long sum = 0;
                int fullWords = span.Length / 64;
                var vSum = Vector256<long>.Zero;
                ref long basePtr = ref MemoryMarshal.GetReference(span);

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
                                vData = Vector256.BitwiseAnd(vData, s_float64MaskLut[nibble]);

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
                            vData = Vector256.BitwiseAnd(vData, s_float64MaskLut[nibble]);

                        vSum = Vector256.Add(vSum, vData);
                    }
                    i += remVectors * 4;
                }

                sum = Vector256.Sum(vSum);
                for (; i < span.Length; i++)
                {
                    if (mask.IsValid(i)) sum += span[i];
                }

                var result = new Int64Series(series.Name + "_sum", 1);
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
        }

        /// <summary>SIMD-accelerated single-pass Welford Var — Float64 uses Vector256 load.</summary>
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
