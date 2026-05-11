using System;
using System.Runtime.CompilerServices;

namespace Glacier.Polaris.Compute
{
    /// <summary>FillNull kernels using bitmap-scan for bulk fills (only visits null positions).</summary>
    public static class FillNullKernels
    {
        public static ISeries FillNull(ISeries source, FillStrategy strategy)
        {
            if (source is Data.Int32Series i32) return FillInt32(i32, strategy);
            if (source is Data.Float64Series f64) return FillFloat64(f64, strategy);
            if (source is Data.Utf8StringSeries utf8) return FillUtf8(utf8, strategy);
            return source;
        }

        public static ISeries FillWithValue(ISeries source, ISeries value)
        {
            if (source is Data.Int32Series i32 && value is Data.Int32Series v32)
            {
                var result = new Data.Int32Series(source.Name, source.Length);
                var srcSpan = i32.Memory.Span;
                var resSpan = result.Memory.Span;
                int fillVal = v32.Memory.Span[0];
                Memory.ValidityMask.BulkFillNulls(srcSpan, resSpan, source.ValidityMask, fillVal, result.ValidityMask);
                return result;
            }
            if (source is Data.Float64Series f64 && value is Data.Float64Series v64)
            {
                var result = new Data.Float64Series(source.Name, source.Length);
                var srcSpan = f64.Memory.Span;
                var resSpan = result.Memory.Span;
                double fillVal = v64.Memory.Span[0];
                Memory.ValidityMask.BulkFillNulls(srcSpan, resSpan, source.ValidityMask, fillVal, result.ValidityMask);
                return result;
            }
            if (source is Data.Utf8StringSeries utf8 && value is Data.Utf8StringSeries vStr)
            {
                string? fillVal = vStr.GetString(0);
                string?[] data = new string?[source.Length];

                if (!source.ValidityMask.HasNulls)
                {
                    for (int i = 0; i < source.Length; i++) data[i] = utf8.GetString(i);
                    return Data.Utf8StringSeries.FromStrings(source.Name, data);
                }

                for (int i = 0; i < source.Length; i++)
                {
                    data[i] = source.ValidityMask.IsValid(i) ? utf8.GetString(i) : fillVal;
                }
                return Data.Utf8StringSeries.FromStrings(source.Name, data);
            }
            return source;
        }
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe ISeries FillInt32(Data.Int32Series source, FillStrategy strategy)
        {
            var result = new Data.Int32Series(source.Name, source.Length);
            var srcSpan = source.Memory.Span;
            var resSpan = result.Memory.Span;
            var mask = source.ValidityMask;
            int n = source.Length;

            if (strategy == FillStrategy.Forward)
            {
                if (!mask.HasNulls)
                {
                    srcSpan.CopyTo(resSpan);
                    result.ValidityMask.SetAllValid();
                    return result;
                }

                fixed (int* pSrc = srcSpan)
                fixed (int* pDst = resSpan)
                fixed (ulong* pBits = &mask.GetRawBitsRef())
                fixed (ulong* pResBits = &result.ValidityMask.GetRawBitsRef())
                {
                    int lastVal = 0;
                    bool hasVal = false;
                    int wordCount = (n + 63) / 64;

                    for (int w = 0; w < wordCount; w++)
                    {
                        ulong word = pBits[w];
                        int startIdx = w * 64;
                        int endIdx = startIdx + 64 > n ? n : startIdx + 64;
                        ulong resWord = 0;

                        if (word == 0)
                        {
                            if (hasVal)
                            {
                                for (int i = startIdx; i < endIdx; i++)
                                    pDst[i] = lastVal;
                                resWord = ulong.MaxValue;
                            }
                        }
                        else if (word == ulong.MaxValue && endIdx - startIdx == 64)
                        {
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(
                                pDst + startIdx, pSrc + startIdx, 64 * sizeof(int));
                            lastVal = pSrc[endIdx - 1];
                            hasVal = true;
                            resWord = ulong.MaxValue;
                        }
                        else
                        {
                            ulong bit = 1;
                            for (int i = startIdx; i < endIdx; i++)
                            {
                                if ((word & 1) == 1)
                                {
                                    int v = pSrc[i];
                                    pDst[i] = v;
                                    lastVal = v;
                                    hasVal = true;
                                }
                                else if (hasVal)
                                {
                                    pDst[i] = lastVal;
                                }

                                if (hasVal)
                                {
                                    resWord |= bit;
                                }

                                word >>= 1;
                                bit <<= 1;
                            }
                        }

                        if (w == wordCount - 1 && n % 64 != 0)
                        {
                            resWord |= (ulong.MaxValue << (n % 64));
                        }
                        pResBits[w] = resWord;
                    }
                }
                return result;
            }

            if (strategy == FillStrategy.Backward)
            {
                if (!mask.HasNulls)
                {
                    srcSpan.CopyTo(resSpan);
                    result.ValidityMask.SetAllValid();
                    return result;
                }

                fixed (int* pSrc = srcSpan)
                fixed (int* pDst = resSpan)
                fixed (ulong* pBits = &mask.GetRawBitsRef())
                fixed (ulong* pResBits = &result.ValidityMask.GetRawBitsRef())
                {
                    int lastVal = 0;
                    bool hasVal = false;
                    int wordCount = (n + 63) / 64;

                    for (int w = wordCount - 1; w >= 0; w--)
                    {
                        ulong word = pBits[w];
                        int startIdx = w * 64;
                        int endIdx = startIdx + 64 > n ? n : startIdx + 64;
                        int count = endIdx - startIdx;
                        ulong resWord = 0;

                        if (word == 0)
                        {
                            if (hasVal)
                            {
                                for (int i = endIdx - 1; i >= startIdx; i--)
                                    pDst[i] = lastVal;
                                resWord = ulong.MaxValue;
                            }
                        }
                        else if (word == ulong.MaxValue && count == 64)
                        {
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(
                                pDst + startIdx, pSrc + startIdx, 64 * sizeof(int));
                            lastVal = pSrc[startIdx];
                            hasVal = true;
                            resWord = ulong.MaxValue;
                        }
                        else
                        {
                            ulong shiftedWord = count == 64 ? word : (word << (64 - count));
                            ulong maskBit = 1ul << 63;

                            for (int i = endIdx - 1; i >= startIdx; i--)
                            {
                                if ((shiftedWord & maskBit) != 0)
                                {
                                    int v = pSrc[i];
                                    pDst[i] = v;
                                    lastVal = v;
                                    hasVal = true;
                                }
                                else if (hasVal)
                                {
                                    pDst[i] = lastVal;
                                }

                                if (hasVal)
                                {
                                    resWord |= (1ul << (i - startIdx));
                                }

                                shiftedWord <<= 1;
                            }
                        }

                        if (w == wordCount - 1 && n % 64 != 0)
                        {
                            resWord |= (ulong.MaxValue << (n % 64));
                        }
                        pResBits[w] = resWord;
                    }
                }
                return result;
            }

            // Bulk fill strategies
            int fillVal = 0;
            bool found = false;

            if (strategy == FillStrategy.Min)
            {
                int min = int.MaxValue;
                for (int i = 0; i < n; i++) if (mask.IsValid(i)) { min = Math.Min(min, srcSpan[i]); found = true; }
                fillVal = min;
            }
            else if (strategy == FillStrategy.Max)
            {
                int max = int.MinValue;
                for (int i = 0; i < n; i++) if (mask.IsValid(i)) { max = Math.Max(max, srcSpan[i]); found = true; }
                fillVal = max;
            }
            else if (strategy == FillStrategy.Mean)
            {
                long sum = 0; int count = 0;
                for (int i = 0; i < n; i++) if (mask.IsValid(i)) { sum += srcSpan[i]; count++; found = true; }
                if (count > 0) fillVal = (int)(sum / count);
            }
            else if (strategy == FillStrategy.Zero) { fillVal = 0; found = true; }
            else if (strategy == FillStrategy.One) { fillVal = 1; found = true; }

            if (!found) return result;

            Memory.ValidityMask.BulkFillNulls(srcSpan, resSpan, mask, fillVal, result.ValidityMask);
            return result;
        }
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe ISeries FillFloat64(Data.Float64Series source, FillStrategy strategy)
        {
            var result = new Data.Float64Series(source.Name, source.Length);
            var srcSpan = source.Memory.Span;
            var resSpan = result.Memory.Span;
            var mask = source.ValidityMask;
            var resMask = result.ValidityMask;
            int n = source.Length;

            if (strategy == FillStrategy.Forward)
            {
                if (!mask.HasNulls)
                {
                    srcSpan.CopyTo(resSpan);
                    resMask.SetAllValid();
                    return result;
                }

                fixed (double* pSrc = srcSpan)
                fixed (double* pDst = resSpan)
                fixed (ulong* pBits = &mask.GetRawBitsRef())
                fixed (ulong* pResBits = &resMask.GetRawBitsRef())
                {
                    double lastVal = 0;
                    bool hasVal = false;
                    int wordCount = (n + 63) / 64;

                    for (int w = 0; w < wordCount; w++)
                    {
                        ulong word = pBits[w];
                        int startIdx = w * 64;
                        int endIdx = startIdx + 64 > n ? n : startIdx + 64;
                        ulong resWord = 0;

                        if (word == 0)
                        {
                            if (hasVal)
                            {
                                for (int i = startIdx; i < endIdx; i++)
                                    pDst[i] = lastVal;
                                resWord = ulong.MaxValue;
                            }
                        }
                        else if (word == ulong.MaxValue && endIdx - startIdx == 64)
                        {
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(
                                pDst + startIdx, pSrc + startIdx, 64 * sizeof(double));
                            lastVal = pSrc[endIdx - 1];
                            hasVal = true;
                            resWord = ulong.MaxValue;
                        }
                        else
                        {
                            ulong bit = 1;
                            for (int i = startIdx; i < endIdx; i++)
                            {
                                if ((word & 1) == 1)
                                {
                                    double v = pSrc[i];
                                    pDst[i] = v;
                                    lastVal = v;
                                    hasVal = true;
                                }
                                else if (hasVal)
                                {
                                    pDst[i] = lastVal;
                                }

                                if (hasVal)
                                {
                                    resWord |= bit;
                                }

                                word >>= 1;
                                bit <<= 1;
                            }
                        }

                        if (w == wordCount - 1 && n % 64 != 0)
                        {
                            resWord |= (ulong.MaxValue << (n % 64));
                        }
                        pResBits[w] = resWord;
                    }
                }
                return result;
            }

            if (strategy == FillStrategy.Backward)
            {
                if (!mask.HasNulls)
                {
                    srcSpan.CopyTo(resSpan);
                    resMask.SetAllValid();
                    return result;
                }

                fixed (double* pSrc = srcSpan)
                fixed (double* pDst = resSpan)
                fixed (ulong* pBits = &mask.GetRawBitsRef())
                fixed (ulong* pResBits = &resMask.GetRawBitsRef())
                {
                    double lastVal = 0;
                    bool hasVal = false;
                    int wordCount = (n + 63) / 64;

                    for (int w = wordCount - 1; w >= 0; w--)
                    {
                        ulong word = pBits[w];
                        int startIdx = w * 64;
                        int endIdx = startIdx + 64 > n ? n : startIdx + 64;
                        int count = endIdx - startIdx;
                        ulong resWord = 0;

                        if (word == 0)
                        {
                            if (hasVal)
                            {
                                for (int i = endIdx - 1; i >= startIdx; i--)
                                    pDst[i] = lastVal;
                                resWord = ulong.MaxValue;
                            }
                        }
                        else if (word == ulong.MaxValue && count == 64)
                        {
                            System.Runtime.CompilerServices.Unsafe.CopyBlock(
                                pDst + startIdx, pSrc + startIdx, 64 * sizeof(double));
                            lastVal = pSrc[startIdx];
                            hasVal = true;
                            resWord = ulong.MaxValue;
                        }
                        else
                        {
                            ulong shiftedWord = count == 64 ? word : (word << (64 - count));
                            ulong maskBit = 1ul << 63;

                            for (int i = endIdx - 1; i >= startIdx; i--)
                            {
                                if ((shiftedWord & maskBit) != 0)
                                {
                                    double v = pSrc[i];
                                    pDst[i] = v;
                                    lastVal = v;
                                    hasVal = true;
                                }
                                else if (hasVal)
                                {
                                    pDst[i] = lastVal;
                                }

                                if (hasVal)
                                {
                                    resWord |= (1ul << (i - startIdx));
                                }

                                shiftedWord <<= 1;
                            }
                        }

                        if (w == wordCount - 1 && n % 64 != 0)
                        {
                            resWord |= (ulong.MaxValue << (n % 64));
                        }
                        pResBits[w] = resWord;
                    }
                }
                return result;
            }

            double fillVal = 0;
            bool found = false;

            if (strategy == FillStrategy.Min)
            {
                double min = double.MaxValue;
                for (int i = 0; i < n; i++) if (mask.IsValid(i)) { min = Math.Min(min, srcSpan[i]); found = true; }
                fillVal = min;
            }
            else if (strategy == FillStrategy.Max)
            {
                double max = double.MinValue;
                for (int i = 0; i < n; i++) if (mask.IsValid(i)) { max = Math.Max(max, srcSpan[i]); found = true; }
                fillVal = max;
            }
            else if (strategy == FillStrategy.Mean)
            {
                double sum = 0; int count = 0;
                for (int i = 0; i < n; i++) if (mask.IsValid(i)) { sum += srcSpan[i]; count++; found = true; }
                if (count > 0) fillVal = sum / count;
            }
            else if (strategy == FillStrategy.Zero) { fillVal = 0.0; found = true; }
            else if (strategy == FillStrategy.One) { fillVal = 1.0; found = true; }

            if (!found) return result;

            Memory.ValidityMask.BulkFillNulls(srcSpan, resSpan, mask, fillVal, result.ValidityMask);
            return result;
        }
        private static ISeries FillUtf8(Data.Utf8StringSeries source, FillStrategy strategy)
        {
            string?[] data = new string?[source.Length];
            string? lastValue = null;

            if (strategy == FillStrategy.Forward)
            {
                for (int i = 0; i < source.Length; i++)
                {
                    if (source.ValidityMask.IsValid(i)) { data[i] = source.GetString(i); lastValue = data[i]; }
                    else data[i] = lastValue;
                }
            }
            else if (strategy == FillStrategy.Backward)
            {
                for (int i = source.Length - 1; i >= 0; i--)
                {
                    if (source.ValidityMask.IsValid(i)) { data[i] = source.GetString(i); lastValue = data[i]; }
                    else data[i] = lastValue;
                }
            }
            else if (strategy == FillStrategy.Zero)
            {
                for (int i = 0; i < source.Length; i++)
                    data[i] = source.ValidityMask.IsValid(i) ? source.GetString(i) : "";
            }
            else
            {
                for (int i = 0; i < source.Length; i++)
                    data[i] = source.ValidityMask.IsValid(i) ? source.GetString(i) : null;
            }

            return Data.Utf8StringSeries.FromStrings(source.Name, data);
        }
    }
}
