using System;
using System.Linq;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class ConditionalKernels
    {
        public static ISeries Select(ISeries condition, ISeries thenResult, ISeries otherwiseResult)
        {
            if (condition is BooleanSeries condBool)
            {
                var condSpan = condBool.Memory.Span;
                var seriesType = thenResult.GetType();
                
                if (thenResult is Utf8StringSeries tStr && otherwiseResult is Utf8StringSeries oStr)
                {
                    return SelectStrings(condSpan, tStr, oStr);
                }

                // Generic unmanaged select
                var result = (ISeries)Activator.CreateInstance(seriesType, thenResult.Name, thenResult.Length)!;
                
                // Use reflection to call a generic helper for performance if needed, 
                // but for now let's use the known pattern.
                if (thenResult is Series<int> t32 && otherwiseResult is Series<int> o32)
                    SelectGeneric(condSpan, t32.Memory.Span, o32.Memory.Span, ((Series<int>)result).Memory.Span);
                else if (thenResult is Series<long> t64 && otherwiseResult is Series<long> o64)
                    SelectGeneric(condSpan, t64.Memory.Span, o64.Memory.Span, ((Series<long>)result).Memory.Span);
                else if (thenResult is Series<double> tDbl && otherwiseResult is Series<double> oDbl)
                    SelectGeneric(condSpan, tDbl.Memory.Span, oDbl.Memory.Span, ((Series<double>)result).Memory.Span);
                else if (thenResult is Series<float> tFlt && otherwiseResult is Series<float> oFlt)
                    SelectGeneric(condSpan, tFlt.Memory.Span, oFlt.Memory.Span, ((Series<float>)result).Memory.Span);
                else
                    throw new NotSupportedException($"Conditional select not supported for {thenResult.GetType().Name}.");

                return result;
            }
            
            // Fallback for Int32 masks (legacy)
            if (condition is Int32Series condI32)
            {
                var condSpan = condI32.Memory.Span;
                // ... (previous logic converted to Boolean internally or just handled here)
                var boolMask = new BooleanSeries("mask", condSpan.Length);
                var boolSpan = boolMask.Memory.Span;
                for (int i = 0; i < condSpan.Length; i++) boolSpan[i] = condSpan[i] != 0;
                return Select(boolMask, thenResult, otherwiseResult);
            }
            
            throw new NotSupportedException($"Conditional select requires BooleanSeries mask.");
        }

        private static void SelectGeneric<T>(ReadOnlySpan<bool> condition, ReadOnlySpan<T> thenSpan, ReadOnlySpan<T> otherwiseSpan, Span<T> resultSpan) where T : unmanaged
        {
            for (int i = 0; i < condition.Length; i++)
            {
                resultSpan[i] = condition[i] ? thenSpan[i] : otherwiseSpan[i];
            }
        }

        private static ISeries SelectStrings(ReadOnlySpan<bool> condition, Utf8StringSeries thenStr, Utf8StringSeries otherwiseStr)
        {
            int length = condition.Length;
            int totalBytes = 0;
            var tOffsets = thenStr.Offsets.Span;
            var oOffsets = otherwiseStr.Offsets.Span;

            for (int i = 0; i < length; i++)
            {
                if (condition[i]) totalBytes += tOffsets[i + 1] - tOffsets[i];
                else totalBytes += oOffsets[i + 1] - oOffsets[i];
            }

            var result = new Utf8StringSeries(thenStr.Name, length, totalBytes);
            var resData = result.DataBytes.Span;
            var resOffsets = result.Offsets.Span;
            var tData = thenStr.DataBytes.Span;
            var oData = otherwiseStr.DataBytes.Span;

            int currentOffset = 0;
            for (int i = 0; i < length; i++)
            {
                resOffsets[i] = currentOffset;
                if (condition[i])
                {
                    var len = tOffsets[i + 1] - tOffsets[i];
                    tData.Slice(tOffsets[i], len).CopyTo(resData.Slice(currentOffset));
                    currentOffset += len;
                }
                else
                {
                    var len = oOffsets[i + 1] - oOffsets[i];
                    oData.Slice(oOffsets[i], len).CopyTo(resData.Slice(currentOffset));
                    currentOffset += len;
                }
            }
            resOffsets[length] = currentOffset;
            return result;
        }
    }

    public static class ComparisonKernels
    {
        public static BooleanSeries Compare<T>(Series<T> left, Series<T> right, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            var result = new BooleanSeries(left.Name + "_bool", left.Length);
            var lSpan = left.Memory.Span;
            var rSpan = right.Memory.Span;
            var resSpan = result.Memory.Span;
            for (int i = 0; i < lSpan.Length; i++)
            {
                resSpan[i] = Evaluate(lSpan[i], rSpan[i], op);
            }
            return result;
        }

        public static BooleanSeries CompareScalar<T>(Series<T> left, T right, FilterOperation op) where T : unmanaged, IComparable<T>
        {
            var result = new BooleanSeries(left.Name + "_bool", left.Length);
            var lSpan = left.Memory.Span;
            var resSpan = result.Memory.Span;
            for (int i = 0; i < lSpan.Length; i++)
            {
                resSpan[i] = Evaluate(lSpan[i], right, op);
            }
            return result;
        }

        private static bool Evaluate<T>(T left, T right, FilterOperation op) where T : IComparable<T>
        {
            int cmp = left.CompareTo(right);
            return op switch
            {
                FilterOperation.Equal => cmp == 0,
                FilterOperation.NotEqual => cmp != 0,
                FilterOperation.GreaterThan => cmp > 0,
                FilterOperation.GreaterThanOrEqual => cmp >= 0,
                FilterOperation.LessThan => cmp < 0,
                FilterOperation.LessThanOrEqual => cmp <= 0,
                _ => false
            };
        }
    }
}
