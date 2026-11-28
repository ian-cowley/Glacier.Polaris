using System;
using Glacier.Formats.Arrow;

namespace Glacier.Polaris.Data
{
    public sealed class ArraySeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(System.Array);
        public int Length { get; }
        public int Width { get; }

        public ISeries Values { get; internal set; }
        public Memory.ValidityMask ValidityMask { get; }

        public ArraySeries(string name, int width, ISeries values)
        {
            Name = name;
            Width = width;
            Length = values.Length / width;
            Values = values;
            ValidityMask = new Memory.ValidityMask(Length);
        }

        public void CopyTo(ISeries target, int offset)
        {
            if (target is not ArraySeries t)
                throw new InvalidOperationException($"Type mismatch in CopyTo: cannot copy ArraySeries to {target?.DataType.Name ?? "null"}");

            if (t.Width != Width)
                throw new ArgumentException($"ArraySeries width mismatch in CopyTo: source width {Width} != target width {t.Width}.", nameof(target));

            ArgumentOutOfRangeException.ThrowIfNegative(offset);
            if (offset > t.Length - Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (!ListSeries.AreValuesCompatible(Values, t.Values))
                throw new InvalidOperationException($"Target ArraySeries values mismatch: expected {Values.GetType().Name}, got {t.Values.GetType().Name}.");

            if (Length == 0 || ReferenceEquals(this, t))
                return;

            int targetValueOffset = checked(offset * Width);
            int srcCount = checked(Length * Width);
            if (Values is Utf8StringSeries srcUtf8 && t.Values is Utf8StringSeries dstUtf8)
            {
                int oldEnd = checked(targetValueOffset + srcCount);
                int suffixCount = dstUtf8.Length - oldEnd;
                var replacement = ListSeries.SpliceUtf8Series(
                    srcUtf8,
                    dstUtf8,
                    targetValueOffset,
                    oldEnd,
                    suffixCount,
                    0,
                    srcCount,
                    null,
                    dstUtf8.Length,
                    allowInPlace: true);
                if (replacement != null)
                {
                    t.Values = replacement;
                    dstUtf8.Dispose();
                }
            }
            else if (Values is BinarySeries srcBin && t.Values is BinarySeries dstBin)
            {
                int oldEnd = checked(targetValueOffset + srcCount);
                int suffixCount = dstBin.Length - oldEnd;
                var replacement = ListSeries.SpliceBinarySeries(
                    srcBin,
                    dstBin,
                    targetValueOffset,
                    oldEnd,
                    suffixCount,
                    0,
                    srcCount,
                    null,
                    dstBin.Length,
                    allowInPlace: true);
                if (replacement != null)
                {
                    t.Values = replacement;
                    dstBin.Dispose();
                }
            }
            else
            {
                Values.CopyTo(t.Values, targetValueOffset);
            }

            ValidityMask.CopyRangeTo(t.ValidityMask, offset);
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is ArraySeries t && t.Width == this.Width)
            {
                int[] valueIndices = new int[indices.Length * Width];
                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    for (int j = 0; j < Width; j++)
                    {
                        valueIndices[i * Width + j] = (row < 0) ? -1 : (row * Width + j);
                    }
                }

                if (Values is Utf8StringSeries srcUtf8 && t.Values is Utf8StringSeries dstUtf8)
                {
                    var replacement = ListSeries.TakeUtf8Into(srcUtf8, dstUtf8, valueIndices);
                    if (replacement != null)
                    {
                        t.Values = replacement;
                        dstUtf8.Dispose();
                    }
                }
                else if (Values is BinarySeries srcBin && t.Values is BinarySeries dstBin)
                {
                    var replacement = ListSeries.TakeBinaryInto(srcBin, dstBin, valueIndices);
                    if (replacement != null)
                    {
                        t.Values = replacement;
                        dstBin.Dispose();
                    }
                }
                else
                {
                    Values.Take(t.Values, valueIndices);
                }

                ValidityMask.Take(t.ValidityMask, indices);
            }
        }

        public void Dispose()
        {
            Values.Dispose();
        }

        public object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            var result = new object?[Width];
            for (int j = 0; j < Width; j++)
            {
                result[j] = Values.Get(i * Width + j);
            }
            return result;
        }

        public ISeries CloneEmpty(int length)
        {
            return new ArraySeries(Name, Width, Values.CloneEmpty(length * Width));
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
             for(int j=0; j<Width; j++) {
                 Values.Take(((ArraySeries)target).Values, srcIdx * Width + j, targetIdx * Width + j);
             }
             if (ValidityMask.IsNull(srcIdx)) target.ValidityMask.SetNull(targetIdx);
             else target.ValidityMask.SetValid(targetIdx);
        }

        public ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Binary, isNullable: ValidityMask.HasNulls);
            return new ArrowColumn(field, Length, ValidityMask.NullCount, ValidityMask.GetNullBitmapMemory(), ReadOnlyMemory<byte>.Empty, Values.ToArrowColumn().DataBuffer);
        }
    }
}
