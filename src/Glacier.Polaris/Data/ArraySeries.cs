using System;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Glacier.Polaris.Data
{
    public sealed class ArraySeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(System.Array);
        public int Length { get; }
        public int Width { get; }

        public ISeries Values { get; }
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
            if (target is ArraySeries t && t.Width == this.Width)
            {
                Values.CopyTo(t.Values, offset * Width);
                ValidityMask.CopyTo(t.ValidityMask, offset);
            }
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
                        valueIndices[i * Width + j] = (row == -1) ? -1 : (row * Width + j);
                    }
                }
                Values.Take(t.Values, valueIndices);
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

        public IArrowArray ToArrowArray()
        {
            var valueArray = Values.ToArrowArray();
            var nullBitmapBuilder = new Apache.Arrow.ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++)
            {
                if (ValidityMask.IsValid(i)) nullBitmapBuilder.Append(true);
                else nullBitmapBuilder.Append(false);
            }

            return new FixedSizeListArray(new FixedSizeListType(valueArray.Data.DataType, Width), Length, valueArray, nullBitmapBuilder.Build());
        }
    }
}
