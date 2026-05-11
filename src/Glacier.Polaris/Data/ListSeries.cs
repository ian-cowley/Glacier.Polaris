using System;

namespace Glacier.Polaris.Data
{
    public sealed class ListSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(Array);
        public int Length { get; }

        public Int32Series Offsets { get; }
        public ISeries Values { get; }
        public Memory.ValidityMask ValidityMask { get; }

        public ListSeries(string name, Int32Series offsets, ISeries values)
        {
            Name = name;
            Length = offsets.Length - 1;
            Offsets = offsets;
            Values = values;
            ValidityMask = new Memory.ValidityMask(Length);
        }

        public void CopyTo(ISeries target, int offset)
        {
            if (target is ListSeries t)
            {
                // This is complex because we need to append values and adjust offsets
                throw new NotImplementedException("CopyTo for ListSeries requires complex values merging.");
            }
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is ListSeries t)
            {
                var srcOffsets = Offsets.Memory.Span;
                var targetOffsets = t.Offsets.Memory.Span;

                // We need to count total values to take
                int totalValues = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    totalValues += srcOffsets[row + 1] - srcOffsets[row];
                }

                // Create a new values series of the correct size
                var newValues = (ISeries)Activator.CreateInstance(Values.GetType(), Values.Name, totalValues)!;

                int currentTargetOffset = 0;
                targetOffsets[0] = 0;

                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    int start = srcOffsets[row];
                    int len = srcOffsets[row + 1] - start;

                    if (len > 0)
                    {
                        // Use a temporary indices buffer to take from values
                        int[] valueIndices = new int[len];
                        for (int j = 0; j < len; j++) valueIndices[j] = start + j;
                        Values.Take(newValues, valueIndices); // This is slow, ideally we'd have a slice-copy
                    }

                    currentTargetOffset += len;
                    targetOffsets[i + 1] = currentTargetOffset;
                }

                // Copy validity mask
                ValidityMask.Take(t.ValidityMask, indices);
            }
            else throw new ArgumentException("Target series type mismatch.");
        }

        public ISeries Explode()
        {
            // Simply returns the underlying flat values.
            // The engine must handle row repetition for other columns.
            return Values;
        }
        public void Dispose()
        {
            Offsets.Dispose();
            Values.Dispose();
        }
        public object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            var start = Offsets.Memory.Span[i];
            var end = Offsets.Memory.Span[i + 1];
            var len = end - start;
            var result = new List<object?>(len);
            for (int j = start; j < end; j++)
            {
                result.Add(Values.Get(j));
            }
            return result.ToArray();
        }
        public ISeries CloneEmpty(int length)
        {
            var emptyOffsets = new Int32Series(Offsets.Name, length + 1);
            var emptyValues = Values.CloneEmpty(0); // Start with 0 values
            return new ListSeries(Name, emptyOffsets, emptyValues);
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            throw new NotSupportedException("ListSeries.Take(int,int) is not supported.");
        }

        public Apache.Arrow.IArrowArray ToArrowArray()
        {
            var valueArray = Values.ToArrowArray();
            var offsetBuf = new Apache.Arrow.ArrowBuffer.Builder<int>();
            var offsetSpan = Offsets.Memory.Span;
            for (int i = 0; i <= Length; i++) offsetBuf.Append(offsetSpan[i]);
            var nullBitmapBuilder = new Apache.Arrow.ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));
            return new Apache.Arrow.ListArray(
                new Apache.Arrow.Types.ListType(valueArray.Data.DataType),
                Length,
                offsetBuf.Build(),
                valueArray,
                nullBitmapBuilder.Build());
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}
