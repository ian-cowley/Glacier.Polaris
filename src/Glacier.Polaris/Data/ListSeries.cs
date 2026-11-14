using System;
using Glacier.Formats.Arrow;

namespace Glacier.Polaris.Data
{
    public sealed class ListSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(Array);
        public int Length { get; }

        public Int32Series Offsets { get; }
        public ISeries Values { get; private set; }
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
            if (target is not ListSeries t)
                throw new ArgumentException("Target series type mismatch.", nameof(target));

            ArgumentOutOfRangeException.ThrowIfNegative(offset);
            if (offset + Length > t.Length)
                throw new ArgumentOutOfRangeException(nameof(offset), "Target ListSeries is smaller than source offset + length.");

            var srcOffsets = Offsets.Memory.Span;
            var targetOffsets = t.Offsets.Memory.Span;
            if (offset == 0)
                targetOffsets[0] = 0;

            int baseValueOffset = targetOffsets[offset];
            int currentTargetOffset = baseValueOffset;
            int srcValueStart = srcOffsets[0];
            int totalSrcValues = srcOffsets[Length] - srcValueStart;

            for (int i = 0; i < Length; i++)
            {
                if (ValidityMask.IsNull(i))
                {
                    t.ValidityMask.SetNull(offset + i);
                    targetOffsets[offset + i + 1] = currentTargetOffset;
                }
                else
                {
                    t.ValidityMask.SetValid(offset + i);
                    currentTargetOffset += srcOffsets[i + 1] - srcOffsets[i];
                    targetOffsets[offset + i + 1] = currentTargetOffset;
                }
            }

            if (totalSrcValues == 0)
                return;

            int requiredValuesLen = baseValueOffset + totalSrcValues;
            if (t.Values.Length >= requiredValuesLen && srcValueStart == 0 && Values.Length == totalSrcValues)
            {
                Values.CopyTo(t.Values, baseValueOffset);
                return;
            }

            // Grow target values series and merge existing prefix [0..baseValueOffset) + source slice
            int[] srcIndices = new int[totalSrcValues];
            for (int i = 0; i < totalSrcValues; i++)
                srcIndices[i] = srcValueStart + i;

            ISeries newValues = CreateTargetValuesSeries(Values, requiredValuesLen, t.Values, baseValueOffset, srcIndices);
            if (baseValueOffset == 0)
            {
                Values.Take(newValues, srcIndices);
            }
            else
            {
                int[] prefixIndices = new int[baseValueOffset];
                for (int i = 0; i < baseValueOffset; i++) prefixIndices[i] = i;
                using ISeries prefixCopy = CreateTargetValuesSeries(t.Values, baseValueOffset, t.Values, 0, prefixIndices);
                t.Values.Take(prefixCopy, prefixIndices);
                prefixCopy.CopyTo(newValues, 0);

                using ISeries srcCopy = CreateTargetValuesSeries(Values, totalSrcValues, t.Values, 0, srcIndices);
                Values.Take(srcCopy, srcIndices);
                srcCopy.CopyTo(newValues, baseValueOffset);
            }

            t.Values.Dispose();
            t.Values = newValues;
        }

        private static ISeries CreateTargetValuesSeries(ISeries srcValues, int totalLength, ISeries existingTargetValues, int existingPrefixLen, int[] srcIndices)
        {
            if (srcValues is Utf8StringSeries u8)
            {
                int totalBytes = 0;
                if (existingTargetValues is Utf8StringSeries tu8)
                {
                    for (int i = 0; i < existingPrefixLen; i++)
                        if (!tu8.ValidityMask.IsNull(i)) totalBytes += tu8.GetStringSpan(i).Length;
                }
                for (int i = 0; i < srcIndices.Length; i++)
                {
                    if (!u8.ValidityMask.IsNull(srcIndices[i])) totalBytes += u8.GetStringSpan(srcIndices[i]).Length;
                }
                return new Utf8StringSeries(srcValues.Name, totalLength, totalBytes);
            }

            if (srcValues is BinarySeries bin)
            {
                int totalBytes = 0;
                if (existingTargetValues is BinarySeries tbin)
                {
                    for (int i = 0; i < existingPrefixLen; i++)
                        if (!tbin.ValidityMask.IsNull(i)) totalBytes += tbin.GetSpan(i).Length;
                }
                for (int i = 0; i < srcIndices.Length; i++)
                {
                    if (!bin.ValidityMask.IsNull(srcIndices[i])) totalBytes += bin.GetSpan(srcIndices[i]).Length;
                }
                return new BinarySeries(srcValues.Name, totalLength, totalBytes);
            }

            return srcValues.CloneEmpty(totalLength);
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is ListSeries t)
            {
                var srcOffsets = Offsets.Memory.Span;
                var targetOffsets = t.Offsets.Memory.Span;

                int totalValues = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    if (row >= 0 && !ValidityMask.IsNull(row))
                    {
                        totalValues += srcOffsets[row + 1] - srcOffsets[row];
                    }
                }

                int[] allValueIndices = new int[totalValues];
                int currentTargetOffset = 0;
                targetOffsets[0] = 0;

                for (int i = 0; i < indices.Length; i++)
                {
                    int row = indices[i];
                    if (row < 0 || ValidityMask.IsNull(row))
                    {
                        t.ValidityMask.SetNull(i);
                        targetOffsets[i + 1] = currentTargetOffset;
                        continue;
                    }

                    int start = srcOffsets[row];
                    int len = srcOffsets[row + 1] - start;
                    for (int j = 0; j < len; j++)
                    {
                        allValueIndices[currentTargetOffset + j] = start + j;
                    }

                    currentTargetOffset += len;
                    targetOffsets[i + 1] = currentTargetOffset;
                    t.ValidityMask.SetValid(i);
                }

                ISeries newValues;
                if (Values is Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < allValueIndices.Length; i++)
                    {
                        if (!u8.ValidityMask.IsNull(allValueIndices[i]))
                            totalBytes += u8.GetStringSpan(allValueIndices[i]).Length;
                    }
                    newValues = new Utf8StringSeries(Values.Name, totalValues, totalBytes);
                }
                else if (Values is BinarySeries bin)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < allValueIndices.Length; i++)
                    {
                        if (!bin.ValidityMask.IsNull(allValueIndices[i]))
                            totalBytes += bin.GetSpan(allValueIndices[i]).Length;
                    }
                    newValues = new BinarySeries(Values.Name, totalValues, totalBytes);
                }
                else
                {
                    newValues = Values.CloneEmpty(totalValues);
                }

                if (totalValues > 0)
                {
                    Values.Take(newValues, allValueIndices);
                }

                t.Values.Dispose();
                t.Values = newValues;
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

        public ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Binary, isNullable: ValidityMask.HasNulls);
            var offsetBytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(Offsets.Memory.Span[..(Length + 1)]).ToArray();
            return new ArrowColumn(field, Length, ValidityMask.NullCount, ValidityMask.GetNullBitmapMemory(), offsetBytes, Values.ToArrowColumn().DataBuffer);
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}
