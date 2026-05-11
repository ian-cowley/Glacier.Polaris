using Glacier.Polaris.Memory;
using Apache.Arrow;

namespace Glacier.Polaris.Data
{
    public sealed class NullSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(object);
        public int Length { get; }
        public ValidityMask ValidityMask { get; }

        public NullSeries(string name, int length)
        {
            Name = name;
            Length = length;
            ValidityMask = new ValidityMask(length);
            for (int i = 0; i < length; i++) ValidityMask.SetNull(i);
        }

        public void CopyTo(ISeries target, int offset)
        {
            if (target is NullSeries other)
            {
                // Nothing to copy, already null
            }
            else throw new InvalidOperationException("Type mismatch in CopyTo");
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is NullSeries other)
            {
                // Nothing to copy, already null
            }
            else throw new InvalidOperationException("Type mismatch in Take");
        }

        public object? Get(int i) => null;

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            if (target is NullSeries other)
            {
                other.ValidityMask.SetNull(targetIdx);
            }
            else throw new InvalidOperationException("Type mismatch in Take");
        }

        public IArrowArray ToArrowArray()
        {
            return new NullArray(Length);
        }

        public void Dispose() { }

        public ISeries CloneEmpty(int length)
        {
            return new NullSeries(Name, length);
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}
