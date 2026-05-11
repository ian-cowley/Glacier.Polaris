using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Memory;

namespace Glacier.Polaris.Data
{
    public sealed class StructSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(IDictionary<string, object>);
        public int Length { get; }

        public ISeries[] Fields { get; }
        public ValidityMask ValidityMask { get; }

        public StructSeries(string name, ISeries[] fields)
        {
            Name = name;
            Fields = fields;
            Length = fields.Length > 0 ? fields[0].Length : 0;
            ValidityMask = new ValidityMask(Length);

            foreach (var field in fields)
            {
                if (field.Length != Length)
                    throw new ArgumentException($"Struct field '{field.Name}' has length {field.Length}, but expected {Length}");
            }
        }

        public void CopyTo(ISeries target, int offset)
        {
            if (target is StructSeries other)
            {
                for (int i = 0; i < Fields.Length; i++)
                {
                    Fields[i].CopyTo(other.Fields[i], offset);
                }
                ValidityMask.CopyTo(other.ValidityMask, offset);
            }
            else throw new InvalidOperationException("Type mismatch in CopyTo");
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is StructSeries other)
            {
                for (int i = 0; i < Fields.Length; i++)
                {
                    Fields[i].Take(other.Fields[i], indices);
                }
                ValidityMask.Take(other.ValidityMask, indices);
            }
            else throw new InvalidOperationException("Type mismatch in Take");
        }

        public object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            var result = new Dictionary<string, object?>();
            foreach (var field in Fields)
            {
                result[field.Name] = field.Get(i);
            }
            return result;
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            if (target is StructSeries other)
            {
                for (int i = 0; i < Fields.Length; i++)
                {
                    Fields[i].Take(other.Fields[i], srcIdx, targetIdx);
                }
                if (ValidityMask.IsNull(srcIdx)) other.ValidityMask.SetNull(targetIdx);
                else other.ValidityMask.SetValid(targetIdx);
            }
            else throw new InvalidOperationException("Type mismatch in Take");
        }

        public Apache.Arrow.IArrowArray ToArrowArray()
        {
            var fields = new System.Collections.Generic.List<Apache.Arrow.Field>();
            var arrays = new System.Collections.Generic.List<Apache.Arrow.IArrowArray>();

            foreach (var field in Fields)
            {
                var arrowArray = field.ToArrowArray();
                fields.Add(new Apache.Arrow.Field(field.Name, arrowArray.Data.DataType, field.ValidityMask.HasNulls));
                arrays.Add(arrowArray);
            }

            var structType = new Apache.Arrow.Types.StructType(fields);

            // Build null bitmap from ValidityMask
            var nullBitmapBuilder = new Apache.Arrow.ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++)
            {
                nullBitmapBuilder.Append(ValidityMask.IsValid(i));
            }

            return new Apache.Arrow.StructArray(structType, Length, arrays, nullBitmapBuilder.Build());
        }

        public void Dispose()
        {
            foreach (var field in Fields) field.Dispose();
        }

        public ISeries CloneEmpty(int length)
        {
            var emptyFields = Fields.Select(f => f.CloneEmpty(length)).ToArray();
            return new StructSeries(Name, emptyFields);
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}
