using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;

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

        public ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Binary, isNullable: ValidityMask.HasNulls);
            return new ArrowColumn(field, Length, ValidityMask.NullCount, ValidityMask.GetNullBitmapMemory(), ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);
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
