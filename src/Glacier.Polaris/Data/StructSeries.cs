using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Memory;
using Glacier.Formats.Arrow;

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
            if (target is not StructSeries other)
                throw new InvalidOperationException("Type mismatch in CopyTo");

            if (Fields.Length != other.Fields.Length)
                throw new ArgumentException($"Struct field count mismatch in CopyTo: source has {Fields.Length}, target has {other.Fields.Length}.", nameof(target));

            ArgumentOutOfRangeException.ThrowIfNegative(offset);
            if (offset > other.Length - Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            // Upfront atomicity check across ALL fields before mutating any field or ValidityMask.
            for (int i = 0; i < Fields.Length; i++)
            {
                if (!ListSeries.AreValuesCompatible(Fields[i], other.Fields[i]))
                    throw new InvalidOperationException($"Struct field {i} type mismatch in CopyTo: expected {Fields[i].GetType().Name}, got {other.Fields[i].GetType().Name}.");
            }

            if (Length == 0 || ReferenceEquals(this, other))
                return;

            ISeries?[]? pendingReplacements = null;
            try
            {
                bool allowInPlace = Fields.Length == 1;
                for (int i = 0; i < Fields.Length; i++)
                {
                    if (Fields[i] is Utf8StringSeries srcUtf8 && other.Fields[i] is Utf8StringSeries dstUtf8)
                    {
                        int oldEnd = checked(offset + Length);
                        int suffixCount = dstUtf8.Length - oldEnd;
                        var replacement = ListSeries.SpliceUtf8Series(
                            srcUtf8,
                            dstUtf8,
                            offset,
                            oldEnd,
                            suffixCount,
                            0,
                            Length,
                            null,
                            dstUtf8.Length,
                            allowInPlace);
                        if (replacement != null)
                        {
                            pendingReplacements ??= new ISeries?[Fields.Length];
                            pendingReplacements[i] = replacement;
                        }
                    }
                    else if (Fields[i] is BinarySeries srcBin && other.Fields[i] is BinarySeries dstBin)
                    {
                        int oldEnd = checked(offset + Length);
                        int suffixCount = dstBin.Length - oldEnd;
                        var replacement = ListSeries.SpliceBinarySeries(
                            srcBin,
                            dstBin,
                            offset,
                            oldEnd,
                            suffixCount,
                            0,
                            Length,
                            null,
                            dstBin.Length,
                            allowInPlace);
                        if (replacement != null)
                        {
                            pendingReplacements ??= new ISeries?[Fields.Length];
                            pendingReplacements[i] = replacement;
                        }
                    }
                }

                for (int i = 0; i < Fields.Length; i++)
                {
                    if (Fields[i] is not Utf8StringSeries and not BinarySeries)
                    {
                        Fields[i].CopyTo(other.Fields[i], offset);
                    }
                }
            }
            catch
            {
                if (pendingReplacements != null)
                {
                    for (int i = 0; i < pendingReplacements.Length; i++)
                        pendingReplacements[i]?.Dispose();
                }
                throw;
            }

            if (pendingReplacements != null)
            {
                for (int i = 0; i < pendingReplacements.Length; i++)
                {
                    var replacement = pendingReplacements[i];
                    if (replacement != null)
                    {
                        ISeries oldField = other.Fields[i];
                        other.Fields[i] = replacement;
                        oldField.Dispose();
                    }
                }
            }

            ValidityMask.CopyRangeTo(other.ValidityMask, offset);
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is StructSeries other)
            {
                for (int i = 0; i < Fields.Length; i++)
                {
                    if (Fields[i] is Utf8StringSeries srcUtf8 && other.Fields[i] is Utf8StringSeries dstUtf8)
                    {
                        var replacement = ListSeries.TakeUtf8Into(srcUtf8, dstUtf8, indices);
                        if (replacement != null)
                        {
                            other.Fields[i] = replacement;
                            dstUtf8.Dispose();
                        }
                    }
                    else if (Fields[i] is BinarySeries srcBin && other.Fields[i] is BinarySeries dstBin)
                    {
                        var replacement = ListSeries.TakeBinaryInto(srcBin, dstBin, indices);
                        if (replacement != null)
                        {
                            other.Fields[i] = replacement;
                            dstBin.Dispose();
                        }
                    }
                    else
                    {
                        Fields[i].Take(other.Fields[i], indices);
                    }
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
