using System;
using System.Collections.Generic;
using Apache.Arrow;

namespace Glacier.Polaris.Data
{
    // ─────────────────────────────────────────────────────────────────────────
    // ObjectSeries  – type-erased heterogeneous column (Polars `Object` dtype)
    // ─────────────────────────────────────────────────────────────────────────
    //
    // Stores arbitrary CLR objects. Useful for mixed-type prototype columns
    // or as an escape hatch when no concrete series type fits.
    //
    // Arrow serialisation: each element is ToString()'d and emitted as a
    // StringArray, because Arrow has no generic Object type. Round-tripping
    // through Arrow will yield a Utf8StringSeries on the way back.

    public sealed class ObjectSeries : ISeries
    {
        private readonly object?[] _data;

        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(object);
        public int Length { get; }
        public Memory.ValidityMask ValidityMask { get; }

        public ObjectSeries(string name, object?[] values)
        {
            Name = name;
            Length = values.Length;
            _data = values;
            ValidityMask = new Memory.ValidityMask(values.Length);
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] != null) ValidityMask.SetValid(i);
                else ValidityMask.SetNull(i);
            }
        }

        public ObjectSeries(string name, int length)
        {
            Name = name;
            Length = length;
            _data = new object?[length];
            ValidityMask = new Memory.ValidityMask(length);
            for (int i = 0; i < length; i++) ValidityMask.SetNull(i);
        }

        public object? GetValue(int i) => ValidityMask.IsNull(i) ? null : _data[i];

        public void SetValue(int i, object? value)
        {
            _data[i] = value;
            if (value != null) ValidityMask.SetValid(i);
            else ValidityMask.SetNull(i);
        }

        public object? Get(int i) => GetValue(i);

        public void CopyTo(ISeries target, int offset)
        {
            if (target is ObjectSeries t)
                for (int i = 0; i < Length; i++) t.SetValue(offset + i, _data[i]);
            else throw new InvalidOperationException("Type mismatch in CopyTo.");
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is ObjectSeries t)
                for (int i = 0; i < indices.Length; i++)
                    t.SetValue(i, indices[i] == -1 ? null : _data[indices[i]]);
            else throw new InvalidOperationException("Type mismatch in Take.");
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            if (target is ObjectSeries t) t.SetValue(targetIdx, srcIdx == -1 ? null : _data[srcIdx]);
            else throw new InvalidOperationException("Type mismatch in Take.");
        }

        public ISeries CloneEmpty(int length) => new ObjectSeries(Name, length);

        /// <summary>
        /// Serialise to Arrow as a StringArray (each value is ToString()'d).
        /// This is a lossy conversion: round-tripping yields Utf8StringSeries.
        /// </summary>
        public IArrowArray ToArrowArray()
        {
            var builder = new StringArray.Builder();
            for (int i = 0; i < Length; i++)
            {
                if (ValidityMask.IsNull(i)) builder.AppendNull();
                else builder.Append(_data[i]?.ToString() ?? string.Empty);
            }
            return builder.Build();
        }

        public void Dispose() { /* managed array */ }
    }
}
