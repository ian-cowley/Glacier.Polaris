using System;
using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;
using System.Runtime.InteropServices;

namespace Glacier.Polaris.Data
{
    public sealed class BinarySeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(byte[]);
        public int Length { get; }

        private readonly System.Buffers.IMemoryOwner<byte> _dataBytes;
        private readonly System.Buffers.IMemoryOwner<int> _offsets;
        private readonly ValidityMask _validityMask;

        public ValidityMask ValidityMask => _validityMask;

        public BinarySeries(string name, int length, System.Buffers.IMemoryOwner<int> offsets, System.Buffers.IMemoryOwner<byte> dataBytes, ValidityMask validityMask)
        {
            Name = name;
            Length = length;
            _offsets = offsets;
            _dataBytes = dataBytes;
            _validityMask = validityMask;
        }

        public BinarySeries(string name, int length, int totalBytes)
        {
            Name = name;
            Length = length;
            _offsets = new MemoryOwnerColumn<int>(length + 1);
            _dataBytes = new MemoryOwnerColumn<byte>(totalBytes);
            _validityMask = new ValidityMask(length);
        }

        public BinarySeries(string name, byte[]?[] data)
        {
            Name = name;
            Length = data.Length;
            int totalBytes = data.Sum(b => b?.Length ?? 0);
            _offsets = new MemoryOwnerColumn<int>(Length + 1);
            _dataBytes = new MemoryOwnerColumn<byte>(totalBytes);
            _validityMask = new ValidityMask(Length);

            var offsets = _offsets.Memory.Span;
            var bytes = _dataBytes.Memory.Span;
            int currentOffset = 0;
            for (int i = 0; i < data.Length; i++)
            {
                offsets[i] = currentOffset;
                if (data[i] == null)
                {
                    _validityMask.SetNull(i);
                }
                else
                {
                    data[i]!.CopyTo(bytes.Slice(currentOffset));
                    currentOffset += data[i]!.Length;
                    _validityMask.SetValid(i);
                }
            }
            offsets[data.Length] = currentOffset;
        }

        public Memory<byte> DataBytes => _dataBytes.Memory;
        public Memory<int> Offsets => _offsets.Memory;

        public ReadOnlySpan<byte> GetSpan(int i)
        {
            var offsets = _offsets.Memory.Span;
            return _dataBytes.Memory.Span.Slice(offsets[i], offsets[i + 1] - offsets[i]);
        }

        public void CopyTo(ISeries target, int offset)
        {
            throw new NotSupportedException("BinarySeries.CopyTo is not supported.");
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            if (target is BinarySeries other)
            {
                var targetData = other.DataBytes.Span;
                var targetOffsets = other.Offsets.Span;
                int currentOffset = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    targetOffsets[i] = currentOffset;
                    if (ValidityMask.IsNull(indices[i]))
                    {
                        other.ValidityMask.SetNull(i);
                    }
                    else
                    {
                        var srcSpan = GetSpan(indices[i]);
                        srcSpan.CopyTo(targetData.Slice(currentOffset));
                        currentOffset += srcSpan.Length;
                        other.ValidityMask.SetValid(i);
                    }
                }
                targetOffsets[indices.Length] = currentOffset;
            }
            else throw new InvalidOperationException("Type mismatch in Take.");
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            throw new NotSupportedException("BinarySeries.Take(int,int) is not supported.");
        }

        public object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            return GetSpan(i).ToArray();
        }

        public byte[]? GetValue(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            return GetSpan(i).ToArray();
        }

        public ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Binary, isNullable: _validityMask.HasNulls);
            ReadOnlyMemory<byte> offsetsMem;
            if (_offsets is NativeMemoryOwner<int> nativeOffsets)
                offsetsMem = nativeOffsets.AsBytesMemory();
            else
                offsetsMem = MemoryMarshal.AsBytes(_offsets.Memory.Span[..(Length + 1)]).ToArray();

            int totalBytes = Length > 0 ? _offsets.Memory.Span[Length] : 0;
            ReadOnlyMemory<byte> dataMem;
            if (_dataBytes is NativeMemoryOwner<byte> nativeData)
                dataMem = nativeData.AsBytesMemory().Slice(0, totalBytes);
            else
                dataMem = _dataBytes.Memory.Slice(0, totalBytes);

            return new ArrowColumn(
                field,
                Length,
                _validityMask.NullCount,
                _validityMask.GetNullBitmapMemory(),
                offsetsMem,
                dataMem);
        }

        public void Dispose()
        {
            _offsets.Dispose();
            _dataBytes.Dispose();
        }

        public ISeries CloneEmpty(int length)
        {
            return new BinarySeries(Name, length, Math.Max(1024, length * 16));
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}
