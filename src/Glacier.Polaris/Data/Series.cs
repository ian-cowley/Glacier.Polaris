using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading;
using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;

namespace Glacier.Polaris.Data
{
    public abstract class Series<T> : ISeries where T : unmanaged
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(T);
        public int Length { get; }

        protected readonly System.Buffers.IMemoryOwner<T> _data;
        protected readonly ValidityMask _validityMask;
        private int _disposed;

        protected Series(string name, int length) : this(name, length, true, true) { }

        protected Series(string name, int length, bool clear, bool setAllValid)
        {
            Name = name;
            Length = length;
            _data = new MemoryOwnerColumn<T>(length, clear);
            _validityMask = new ValidityMask(length, setAllValid);
        }

        protected Series(string name, int length, System.Buffers.IMemoryOwner<T> data)
        {
            Name = name;
            Length = length;
            _data = data;
            _validityMask = new ValidityMask(length);
        }

        protected Series(string name, int length, System.Buffers.IMemoryOwner<T> data, ValidityMask validityMask)
        {
            Name = name;
            Length = length;
            _data = data;
            _validityMask = validityMask;
        }

        public Memory<T> Memory
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                return _data.Memory;
            }
        }

        public ValidityMask ValidityMask
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                return _validityMask;
            }
        }

        /// <summary>
        /// Returns a zero-copy ReadOnlyMemory view of the underlying unmanaged/managed column bytes.
        /// </summary>
        public unsafe ReadOnlyMemory<byte> AsBytesMemory()
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_data is NativeMemoryOwner<T> nativeOwner)
            {
                return nativeOwner.AsBytesMemory();
            }
            if (Length == 0) return ReadOnlyMemory<byte>.Empty;
            var handle = _data.Memory.Pin();
            return new UnmanagedMemoryManager<byte>((byte*)handle.Pointer, Length * sizeof(T)).Memory;
        }

        public T this[int i]
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                if ((uint)i >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(i));
                return Memory.Span[i];
            }
            set
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                if ((uint)i >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(i));
                Memory.Span[i] = value;
                _validityMask.SetValid(i);
            }
        }

        public void CopyTo(ISeries target, int offset)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (target is Series<T> other)
            {
                Memory.Span.CopyTo(other.Memory.Span.Slice(offset));
            }
            else
            {
                throw new InvalidOperationException($"Type mismatch in CopyTo: cannot copy {typeof(T).Name} to {target.DataType.Name}");
            }
        }
        public virtual void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (target is Series<T> other)
            {
                Compute.ComputeKernels.TakeWithNulls<T>(Memory.Span, indices, other.Memory.Span, other.ValidityMask);
                for (int i = 0; i < indices.Length; i++)
                {
                    if (indices[i] != -1 && this.ValidityMask.IsNull(indices[i]))
                    {
                        other.ValidityMask.SetNull(i);
                    }
                }
            }
            else
            {
                throw new InvalidOperationException($"Type mismatch in Take: cannot take into {target.DataType.Name}");
            }
        }
        public virtual object? Get(int i)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if ((uint)i >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(i));
            if (ValidityMask.IsNull(i)) return null;
            return Memory.Span[i];
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if ((uint)srcIdx >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(srcIdx));
            if ((uint)targetIdx >= (uint)target.Length) throw new ArgumentOutOfRangeException(nameof(targetIdx));
            if (target is Series<T> other)
            {
                other.Memory.Span[targetIdx] = Memory.Span[srcIdx];
                if (ValidityMask.IsNull(srcIdx)) other.ValidityMask.SetNull(targetIdx);
                else other.ValidityMask.SetValid(targetIdx);
            }
            else throw new InvalidOperationException($"Type mismatch in Take: cannot take into {target.DataType.Name}");
        }

        public virtual ArrowColumn ToArrowColumn()
        {
            var arrowType = typeof(T) switch
            {
                Type t when t == typeof(sbyte) => ArrowType.Int8,
                Type t when t == typeof(byte) => ArrowType.UInt8,
                Type t when t == typeof(short) => ArrowType.Int16,
                Type t when t == typeof(ushort) => ArrowType.UInt16,
                Type t when t == typeof(int) => ArrowType.Int32,
                Type t when t == typeof(uint) => ArrowType.UInt32,
                Type t when t == typeof(long) => ArrowType.Int64,
                Type t when t == typeof(ulong) => ArrowType.UInt64,
                Type t when t == typeof(float) => ArrowType.Float,
                Type t when t == typeof(double) => ArrowType.Double,
                Type t when t == typeof(bool) => ArrowType.Boolean,
                _ => ArrowType.Binary
            };
            var field = new ArrowField(Name, arrowType, isNullable: _validityMask.HasNulls);
            return new ArrowColumn(
                field,
                Length,
                _validityMask.NullCount,
                _validityMask.GetNullBitmapMemory(),
                ReadOnlyMemory<byte>.Empty,
                AsBytesMemory());
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _data.Dispose();
            }
        }
        public virtual ISeries CloneEmpty(int length)
        {
            return (ISeries)Activator.CreateInstance(this.GetType(), Name, length)!;
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }

    public sealed class Int32Series : Series<int>
    {
        public Int32Series(string name, int length) : base(name, length) { }
        public Int32Series(string name, int length, bool clear, bool setAllValid) : base(name, length, clear, setAllValid) { }
        public Int32Series(string name, int length, System.Buffers.IMemoryOwner<int> data) : base(name, length, data) { }
        public Int32Series(string name, int length, System.Buffers.IMemoryOwner<int> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }

        public static Int32Series FromMmf(string name, string filePath, int length)
        {
            var storage = new MmfMemoryOwnerColumn<int>(filePath, length);
            return new Int32Series(name, length, storage);
        }
        public Int32Series(string name, int[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }

        public static Int32Series FromValues(string name, int?[] values)
        {
            var series = new Int32Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        
    }

    public sealed class Int8Series : Series<sbyte>
    {
        public Int8Series(string name, int length) : base(name, length) { }
        public Int8Series(string name, int length, System.Buffers.IMemoryOwner<sbyte> data) : base(name, length, data) { }
        public Int8Series(string name, int length, System.Buffers.IMemoryOwner<sbyte> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public Int8Series(string name, sbyte[] data) : base(name, data.Length) { data.CopyTo(Memory); }

        public static Int8Series FromValues(string name, sbyte?[] values)
        {
            var series = new Int8Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }
    }

    public sealed class Int16Series : Series<short>
    {
        public Int16Series(string name, int length) : base(name, length) { }
        public Int16Series(string name, int length, System.Buffers.IMemoryOwner<short> data) : base(name, length, data) { }
        public Int16Series(string name, int length, System.Buffers.IMemoryOwner<short> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public Int16Series(string name, short[] data) : base(name, data.Length) { data.CopyTo(Memory); }

        public static Int16Series FromValues(string name, short?[] values)
        {
            var series = new Int16Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }
    }

    public sealed class Int64Series : Series<long>
    {
        public Int64Series(string name, int length) : base(name, length) { }
        public Int64Series(string name, int length, System.Buffers.IMemoryOwner<long> data) : base(name, length, data) { }
        public Int64Series(string name, int length, System.Buffers.IMemoryOwner<long> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }

        public static Int64Series FromValues(string name, long?[] values)
        {
            var series = new Int64Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        public static Int64Series FromMmf(string name, string filePath, int length)
        {
            var storage = new MmfMemoryOwnerColumn<long>(filePath, length);
            return new Int64Series(name, length, storage);
        }
        public Int64Series(string name, long[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }
    }

    public sealed class UInt8Series : Series<byte>
    {
        public UInt8Series(string name, int length) : base(name, length) { }
        public UInt8Series(string name, int length, System.Buffers.IMemoryOwner<byte> data) : base(name, length, data) { }
        public UInt8Series(string name, int length, System.Buffers.IMemoryOwner<byte> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public UInt8Series(string name, byte[] data) : base(name, data.Length) { data.CopyTo(Memory); }

        public static UInt8Series FromValues(string name, byte?[] values)
        {
            var series = new UInt8Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }
    }

    public sealed class UInt16Series : Series<ushort>
    {
        public UInt16Series(string name, int length) : base(name, length) { }
        public UInt16Series(string name, int length, System.Buffers.IMemoryOwner<ushort> data) : base(name, length, data) { }
        public UInt16Series(string name, int length, System.Buffers.IMemoryOwner<ushort> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public UInt16Series(string name, ushort[] data) : base(name, data.Length) { data.CopyTo(Memory); }

        public static UInt16Series FromValues(string name, ushort?[] values)
        {
            var series = new UInt16Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }
    }

    public sealed class UInt32Series : Series<uint>
    {
        public UInt32Series(string name, int length) : base(name, length) { }
        public UInt32Series(string name, int length, System.Buffers.IMemoryOwner<uint> data) : base(name, length, data) { }
        public UInt32Series(string name, int length, System.Buffers.IMemoryOwner<uint> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public UInt32Series(string name, uint[] data) : base(name, data.Length) { data.CopyTo(Memory); }

        public static UInt32Series FromValues(string name, uint?[] values)
        {
            var series = new UInt32Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }
    }

    public sealed class UInt64Series : Series<ulong>
    {
        public UInt64Series(string name, int length) : base(name, length) { }
        public UInt64Series(string name, int length, System.Buffers.IMemoryOwner<ulong> data) : base(name, length, data) { }
        public UInt64Series(string name, int length, System.Buffers.IMemoryOwner<ulong> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public UInt64Series(string name, ulong[] data) : base(name, data.Length) { data.CopyTo(Memory); }

        public static UInt64Series FromValues(string name, ulong?[] values)
        {
            var series = new UInt64Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }
    }

    public sealed class Float32Series : Series<float>
    {
        public Float32Series(string name, int length) : base(name, length) { }
        public Float32Series(string name, int length, System.Buffers.IMemoryOwner<float> data) : base(name, length, data) { }
        public Float32Series(string name, int length, System.Buffers.IMemoryOwner<float> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public Float32Series(string name, float[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }
        public Float32Series(string name, ReadOnlySpan<float> data) : base(name, data.Length)
        {
            data.CopyTo(Memory.Span);
        }

        public static Float32Series FromValues(string name, float?[] values)
        {
            var series = new Float32Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        public Float32Series Add(Float32Series other, Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            if (Length != other.Length) throw new ArgumentException("Series lengths must match.");
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorAdd(Memory.Span, other.Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Subtract(Float32Series other, Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            if (Length != other.Length) throw new ArgumentException("Series lengths must match.");
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorSub(Memory.Span, other.Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Multiply(Float32Series other, Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            if (Length != other.Length) throw new ArgumentException("Series lengths must match.");
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorMul(Memory.Span, other.Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Divide(Float32Series other, Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            if (Length != other.Length) throw new ArgumentException("Series lengths must match.");
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorDiv(Memory.Span, other.Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Fma(Float32Series mul, Float32Series add, Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            if (Length != mul.Length || Length != add.Length) throw new ArgumentException("Series lengths must match.");
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorFma(Memory.Span, mul.Memory.Span, add.Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Exp(Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorExp(Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Log(Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorLog(Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Sqrt(Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorSqrt(Memory.Span, res.Memory.Span, target);
            return res;
        }

        public Float32Series Sigmoid(Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            var res = new Float32Series(Name, Length);
            Compute.GpuPolarisAccelerator.VectorSigmoid(Memory.Span, res.Memory.Span, target);
            return res;
        }

        public float Sum(Compute.GpuTarget target = Compute.GpuTarget.Auto)
        {
            return Compute.GpuPolarisAccelerator.VectorSum(Memory.Span, target);
        }

        
    }

    public sealed class Float64Series : Series<double>
    {
        public Float64Series(string name, int length) : base(name, length) { }
        public Float64Series(string name, int length, bool clear, bool setAllValid) : base(name, length, clear, setAllValid) { }
        public Float64Series(string name, int length, System.Buffers.IMemoryOwner<double> data) : base(name, length, data) { }
        public Float64Series(string name, int length, System.Buffers.IMemoryOwner<double> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }

        public static Float64Series FromMmf(string name, string filePath, int length)
        {
            var storage = new MmfMemoryOwnerColumn<double>(filePath, length);
            return new Float64Series(name, length, storage);
        }
        public Float64Series(string name, double[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }

        public static Float64Series FromValues(string name, double?[] values)
        {
            var series = new Float64Series(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        
    }

    public sealed class BooleanSeries : Series<bool>
    {
        public BooleanSeries(string name, int length) : base(name, length) { }
        public BooleanSeries(string name, int length, System.Buffers.IMemoryOwner<bool> data) : base(name, length, data) { }
        public BooleanSeries(string name, int length, System.Buffers.IMemoryOwner<bool> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public BooleanSeries(string name, bool[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }

        public static BooleanSeries FromValues(string name, bool?[] values)
        {
            var series = new BooleanSeries(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        public override ArrowColumn ToArrowColumn()
        {
            int byteCount = (Length + 7) / 8;
            var packed = new NativeMemoryOwner<byte>(byteCount);
            var packedSpan = packed.Span;
            packedSpan.Clear();
            var boolSpan = Memory.Span;
            for (int i = 0; i < Length; i++)
            {
                if (boolSpan[i])
                {
                    packedSpan[i >> 3] |= (byte)(1 << (i & 7));
                }
            }
            var field = new ArrowField(Name, ArrowType.Boolean, isNullable: _validityMask.HasNulls);
            return new ArrowColumn(
                field,
                Length,
                _validityMask.NullCount,
                _validityMask.GetNullBitmapMemory(),
                ReadOnlyMemory<byte>.Empty,
                packed.AsBytesMemory());
        }
    }

    public sealed class DateSeries : Series<int>
    {
        public DateSeries(string name, int length) : base(name, length) { }
        public DateSeries(string name, int length, System.Buffers.IMemoryOwner<int> data) : base(name, length, data) { }
        public DateSeries(string name, int length, System.Buffers.IMemoryOwner<int> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }
        public DateSeries(string name, DateTime[] data) : base(name, data.Length)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var span = Memory.Span;
            for (int i = 0; i < data.Length; i++)
            {
                span[i] = (int)(data[i].Date - epoch).TotalDays;
            }
        }

        public static DateSeries FromValues(string name, int?[] values)
        {
            var series = new DateSeries(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        public override ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Date32, isNullable: _validityMask.HasNulls);
            return new ArrowColumn(
                field,
                Length,
                _validityMask.NullCount,
                _validityMask.GetNullBitmapMemory(),
                ReadOnlyMemory<byte>.Empty,
                AsBytesMemory());
        }
    }

    public sealed class DatetimeSeries : Series<long>
    {
        public DatetimeSeries(string name, int length) : base(name, length) { }
        public DatetimeSeries(string name, int length, System.Buffers.IMemoryOwner<long> data) : base(name, length, data) { }
        public DatetimeSeries(string name, int length, System.Buffers.IMemoryOwner<long> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }

        public static DatetimeSeries FromValues(string name, long?[] values)
        {
            var series = new DatetimeSeries(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        public override ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Timestamp, isNullable: _validityMask.HasNulls);
            return new ArrowColumn(
                field,
                Length,
                _validityMask.NullCount,
                _validityMask.GetNullBitmapMemory(),
                ReadOnlyMemory<byte>.Empty,
                AsBytesMemory());
        }
    }

    public sealed class DurationSeries : Series<long>
    {
        public DurationSeries(string name, int length) : base(name, length) { }
        public DurationSeries(string name, int length, System.Buffers.IMemoryOwner<long> data) : base(name, length, data) { }
        public DurationSeries(string name, int length, System.Buffers.IMemoryOwner<long> data, ValidityMask validityMask) : base(name, length, data, validityMask) { }

        public static DurationSeries FromValues(string name, long?[] values)
        {
            var series = new DurationSeries(name, values.Length);
            var span = series.Memory.Span;
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == null) series.ValidityMask.SetNull(i);
                else span[i] = values[i]!.Value;
            }
            return series;
        }

        public override ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Duration, isNullable: _validityMask.HasNulls);
            return new ArrowColumn(
                field,
                Length,
                _validityMask.NullCount,
                _validityMask.GetNullBitmapMemory(),
                ReadOnlyMemory<byte>.Empty,
                AsBytesMemory());
        }
    }

    public sealed class Utf8StringSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(string);
        public int Length { get; }

        private readonly System.Buffers.IMemoryOwner<byte> _dataBytes;
        private readonly System.Buffers.IMemoryOwner<int> _offsets;
        private readonly Glacier.Polaris.Memory.ValidityMask _validityMask;
        private int _disposed;

        public Glacier.Polaris.Memory.ValidityMask ValidityMask
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                return _validityMask;
            }
        }

        public Utf8StringSeries(string name, int length, System.Buffers.IMemoryOwner<int> offsets, System.Buffers.IMemoryOwner<byte> dataBytes, ValidityMask validityMask)
        {
            Name = name;
            Length = length;
            _offsets = offsets;
            _dataBytes = dataBytes;
            _validityMask = validityMask;
        }

        public Utf8StringSeries(string name, int length)
        {
            Name = name;
            Length = length;
            _offsets = new MemoryOwnerColumn<int>(length + 1);
            _dataBytes = new MemoryOwnerColumn<byte>(Math.Max(1024, length * 16));
            _validityMask = new ValidityMask(length);
        }

        public Utf8StringSeries(string name, int length, int totalBytes)
        {
            Name = name;
            Length = length;
            _offsets = new MemoryOwnerColumn<int>(length + 1);
            _dataBytes = new MemoryOwnerColumn<byte>(totalBytes);
            _validityMask = new ValidityMask(length);
        }

        public Utf8StringSeries(string name, string[] data)
        {
            Name = name;
            Length = data.Length;
            int totalBytes = 0;
            foreach (var s in data) totalBytes += System.Text.Encoding.UTF8.GetByteCount(s);
            _offsets = new MemoryOwnerColumn<int>(Length + 1);
            _dataBytes = new MemoryOwnerColumn<byte>(totalBytes);
            _validityMask = new ValidityMask(Length);
            var offsetSpan = _offsets.Memory.Span;
            var dataSpan = _dataBytes.Memory.Span;
            int currentOffset = 0;
            for (int i = 0; i < data.Length; i++)
            {
                offsetSpan[i] = currentOffset;
                var bytes = System.Text.Encoding.UTF8.GetBytes(data[i]);
                bytes.CopyTo(dataSpan.Slice(currentOffset));
                currentOffset += bytes.Length;
            }
            offsetSpan[data.Length] = currentOffset;
        }

        public Memory<byte> DataBytes
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                return _dataBytes.Memory;
            }
        }
        public Memory<int> Offsets
        {
            get
            {
                ObjectDisposedException.ThrowIf(_disposed != 0, this);
                return _offsets.Memory;
            }
        }

        public ReadOnlySpan<byte> GetStringSpan(int i)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if ((uint)i >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(i));
            var offsets = _offsets.Memory.Span;
            int start = offsets[i];
            int end = offsets[i + 1];
            if (end < start) throw new InvalidOperationException($"Corrupt string offsets: start={start}, end={end}");
            return _dataBytes.Memory.Span.Slice(start, end - start);
        }

        public void CopyTo(ISeries target, int offset)
        {
            throw new NotSupportedException("Utf8StringSeries.CopyTo is not supported. Use specialized merging logic.");
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (target is Utf8StringSeries other)
            {
                var targetData = other.DataBytes.Span;
                var targetOffsets = other.Offsets.Span;
                int currentOffset = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    targetOffsets[i] = currentOffset;
                    if (indices[i] == -1 || this.ValidityMask.IsNull(indices[i]))
                    {
                        other.ValidityMask.SetNull(i);
                        continue;
                    }
                    var srcSpan = GetStringSpan(indices[i]);
                    if (currentOffset + srcSpan.Length > targetData.Length)
                    {
                        throw new InvalidOperationException("Target buffer too small for Take.");
                    }
                    srcSpan.CopyTo(targetData.Slice(currentOffset));
                    currentOffset += srcSpan.Length;
                }
                targetOffsets[indices.Length] = currentOffset;
            }
            else throw new InvalidOperationException("Type mismatch in Take.");
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _offsets.Dispose();
                _dataBytes.Dispose();
            }
        }
        public object? Get(int i)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if ((uint)i >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(i));
            if (ValidityMask.IsNull(i)) return null;
            var span = GetStringSpan(i);
            return System.Text.Encoding.UTF8.GetString(span);
        }
        public string? GetString(int i)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if ((uint)i >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(i));
            if (ValidityMask.IsNull(i)) return null;
            var span = GetStringSpan(i);
            return System.Text.Encoding.UTF8.GetString(span);
        }

        public static Utf8StringSeries FromStrings(string name, string?[] data)
        {
            int totalBytes = 0;
            foreach (var s in data) if (s != null) totalBytes += System.Text.Encoding.UTF8.GetByteCount(s);
            var series = new Utf8StringSeries(name, data.Length, totalBytes);
            var offsetSpan = series._offsets.Memory.Span;
            var dataSpan = series._dataBytes.Memory.Span;
            int currentOffset = 0;
            for (int i = 0; i < data.Length; i++)
            {
                offsetSpan[i] = currentOffset;
                if (data[i] == null) { series._validityMask.SetNull(i); }
                else
                {
                    var bytes = System.Text.Encoding.UTF8.GetBytes(data[i]!);
                    bytes.CopyTo(dataSpan.Slice(currentOffset));
                    currentOffset += bytes.Length;
                }
            }
            offsetSpan[data.Length] = currentOffset;
            return series;
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if ((uint)srcIdx >= (uint)Length) throw new ArgumentOutOfRangeException(nameof(srcIdx));
            if ((uint)targetIdx >= (uint)target.Length) throw new ArgumentOutOfRangeException(nameof(targetIdx));
            if (target is Utf8StringSeries other)
            {
                var targetOffsets = other.Offsets.Span;
                var targetData = other.DataBytes.Span;
                int dataPos = targetOffsets[other.Length];
                if (ValidityMask.IsNull(srcIdx))
                {
                    other.ValidityMask.SetNull(targetIdx);
                    targetOffsets[targetIdx] = dataPos;
                    return;
                }
                var srcSpan = GetStringSpan(srcIdx);
                if (dataPos + srcSpan.Length > targetData.Length)
                {
                    throw new InvalidOperationException("Target buffer too small for Take.");
                }
                targetOffsets[targetIdx] = dataPos;
                srcSpan.CopyTo(targetData.Slice(dataPos));
                dataPos += srcSpan.Length;
                targetOffsets[other.Length] = dataPos;
                other.ValidityMask.SetValid(targetIdx);
            }
            else throw new InvalidOperationException("Type mismatch in Take.");
        }

        public ArrowColumn ToArrowColumn()
        {
            var field = new ArrowField(Name, ArrowType.Utf8, isNullable: _validityMask.HasNulls);
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

        public ISeries CloneEmpty(int length)
        {
            return new Utf8StringSeries(Name, length, Math.Max(1024, length * 16));
        }
        public DataFrame ValueCounts(bool sort = false, bool parallel = true) => Compute.UniqueKernels.ValueCounts(this, sort, parallel);
        public ISeries IsFirst() => Compute.UniqueKernels.IsFirst(this);
        public double Entropy() => Compute.AggregationKernels.Entropy(this);
        public int ApproxNUnique() => Compute.UniqueKernels.ApproxNUnique(this);
        public ISeries MapElements(Func<object?, object?> mapping, Type returnType) => Compute.ComputeKernels.MapElements(this, mapping, returnType);
    }
}