using Glacier.Polaris.Memory;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Glacier.Polaris.Data
{
    public abstract class Series<T> : ISeries where T : unmanaged
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(T);
        public int Length { get; }

        protected readonly MemoryOwnerColumn<T> _data;
        protected readonly ValidityMask _validityMask;

        protected Series(string name, int length)
        {
            Name = name;
            Length = length;
            _data = new MemoryOwnerColumn<T>(length);
            _validityMask = new ValidityMask(length);
        }

        public Memory<T> Memory => _data.Memory;
        public ValidityMask ValidityMask => _validityMask;

        public T this[int i]
        {
            get => Memory.Span[i];
            set { Memory.Span[i] = value; _validityMask.SetValid(i); }
        }

        public void CopyTo(ISeries target, int offset)
        {
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
            if (target is Series<T> other)
            {
                Compute.ComputeKernels.TakeWithNulls<T>(Memory.Span, indices, other.Memory.Span, other.ValidityMask);
                // Also propagate existing nulls from source if index is valid
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
            if (ValidityMask.IsNull(i)) return null;
            return Memory.Span[i];
        }

        public void Take(ISeries target, int srcIdx, int targetIdx)
        {
            if (target is Series<T> other)
            {
                other.Memory.Span[targetIdx] = Memory.Span[srcIdx];
                if (ValidityMask.IsNull(srcIdx)) other.ValidityMask.SetNull(targetIdx);
                else other.ValidityMask.SetValid(targetIdx);
            }
            else throw new InvalidOperationException($"Type mismatch in Take: cannot take into {target.DataType.Name}");
        }

        public virtual Apache.Arrow.IArrowArray ToArrowArray()
        {
            throw new NotSupportedException($"ToArrowArray not implemented for {typeof(T).Name}");
        }

        public void Dispose() => _data.Dispose();
        public virtual ISeries CloneEmpty(int length)
        {
            return (ISeries)Activator.CreateInstance(this.GetType(), Name, length)!;
        }
    }

    public sealed class Int32Series : Series<int>
    {
        public Int32Series(string name, int length) : base(name, length) { }
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

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));
            
            return new Int32Array(
                new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(Memory.Span).ToArray()),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }

    public sealed class Int8Series : Series<sbyte>
    {
        public Int8Series(string name, int length) : base(name, length) { }
    }

    public sealed class Int16Series : Series<short>
    {
        public Int16Series(string name, int length) : base(name, length) { }
    }

    public sealed class Int64Series : Series<long>
    {
        public Int64Series(string name, int length) : base(name, length) { }
        public Int64Series(string name, long[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }
    }

    public sealed class UInt8Series : Series<byte>
    {
        public UInt8Series(string name, int length) : base(name, length) { }
    }

    public sealed class UInt16Series : Series<ushort>
    {
        public UInt16Series(string name, int length) : base(name, length) { }
    }

    public sealed class UInt32Series : Series<uint>
    {
        public UInt32Series(string name, int length) : base(name, length) { }
    }

    public sealed class UInt64Series : Series<ulong>
    {
        public UInt64Series(string name, int length) : base(name, length) { }
    }

    public sealed class Float32Series : Series<float>
    {
        public Float32Series(string name, int length) : base(name, length) { }
    }

    public sealed class Float64Series : Series<double>
    {
        public Float64Series(string name, int length) : base(name, length) { }
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

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));

            return new DoubleArray(
                new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(Memory.Span).ToArray()),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }

    public sealed class BooleanSeries : Series<bool>
    {
        public BooleanSeries(string name, int length) : base(name, length) { }
        public BooleanSeries(string name, bool[] data) : base(name, data.Length)
        {
            data.CopyTo(Memory);
        }

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));

            var valueBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) valueBitmapBuilder.Append(Memory.Span[i]);

            return new BooleanArray(
                valueBitmapBuilder.Build(),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }

    public sealed class DateSeries : Series<int>
    {
        public DateSeries(string name, int length) : base(name, length) { }
        public DateSeries(string name, DateTime[] data) : base(name, data.Length)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var span = Memory.Span;
            for (int i = 0; i < data.Length; i++)
            {
                span[i] = (int)(data[i].Date - epoch).TotalDays;
            }
        }

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));

            return new Date32Array(
                new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(Memory.Span).ToArray()),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }

    public sealed class DatetimeSeries : Series<long>
    {
        public DatetimeSeries(string name, int length) : base(name, length) { }
        // Logical type: Datetime (microseconds since epoch)

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));

            return new TimestampArray(
                new TimestampType(TimeUnit.Nanosecond, (string?)null),
                new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(Memory.Span).ToArray()),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }

    public sealed class DurationSeries : Series<long>
    {
        public DurationSeries(string name, int length) : base(name, length) { }

        public override IArrowArray ToArrowArray()
        {
            var nullBitmapBuilder = new ArrowBuffer.BitmapBuilder(Length);
            for (int i = 0; i < Length; i++) nullBitmapBuilder.Append(ValidityMask.IsValid(i));

            return new DurationArray(
                DurationType.Nanosecond,
                new ArrowBuffer(System.Runtime.InteropServices.MemoryMarshal.AsBytes(Memory.Span).ToArray()),
                nullBitmapBuilder.Build(),
                Length,
                ValidityMask.NullCount,
                0);
        }
    }

    /// <summary>
    /// For Utf8StringSeries, instead of an array of objects/strings, 
    /// a high-performance engine uses a single large byte array and an offsets array.
    /// This implementation is a skeleton demonstrating the zero-allocation approach.
    /// </summary>
    public sealed class Utf8StringSeries : ISeries
    {
        public string Name { get; private set; }
        public void Rename(string name) => Name = name;
        public Type DataType => typeof(string);
        public int Length { get; }

        private readonly MemoryOwnerColumn<byte> _dataBytes;
        private readonly MemoryOwnerColumn<int> _offsets;
        private readonly Glacier.Polaris.Memory.ValidityMask _validityMask;

        public Glacier.Polaris.Memory.ValidityMask ValidityMask => _validityMask;

        public Utf8StringSeries(string name, int length)
        {
            Name = name;
            Length = length;

            // Default: allocate for average 16 bytes per string
            _offsets = new MemoryOwnerColumn<int>(length + 1);
            _dataBytes = new MemoryOwnerColumn<byte>(Math.Max(1024, length * 16));

            _validityMask = new ValidityMask(length);
        }

        public Utf8StringSeries(string name, int length, int totalBytes)
        {
            Name = name;
            Length = length;

            // Offsets array contains starting index of each string
            _offsets = new MemoryOwnerColumn<int>(length + 1);
            // Flat byte array containing all UTF8 characters
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

        public Memory<byte> DataBytes => _dataBytes.Memory;
        public Memory<int> Offsets => _offsets.Memory;

        public ReadOnlySpan<byte> GetStringSpan(int i)
        {
            var offsets = _offsets.Memory.Span;
            return _dataBytes.Memory.Span.Slice(offsets[i], offsets[i + 1] - offsets[i]);
        }

        public void CopyTo(ISeries target, int offset)
        {
            throw new NotSupportedException("Utf8StringSeries.CopyTo is not supported. Use specialized merging logic.");
        }

        public void Take(ISeries target, ReadOnlySpan<int> indices)
        {
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
                        // This should not happen if the engine pre-calculates, but let's be safe.
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
            _offsets.Dispose();
            _dataBytes.Dispose();
        }
        public object? Get(int i)
        {
            if (ValidityMask.IsNull(i)) return null;
            var span = GetStringSpan(i);
            return System.Text.Encoding.UTF8.GetString(span);
        }
        public string? GetString(int i)
        {
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
            if (target is Utf8StringSeries other)
            {
                var targetOffsets = other.Offsets.Span;
                var targetData = other.DataBytes.Span;
                // Use the final offset slot (other.Length) as a running data position accumulator
                int dataPos = targetOffsets[other.Length];
                if (ValidityMask.IsNull(srcIdx))
                {
                    other.ValidityMask.SetNull(targetIdx);
                    targetOffsets[targetIdx] = dataPos;
                    // Don't advance dataPos for null entries
                    return;
                }
                var srcSpan = GetStringSpan(srcIdx);
                if (dataPos + srcSpan.Length > targetData.Length)
                {
                    throw new InvalidOperationException("Target buffer too small for Take.");
                }
                // Store the start position in offsets[targetIdx]
                targetOffsets[targetIdx] = dataPos;
                srcSpan.CopyTo(targetData.Slice(dataPos));
                dataPos += srcSpan.Length;
                // Update running data position accumulator
                targetOffsets[other.Length] = dataPos;
                other.ValidityMask.SetValid(targetIdx);
            }
            else throw new InvalidOperationException("Type mismatch in Take.");
        }


        public Apache.Arrow.IArrowArray ToArrowArray()
        {
            var builder = new Apache.Arrow.StringArray.Builder();
            for (int i = 0; i < Length; i++)
            {
                if (ValidityMask.IsNull(i)) builder.AppendNull();
                else builder.Append(GetString(i));
            }
            return builder.Build();
        }

        public ISeries CloneEmpty(int length)
        {
            return new Utf8StringSeries(Name, length, Math.Max(1024, length * 16));
        }
    }
}
