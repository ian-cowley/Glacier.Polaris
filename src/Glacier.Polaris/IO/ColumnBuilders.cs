using System;
using System.Buffers;
using System.Buffers.Text;
using System.Text;

namespace Glacier.Polaris.IO
{
    public interface IColumnBuilder : IDisposable
    {
        string Name { get; }
        void AddToken(ReadOnlySpan<byte> token);
        ISeries Build();
    }

    public class Int32ColumnBuilder : IColumnBuilder
    {
        public string Name { get; }
        private int[] _buffer;
        private int _count;

        public Int32ColumnBuilder(string name, int initialCapacity = 65536)
        {
            Name = name;
            _buffer = ArrayPool<int>.Shared.Rent(initialCapacity);
        }

        public void AddToken(ReadOnlySpan<byte> token)
        {
            if (_count >= _buffer.Length)
            {
                var newBuffer = ArrayPool<int>.Shared.Rent(_buffer.Length * 2);
                Array.Copy(_buffer, newBuffer, _count);
                ArrayPool<int>.Shared.Return(_buffer);
                _buffer = newBuffer;
            }

            if (Utf8Parser.TryParse(token, out int value, out _))
            {
                _buffer[_count++] = value;
            }
            else
            {
                _buffer[_count++] = 0;
            }
        }

        public ISeries Build()
        {
            var series = new Data.Int32Series(Name, _count);
            new ReadOnlySpan<int>(_buffer, 0, _count).CopyTo(series.Memory.Span);
            return series;
        }

        public void Dispose()
        {
            if (_buffer != null)
            {
                ArrayPool<int>.Shared.Return(_buffer);
                _buffer = null!;
            }
        }
    }

    public class Float64ColumnBuilder : IColumnBuilder
    {
        public string Name { get; }
        private double[] _buffer;
        private int _count;

        public Float64ColumnBuilder(string name, int initialCapacity = 65536)
        {
            Name = name;
            _buffer = ArrayPool<double>.Shared.Rent(initialCapacity);
        }

        public void AddToken(ReadOnlySpan<byte> token)
        {
            if (_count >= _buffer.Length)
            {
                var newBuffer = ArrayPool<double>.Shared.Rent(_buffer.Length * 2);
                Array.Copy(_buffer, newBuffer, _count);
                ArrayPool<double>.Shared.Return(_buffer);
                _buffer = newBuffer;
            }

            if (Utf8Parser.TryParse(token, out double value, out _))
            {
                _buffer[_count++] = value;
            }
            else
            {
                _buffer[_count++] = double.NaN;
            }
        }

        public ISeries Build()
        {
            var series = new Data.Float64Series(Name, _count);
            new ReadOnlySpan<double>(_buffer, 0, _count).CopyTo(series.Memory.Span);
            return series;
        }

        public void Dispose()
        {
            if (_buffer != null)
            {
                ArrayPool<double>.Shared.Return(_buffer);
                _buffer = null!;
            }
        }
    }

    public class Utf8StringColumnBuilder : IColumnBuilder
    {
        public string Name { get; }
        private byte[] _dataBuffer;
        private int[] _offsetsBuffer;
        private int _dataCount;
        private int _rowCount;

        public Utf8StringColumnBuilder(string name, int initialRowCapacity = 65536)
        {
            Name = name;
            _dataBuffer = ArrayPool<byte>.Shared.Rent(initialRowCapacity * 16);
            _offsetsBuffer = ArrayPool<int>.Shared.Rent(initialRowCapacity + 1);
            _offsetsBuffer[0] = 0;
        }

        public void AddToken(ReadOnlySpan<byte> token)
        {
            if (_rowCount + 1 >= _offsetsBuffer.Length)
            {
                var newOffsets = ArrayPool<int>.Shared.Rent(_offsetsBuffer.Length * 2);
                Array.Copy(_offsetsBuffer, newOffsets, _rowCount + 1);
                ArrayPool<int>.Shared.Return(_offsetsBuffer);
                _offsetsBuffer = newOffsets;
            }

            if (_dataCount + token.Length > _dataBuffer.Length)
            {
                int newSize = Math.Max(_dataBuffer.Length * 2, _dataCount + token.Length + 1024);
                var newData = ArrayPool<byte>.Shared.Rent(newSize);
                Array.Copy(_dataBuffer, newData, _dataCount);
                ArrayPool<byte>.Shared.Return(_dataBuffer);
                _dataBuffer = newData;
            }

            token.CopyTo(_dataBuffer.AsSpan(_dataCount));
            _dataCount += token.Length;
            _rowCount++;
            _offsetsBuffer[_rowCount] = _dataCount;
        }

        public ISeries Build()
        {
            var series = new Data.Utf8StringSeries(Name, _rowCount, _dataCount);
            
            new ReadOnlySpan<byte>(_dataBuffer, 0, _dataCount).CopyTo(series.DataBytes.Span);
            new ReadOnlySpan<int>(_offsetsBuffer, 0, _rowCount + 1).CopyTo(series.Offsets.Span);

            return series;
        }

        public void Dispose()
        {
            if (_dataBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(_dataBuffer);
                _dataBuffer = null!;
            }
            if (_offsetsBuffer != null)
            {
                ArrayPool<int>.Shared.Return(_offsetsBuffer);
                _offsetsBuffer = null!;
            }
        }
    }
}
