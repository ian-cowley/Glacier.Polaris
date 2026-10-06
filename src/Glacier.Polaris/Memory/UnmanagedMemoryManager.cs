using System;
using System.Buffers;
using System.Threading;

namespace Glacier.Polaris.Memory
{
    /// <summary>
    /// A custom MemoryManager that wraps a raw unmanaged pointer and exposes it as standard Memory<T>.
    /// Used for zero-copy memory mapping.
    /// </summary>
    public sealed unsafe class UnmanagedMemoryManager<T> : MemoryManager<T> where T : unmanaged
    {
        private readonly T* _pointer;
        private readonly int _length;
        private int _disposed;

        public UnmanagedMemoryManager(T* pointer, int length)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            if (length > 0 && pointer == null)
                throw new ArgumentNullException(nameof(pointer));
            _pointer = pointer;
            _length = length;
        }

        public override Span<T> GetSpan()
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_length == 0 || _pointer == null)
                return Span<T>.Empty;
            return new Span<T>(_pointer, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_length == 0 && elementIndex == 0)
                return new MemoryHandle(null);
            ArgumentOutOfRangeException.ThrowIfNegative(elementIndex);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(elementIndex, _length);
            return new MemoryHandle(_pointer + elementIndex);
        }

        public override void Unpin() { }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected override void Dispose(bool disposing)
        {
            Interlocked.Exchange(ref _disposed, 1);
        }
    }
}
