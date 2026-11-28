using System;
using System.Buffers;
using System.Threading;

namespace Glacier.Polaris.Memory
{
    internal interface IRefCountedMemoryLifetime
    {
        bool IsDisposed { get; }
        bool TryRetain();
        void Release();
    }

    /// <summary>
    /// A custom MemoryManager that wraps a raw unmanaged pointer and exposes it as standard Memory&lt;T&gt;.
    /// Used for zero-copy memory mapping.
    /// </summary>
    public sealed unsafe class UnmanagedMemoryManager<T> : MemoryManager<T> where T : unmanaged
    {
        private readonly T* _pointer;
        private readonly int _length;
        private readonly IRefCountedMemoryLifetime? _lifetime;
        private int _disposed;
        private int _pinCount;

        public UnmanagedMemoryManager(T* pointer, int length)
            : this(pointer, length, null)
        {
        }

        internal UnmanagedMemoryManager(T* pointer, int length, IRefCountedMemoryLifetime? lifetime)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            if (length > 0 && pointer == null)
                throw new ArgumentNullException(nameof(pointer));
            _pointer = pointer;
            _length = length;
            _lifetime = lifetime;
        }

        private bool IsEffectivelyDisposed =>
            Volatile.Read(ref _disposed) != 0 || (_lifetime?.IsDisposed ?? false);

        public override Span<T> GetSpan()
        {
            ObjectDisposedException.ThrowIf(IsEffectivelyDisposed, this);
            if (_length == 0 || _pointer == null)
                return Span<T>.Empty;
            return new Span<T>(_pointer, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            ObjectDisposedException.ThrowIf(IsEffectivelyDisposed, this);
            if (_length == 0 && elementIndex == 0)
                return new MemoryHandle(null);
            ArgumentOutOfRangeException.ThrowIfNegative(elementIndex);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(elementIndex, _length);

            if (_lifetime != null && !_lifetime.TryRetain())
                throw new ObjectDisposedException(nameof(UnmanagedMemoryManager<T>));

            Interlocked.Increment(ref _pinCount);
            if (Volatile.Read(ref _disposed) != 0)
            {
                Unpin();
                throw new ObjectDisposedException(nameof(UnmanagedMemoryManager<T>));
            }

            return new MemoryHandle(_pointer + elementIndex, pinnable: this);
        }

        public override void Unpin()
        {
            while (true)
            {
                int current = Volatile.Read(ref _pinCount);
                if (current <= 0)
                    return;
                if (Interlocked.CompareExchange(ref _pinCount, current - 1, current) == current)
                {
                    _lifetime?.Release();
                    return;
                }
            }
        }

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
