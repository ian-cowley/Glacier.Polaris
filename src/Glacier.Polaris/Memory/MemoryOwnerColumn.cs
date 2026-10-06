using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Glacier.Polaris.Memory
{
    /// <summary>
    /// Wrapper for renting and deterministically disposing column memory backing.
    /// Strictly follows the Owner/Consumer lifetime model.
    /// </summary>
    public sealed class MemoryOwnerColumn<T> : IMemoryOwner<T>
    {
        private T[]? _rentedArray;
        private readonly int _length;

        public MemoryOwnerColumn(int length) : this(length, true) { }

        public MemoryOwnerColumn(int length, bool clear)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            _length = length;
            // Rent from ArrayPool
            _rentedArray = ArrayPool<T>.Shared.Rent(length);
            if (clear)
            {
                // Clear the active slice to prevent garbage/data leakage
                Array.Clear(_rentedArray, 0, length);
            }
        }

        public Memory<T> Memory
        {
            get
            {
                ObjectDisposedException.ThrowIf(_rentedArray == null, this);
                return new Memory<T>(_rentedArray, 0, _length);
            }
        }

        public void Dispose()
        {
            var arr = System.Threading.Interlocked.Exchange(ref _rentedArray, null);
            if (arr != null)
            {
                ArrayPool<T>.Shared.Return(arr);
            }
        }
    }

    /// <summary>
    /// Example of .NET 8+ [InlineArray] for fixed-size zero-allocation state tracking inside structs
    /// avoiding unsafe blocks.
    /// </summary>
    [InlineArray(16)]
    public struct VectorStateBuffer<T>
    {
        private T _element0;
    }
}
