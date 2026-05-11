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

        public MemoryOwnerColumn(int length)
        {
            _length = length;
            // Rent from ArrayPool
            _rentedArray = ArrayPool<T>.Shared.Rent(length);
        }

        public Memory<T> Memory
        {
            get
            {
                if (_rentedArray == null) throw new ObjectDisposedException(nameof(MemoryOwnerColumn<T>));
                return new Memory<T>(_rentedArray, 0, _length);
            }
        }

        public void Dispose()
        {
            if (_rentedArray != null)
            {
                ArrayPool<T>.Shared.Return(_rentedArray);
                _rentedArray = null; // Prevent double return
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
