namespace Glacier.Polaris.Memory;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
/// Unmanaged memory owner backed by 64-byte aligned unmanaged memory (NativeMemory.AlignedAlloc).
/// Implements MemoryManager&lt;T&gt; to provide zero-copy, cache-aligned Memory&lt;T&gt; and Span&lt;T&gt;
/// directly suitable for SIMD (AVX-512, AVX2, ARM Neon) and high-throughput Arrow IPC streaming.
/// </summary>
public sealed unsafe class NativeMemoryOwner<T> : MemoryManager<T> where T : unmanaged
{
    private void* _pointer;
    private readonly int _length;
    private readonly bool _ownsMemory;
    private int _disposed;

    /// <summary>
    /// Allocates unmanaged memory aligned to a 64-byte boundary.
    /// Memory is cleared to zero upon allocation.
    /// </summary>
    public NativeMemoryOwner(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        _length = length;
        _ownsMemory = true;

        if (length == 0)
        {
            _pointer = null;
            return;
        }

        nuint byteCount = (nuint)length * (nuint)sizeof(T);
        _pointer = NativeMemory.AlignedAlloc(byteCount, 64);
        NativeMemory.Clear(_pointer, byteCount);
    }

    /// <summary>
    /// Wraps an existing unmanaged pointer as an IMemoryOwner&lt;T&gt;.
    /// </summary>
    public NativeMemoryOwner(void* pointer, int length, bool ownsMemory = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        _pointer = pointer;
        _length = length;
        _ownsMemory = ownsMemory;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Span<T> GetSpan()
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (_length == 0 || _pointer == null)
            return Span<T>.Empty;

        return new Span<T>(_pointer, _length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override MemoryHandle Pin(int elementIndex = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (_length == 0 && elementIndex == 0)
            return new MemoryHandle(null);

        ArgumentOutOfRangeException.ThrowIfNegative(elementIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(elementIndex, _length);
        return new MemoryHandle((byte*)_pointer + ((nuint)elementIndex * (nuint)sizeof(T)));
    }

    public override void Unpin()
    {
        // Unmanaged memory does not move; no unpinning required.
    }

    protected override void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            if (_ownsMemory && _pointer != null)
            {
                NativeMemory.AlignedFree(_pointer);
                _pointer = null;
            }
        }
    }

    /// <summary>Raw unmanaged 64-byte aligned pointer.</summary>
    public void* UnmanagedPointer => _pointer;

    /// <summary>Number of elements of type T.</summary>
    public int Length => _length;

    /// <summary>Direct access to the underlying Span&lt;T&gt;.</summary>
    public Span<T> Span => GetSpan();

    /// <summary>Creates a zero-copy byte memory view over the unmanaged native buffer.</summary>
    public ReadOnlyMemory<byte> AsBytesMemory()
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (_length == 0 || _pointer == null)
            return ReadOnlyMemory<byte>.Empty;

        return new UnmanagedMemoryManager<byte>((byte*)_pointer, _length * sizeof(T)).Memory;
    }

    /// <summary>Creates a writable byte memory view over the unmanaged native buffer.</summary>
    public Memory<byte> AsWritableBytesMemory()
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (_length == 0 || _pointer == null)
            return Memory<byte>.Empty;

        return new UnmanagedMemoryManager<byte>((byte*)_pointer, _length * sizeof(T)).Memory;
    }
}
