namespace Glacier.Polaris.Memory;

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
/// Unmanaged memory owner backed by 64-byte aligned unmanaged memory (NativeMemory.AlignedAlloc).
/// Implements MemoryManager&lt;T&gt; to provide zero-copy, cache-aligned Memory&lt;T&gt; and Span&lt;T&gt;
/// directly suitable for SIMD (AVX-512, AVX2, ARM Neon) and high-throughput Glacier Compact Columnar IPC v1 streaming.
/// </summary>
public sealed unsafe class NativeMemoryOwner<T> : MemoryManager<T>, IRefCountedMemoryLifetime where T : unmanaged
{
    private nint _pointer;
    private readonly int _length;
    private readonly bool _ownsMemory;
    private int _disposed;
    private int _refCount = 1;
    private int _pinCount;

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
            _pointer = 0;
            GC.SuppressFinalize(this);
            return;
        }

        nuint byteCount = checked((nuint)length * (nuint)sizeof(T));
        void* allocated = NativeMemory.AlignedAlloc(byteCount, 64);
        NativeMemory.Clear(allocated, byteCount);
        _pointer = (nint)allocated;
    }

    /// <summary>
    /// Wraps an existing unmanaged pointer as an IMemoryOwner&lt;T&gt;.
    /// </summary>
    public NativeMemoryOwner(void* pointer, int length, bool ownsMemory = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (length > 0 && pointer == null)
            throw new ArgumentNullException(nameof(pointer));
        _pointer = (nint)pointer;
        _length = length;
        _ownsMemory = ownsMemory;
        if (!ownsMemory || pointer == null)
        {
            GC.SuppressFinalize(this);
        }
    }

#pragma warning disable CA2015 // NativeMemoryOwner owns NativeMemory.AlignedAlloc buffers and refcounts pins/views via IRefCountedMemoryLifetime.
    ~NativeMemoryOwner()
    {
        Dispose(false);
    }
#pragma warning restore CA2015

    bool IRefCountedMemoryLifetime.IsDisposed => Volatile.Read(ref _disposed) != 0;

    bool IRefCountedMemoryLifetime.TryRetain() => TryRetain();

    void IRefCountedMemoryLifetime.Release() => Release();

    private bool TryRetain()
    {
        while (true)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return false;

            int current = Volatile.Read(ref _refCount);
            if (current <= 0)
                return false;

            if (Interlocked.CompareExchange(ref _refCount, current + 1, current) == current)
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    Release();
                    return false;
                }
                return true;
            }
        }
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref _refCount) == 0)
        {
            nint ptr = Interlocked.Exchange(ref _pointer, 0);
            if (_ownsMemory && ptr != 0)
            {
                NativeMemory.AlignedFree((void*)ptr);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Span<T> GetSpan()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_length == 0)
            return Span<T>.Empty;

        nint ptr = Volatile.Read(ref _pointer);
        if (ptr == 0 || Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(NativeMemoryOwner<T>));

        return new Span<T>((void*)ptr, _length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override MemoryHandle Pin(int elementIndex = 0)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_length == 0 && elementIndex == 0)
            return new MemoryHandle(null);

        ArgumentOutOfRangeException.ThrowIfNegative(elementIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(elementIndex, _length);

        if (!TryRetain())
            throw new ObjectDisposedException(nameof(NativeMemoryOwner<T>));

        nint ptr = Volatile.Read(ref _pointer);
        if (ptr == 0)
        {
            Release();
            throw new ObjectDisposedException(nameof(NativeMemoryOwner<T>));
        }

        Interlocked.Increment(ref _pinCount);
        return new MemoryHandle((byte*)ptr + checked((nuint)elementIndex * (nuint)sizeof(T)), pinnable: this);
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
                Release();
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
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            Release();
        }
    }

    /// <summary>Raw unmanaged 64-byte aligned pointer.</summary>
    public void* UnmanagedPointer
    {
        get
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return (void*)_pointer;
        }
    }

    /// <summary>Number of elements of type T.</summary>
    public int Length => _length;

    /// <summary>Direct access to the underlying Span&lt;T&gt;.</summary>
    public Span<T> Span => GetSpan();

    /// <summary>Creates a zero-copy byte memory view over the unmanaged native buffer.</summary>
    public ReadOnlyMemory<byte> AsBytesMemory()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_length == 0)
            return ReadOnlyMemory<byte>.Empty;

        int byteCount = checked((int)checked((nuint)_length * (nuint)sizeof(T)));
        nint ptr = Volatile.Read(ref _pointer);
        if (ptr == 0 || Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(NativeMemoryOwner<T>));

        return new UnmanagedMemoryManager<byte>((byte*)ptr, byteCount, this).Memory;
    }

    /// <summary>Creates a writable byte memory view over the unmanaged native buffer.</summary>
    public Memory<byte> AsWritableBytesMemory()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_length == 0)
            return Memory<byte>.Empty;

        int byteCount = checked((int)checked((nuint)_length * (nuint)sizeof(T)));
        nint ptr = Volatile.Read(ref _pointer);
        if (ptr == 0 || Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(NativeMemoryOwner<T>));

        return new UnmanagedMemoryManager<byte>((byte*)ptr, byteCount, this).Memory;
    }
}
