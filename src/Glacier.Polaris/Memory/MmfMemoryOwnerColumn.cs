using System;
using System.Buffers;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;

namespace Glacier.Polaris.Memory
{
    /// <summary>
    /// An IMemoryOwner implementation that maps a file on disk directly into unmanaged memory.
    /// Exposes the file contents as a standard Memory&lt;T&gt; for zero-copy column operations.
    /// </summary>
    public sealed unsafe class MmfMemoryOwnerColumn<T> : IMemoryOwner<T>, IRefCountedMemoryLifetime where T : unmanaged
    {
        private MemoryMappedFile? _mmf;
        private MemoryMappedViewAccessor? _accessor;
        private byte* _pointer;
        private bool _pointerAcquired;
        private readonly int _length;
        private readonly UnmanagedMemoryManager<T>? _manager;
        private int _disposed;
        private int _refCount = 1;

        public MmfMemoryOwnerColumn(string filePath, int length)
        {
            ArgumentException.ThrowIfNullOrEmpty(filePath);
            ArgumentOutOfRangeException.ThrowIfNegative(length);
            _length = length;

            if (length == 0)
            {
                using (new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
                {
                }
                _manager = new UnmanagedMemoryManager<T>(null, 0, this);
                GC.SuppressFinalize(this);
                return;
            }

            long bytesRequired = checked((long)length * sizeof(T));
            FileStream? fileStream = null;
            bool mmfOwnsStream = false;

            try
            {
                // Open or create the file, resizing if it's too small
                fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                if (fileStream.Length < bytesRequired)
                {
                    fileStream.SetLength(bytesRequired);
                }

                _mmf = MemoryMappedFile.CreateFromFile(fileStream, null, bytesRequired, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: false);
                mmfOwnsStream = true;
                _accessor = _mmf.CreateViewAccessor(0, bytesRequired, MemoryMappedFileAccess.ReadWrite);
                _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);
                _pointerAcquired = true;

                _manager = new UnmanagedMemoryManager<T>((T*)(_pointer + _accessor.PointerOffset), length, this);
            }
            catch
            {
                if (!mmfOwnsStream)
                {
                    fileStream?.Dispose();
                }
                Dispose();
                throw;
            }
        }

        ~MmfMemoryOwnerColumn()
        {
            Dispose(false);
        }

        public Memory<T> Memory
        {
            get
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (_length == 0 || _manager == null)
                    return Memory<T>.Empty;
                return _manager.Memory;
            }
        }

        bool IRefCountedMemoryLifetime.IsDisposed => Volatile.Read(ref _disposed) != 0;

        bool IRefCountedMemoryLifetime.TryRetain()
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
                        Release(disposing: true);
                        return false;
                    }
                    return true;
                }
            }
        }

        void IRefCountedMemoryLifetime.Release() => Release(disposing: true);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                if (disposing)
                {
                    ((IDisposable?)_manager)?.Dispose();
                }
                Release(disposing);
            }
        }

        private void Release(bool disposing)
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
            {
                if (_pointerAcquired && _accessor != null)
                {
                    var handle = _accessor.SafeMemoryMappedViewHandle;
                    if (!handle.IsClosed)
                    {
                        handle.ReleasePointer();
                    }
                    _pointerAcquired = false;
                    _pointer = null;
                }

                if (disposing)
                {
                    _accessor?.Dispose();
                    _mmf?.Dispose();
                }
                _accessor = null;
                _mmf = null;
            }
        }
    }
}
