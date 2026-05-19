using System;
using System.Buffers;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace Glacier.Polaris.Memory
{
    /// <summary>
    /// An IMemoryOwner implementation that maps a file on disk directly into unmanaged memory.
    /// Exposes the file contents as a standard Memory<T> for zero-copy column operations.
    /// </summary>
    public sealed unsafe class MmfMemoryOwnerColumn<T> : IMemoryOwner<T> where T : unmanaged
    {
        private MemoryMappedFile? _mmf;
        private MemoryMappedViewAccessor? _accessor;
        private byte* _pointer;
        private readonly int _length;
        private readonly UnmanagedMemoryManager<T> _manager;

        public MmfMemoryOwnerColumn(string filePath, int length)
        {
            _length = length;
            long bytesRequired = (long)length * sizeof(T);

            // Open or create the file, resizing if it's too small
            var fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            if (fileStream.Length < bytesRequired)
            {
                fileStream.SetLength(bytesRequired);
            }

            _mmf = MemoryMappedFile.CreateFromFile(fileStream, null, bytesRequired, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: false);
            _accessor = _mmf.CreateViewAccessor(0, bytesRequired, MemoryMappedFileAccess.ReadWrite);
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);

            _manager = new UnmanagedMemoryManager<T>((T*)(_pointer + _accessor.PointerOffset), length);
        }

        public Memory<T> Memory => _manager.Memory;

        public void Dispose()
        {
            if (_pointer != null)
            {
                _accessor?.SafeMemoryMappedViewHandle.ReleasePointer();
                _pointer = null;
            }
            _accessor?.Dispose();
            _mmf?.Dispose();
        }
    }
}
