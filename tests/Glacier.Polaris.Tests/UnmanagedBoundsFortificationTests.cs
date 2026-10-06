using System;
using System.IO;
using System.Runtime.InteropServices;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Glacier.Polaris.Memory;

namespace Glacier.Polaris.Tests
{
    public class UnmanagedBoundsFortificationTests
    {
        // ---------------------------------------------------------------------
        // 1. NativeMemoryOwner<T>
        // ---------------------------------------------------------------------
        [Fact]
        public void NativeMemoryOwner_NegativeLength_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new NativeMemoryOwner<int>(-1));
        }

        [Fact]
        public unsafe void NativeMemoryOwner_NullPointerWithNonZeroLength_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new NativeMemoryOwner<int>(null, 10));
        }

        [Fact]
        public void NativeMemoryOwner_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            var owner = new NativeMemoryOwner<int>(16);
            Assert.Equal(16, owner.Length);

            void AccessSpan() { _ = owner.Span; }
            void AccessGetSpan() { _ = owner.GetSpan(); }

            owner.Dispose();

            Assert.Throws<ObjectDisposedException>(AccessSpan);
            Assert.Throws<ObjectDisposedException>(AccessGetSpan);
            Assert.Throws<ObjectDisposedException>(() => owner.Memory);
            Assert.Throws<ObjectDisposedException>(() => owner.AsBytesMemory());
            Assert.Throws<ObjectDisposedException>(() => owner.AsWritableBytesMemory());
            Assert.Throws<ObjectDisposedException>(() => owner.Pin(0));
            unsafe
            {
                Assert.Throws<ObjectDisposedException>(() => _ = owner.UnmanagedPointer);
            }

            // Idempotent dispose
            owner.Dispose();
        }

        [Fact]
        public void NativeMemoryOwner_PinOutOfRange_ThrowsArgumentOutOfRangeException()
        {
            using var owner = new NativeMemoryOwner<int>(10);
            Assert.Throws<ArgumentOutOfRangeException>(() => owner.Pin(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => owner.Pin(10));
        }

        // ---------------------------------------------------------------------
        // 2. UnmanagedMemoryManager<T>
        // ---------------------------------------------------------------------
        [Fact]
        public unsafe void UnmanagedMemoryManager_NegativeLength_ThrowsArgumentOutOfRangeException()
        {
            int* ptr = (int*)NativeMemory.Alloc(sizeof(int));
            try
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new UnmanagedMemoryManager<int>(ptr, -1));
            }
            finally
            {
                NativeMemory.Free(ptr);
            }
        }

        [Fact]
        public unsafe void UnmanagedMemoryManager_NullPointer_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => new UnmanagedMemoryManager<int>(null, 10));
        }

        [Fact]
        public unsafe void UnmanagedMemoryManager_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            int* ptr = (int*)NativeMemory.Alloc(sizeof(int));
            try
            {
                var manager = new UnmanagedMemoryManager<int>(ptr, 1);
                void AccessGetSpan() { _ = manager.GetSpan(); }

                manager.Dispose();

                Assert.Throws<ObjectDisposedException>(AccessGetSpan);
                Assert.Throws<ObjectDisposedException>(() => manager.Pin(0));

                // Idempotent dispose
                manager.Dispose();
            }
            finally
            {
                NativeMemory.Free(ptr);
            }
        }

        // ---------------------------------------------------------------------
        // 3. MemoryOwnerColumn<T>
        // ---------------------------------------------------------------------
        [Fact]
        public void MemoryOwnerColumn_NegativeLength_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new MemoryOwnerColumn<double>(-5));
        }

        [Fact]
        public void MemoryOwnerColumn_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            var col = new MemoryOwnerColumn<double>(8);
            Assert.Equal(8, col.Memory.Length);

            col.Dispose();

            Assert.Throws<ObjectDisposedException>(() => col.Memory);

            // Idempotent dispose
            col.Dispose();
        }

        // ---------------------------------------------------------------------
        // 4. MmfMemoryOwnerColumn<T>
        // ---------------------------------------------------------------------
        [Fact]
        public void MmfMemoryOwnerColumn_NegativeLength_ThrowsArgumentOutOfRangeException()
        {
            string tempFile = Path.Combine(Path.GetTempPath(), $"mmf_fortify_{Guid.NewGuid():N}.bin");
            try
            {
                Assert.Throws<ArgumentOutOfRangeException>(() => new MmfMemoryOwnerColumn<double>(tempFile, -1));
            }
            finally
            {
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
        }

        [Fact]
        public void MmfMemoryOwnerColumn_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            string tempFile = Path.Combine(Path.GetTempPath(), $"mmf_fortify_{Guid.NewGuid():N}.bin");
            try
            {
                var col = new MmfMemoryOwnerColumn<double>(tempFile, 10);
                Assert.Equal(10, col.Memory.Length);

                col.Dispose();

                Assert.Throws<ObjectDisposedException>(() => col.Memory);

                // Idempotent dispose
                col.Dispose();
            }
            finally
            {
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
        }

        // ---------------------------------------------------------------------
        // 5. ValidityMask
        // ---------------------------------------------------------------------
        [Fact]
        public void ValidityMask_NegativeLength_ThrowsArgumentOutOfRangeException()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ValidityMask(-1));
        }

        [Fact]
        public void ValidityMask_IndexOutOfBounds_ThrowsArgumentOutOfRangeException()
        {
            var mask = new ValidityMask(10);
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.IsValid(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.IsValid(10));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.SetNull(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.SetNull(10));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.SetValid(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.SetValid(10));
        }

        [Fact]
        public void ValidityMask_WordIndexOutOfBounds_ThrowsArgumentOutOfRangeException()
        {
            var mask = new ValidityMask(64); // WordCount = 1
            Assert.Equal(1, mask.WordCount);
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.GetWord(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.GetWord(1));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.SetWord(-1, 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => mask.SetWord(1, 0));
        }

        [Fact]
        public void ValidityMask_CopyNegativeOffset_ThrowsArgumentOutOfRangeException()
        {
            var src = new ValidityMask(10);
            var dst = new ValidityMask(20);
            Assert.Throws<ArgumentOutOfRangeException>(() => src.CopyTo(dst, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => src.CopyToBulk(dst, -1));
        }

        // ---------------------------------------------------------------------
        // 6. Series<T>
        // ---------------------------------------------------------------------
        [Fact]
        public void Series_IndexOutOfBounds_ThrowsArgumentOutOfRangeException()
        {
            using var s = new Int32Series("a", new int[] { 10, 20, 30 });
            Assert.Throws<ArgumentOutOfRangeException>(() => s[-1]);
            Assert.Throws<ArgumentOutOfRangeException>(() => s[3]);
            Assert.Throws<ArgumentOutOfRangeException>(() => s.Get(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.Get(3));
        }

        [Fact]
        public void Series_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            var s = new Int32Series("a", new int[] { 1, 2, 3 });
            Assert.Equal(3, s.Length);

            s.Dispose();

            Assert.Throws<ObjectDisposedException>(() => s.Memory);
            Assert.Throws<ObjectDisposedException>(() => s.AsBytesMemory());
            Assert.Throws<ObjectDisposedException>(() => s[0]);
            Assert.Throws<ObjectDisposedException>(() => s.Get(0));

            // Idempotent dispose
            s.Dispose();
        }

        // ---------------------------------------------------------------------
        // 7. Utf8StringSeries
        // ---------------------------------------------------------------------
        [Fact]
        public void Utf8StringSeries_IndexOutOfBounds_ThrowsArgumentOutOfRangeException()
        {
            using var s = new Utf8StringSeries("s", new string[] { "hello", "world" });
            void AccessSpanNeg() { _ = s.GetStringSpan(-1); }
            void AccessSpanOutOfRange() { _ = s.GetStringSpan(2); }

            Assert.Throws<ArgumentOutOfRangeException>(AccessSpanNeg);
            Assert.Throws<ArgumentOutOfRangeException>(AccessSpanOutOfRange);
            Assert.Throws<ArgumentOutOfRangeException>(() => s.GetString(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.GetString(2));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.Get(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.Get(2));
        }

        [Fact]
        public void Utf8StringSeries_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            var s = new Utf8StringSeries("s", new string[] { "foo", "bar" });
            Assert.Equal(2, s.Length);

            void AccessSpan() { _ = s.GetStringSpan(0); }

            s.Dispose();

            Assert.Throws<ObjectDisposedException>(() => s.DataBytes);
            Assert.Throws<ObjectDisposedException>(() => s.Offsets);
            Assert.Throws<ObjectDisposedException>(AccessSpan);
            Assert.Throws<ObjectDisposedException>(() => s.GetString(0));
            Assert.Throws<ObjectDisposedException>(() => s.Get(0));

            // Idempotent dispose
            s.Dispose();
        }

        // ---------------------------------------------------------------------
        // 8. BinarySeries
        // ---------------------------------------------------------------------
        [Fact]
        public void BinarySeries_IndexOutOfBounds_ThrowsArgumentOutOfRangeException()
        {
            using var s = new BinarySeries("b", new byte[]?[] { new byte[] { 1, 2 }, new byte[] { 3 } });
            void AccessSpanNeg() { _ = s.GetSpan(-1); }
            void AccessSpanOutOfRange() { _ = s.GetSpan(2); }

            Assert.Throws<ArgumentOutOfRangeException>(AccessSpanNeg);
            Assert.Throws<ArgumentOutOfRangeException>(AccessSpanOutOfRange);
            Assert.Throws<ArgumentOutOfRangeException>(() => s.GetValue(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.GetValue(2));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.Get(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => s.Get(2));
        }

        [Fact]
        public void BinarySeries_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            var s = new BinarySeries("b", new byte[]?[] { new byte[] { 1, 2 } });
            Assert.Equal(1, s.Length);

            void AccessSpan() { _ = s.GetSpan(0); }

            s.Dispose();

            Assert.Throws<ObjectDisposedException>(() => s.DataBytes);
            Assert.Throws<ObjectDisposedException>(() => s.Offsets);
            Assert.Throws<ObjectDisposedException>(AccessSpan);
            Assert.Throws<ObjectDisposedException>(() => s.GetValue(0));
            Assert.Throws<ObjectDisposedException>(() => s.Get(0));

            // Idempotent dispose
            s.Dispose();
        }

        // ---------------------------------------------------------------------
        // 9. DataFrame
        // ---------------------------------------------------------------------
        [Fact]
        public void DataFrame_AccessAfterDispose_ThrowsObjectDisposedException()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new int[] { 1, 2 }),
                new Float64Series("b", new double[] { 1.5, 2.5 })
            });

            Assert.Equal(2, df.RowCount);
            Assert.Equal(2, df.Columns.Count);

            df.Dispose();

            Assert.Throws<ObjectDisposedException>(() => df.RowCount);
            Assert.Throws<ObjectDisposedException>(() => df.Columns);
            Assert.Throws<ObjectDisposedException>(() => df.GetColumn("a"));
            Assert.Throws<ObjectDisposedException>(() => df.Select("a"));
            Assert.Throws<ObjectDisposedException>(() => df.Clone());

            // Idempotent dispose
            df.Dispose();
        }

        // ---------------------------------------------------------------------
        // 10. ComputeKernels.Take and TakeWithNulls
        // ---------------------------------------------------------------------
        [Fact]
        public void ComputeKernels_Take_ShortDestination_ThrowsArgumentException()
        {
            int[] src = new int[] { 10, 20, 30, 40 };
            int[] idx = new int[] { 0, 1, 2 };
            int[] shortDst = new int[2]; // length 2 < indices length 3

            Assert.Throws<ArgumentException>(() => ComputeKernels.Take<int>(src, idx, shortDst));
        }

        [Fact]
        public void ComputeKernels_TakeWithNulls_ShortDestinationOrMask_ThrowsArgumentException()
        {
            int[] src = new int[] { 10, 20, 30 };
            int[] idx = new int[] { 0, -1, 2 };
            int[] shortDst = new int[2];
            int[] goodDst = new int[3];
            var shortMask = new ValidityMask(2);
            var goodMask = new ValidityMask(3);

            Assert.Throws<ArgumentException>(() => ComputeKernels.TakeWithNulls<int>(src, idx, shortDst, goodMask));
            Assert.Throws<ArgumentException>(() => ComputeKernels.TakeWithNulls<int>(src, idx, goodDst, shortMask));
        }

        [Fact]
        public void ComputeKernels_TakeWithNulls_ValidExecution_SetsValuesAndNullBits()
        {
            int[] src = new int[] { 10, 20, 30 };
            int[] idx = new int[] { 2, -1, 0 };
            int[] dst = new int[3];
            var mask = new ValidityMask(3);

            ComputeKernels.TakeWithNulls<int>(src, idx, dst, mask);

            Assert.Equal(30, dst[0]);
            Assert.Equal(0, dst[1]);
            Assert.Equal(10, dst[2]);

            Assert.True(mask.IsValid(0));
            Assert.False(mask.IsValid(1)); // -1 was converted to null
            Assert.True(mask.IsValid(2));
        }
    }
}
