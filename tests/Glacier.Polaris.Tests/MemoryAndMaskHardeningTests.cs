using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Glacier.Polaris.Memory;
using Xunit;

namespace Glacier.Polaris.Tests;

public class MemoryAndMaskHardeningTests
{
    // -------------------------------------------------------------------------
    // Feature #1: NativeMemoryOwner & Series<T> Lifetime / Overflow
    // -------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference CreateAndDropNativeMemoryOwner()
    {
        var owner = new NativeMemoryOwner<int>(64);
        owner.Span[0] = 12345;
        return new WeakReference(owner);
    }

    [Fact]
    public void NativeMemoryOwner_Finalizer_FreesUnownedCollectedInstance()
    {
        WeakReference weak = CreateAndDropNativeMemoryOwner();
        for (int i = 0; i < 4 && weak.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        Assert.False(weak.IsAlive);
    }

    [Fact]
    public unsafe void NativeMemoryOwner_PinAndDispose_DefersFreeUntilUnpin()
    {
        var owner = new NativeMemoryOwner<int>(16);
        owner.Span[0] = 42;
        owner.Span[1] = 99;

        MemoryHandle directPin = owner.Pin(0);
        ReadOnlyMemory<byte> bytesMem = owner.AsBytesMemory();
        MemoryHandle bytesPin = bytesMem.Pin();

        owner.Dispose();

        // Owner accessors throw immediately after Dispose
        Assert.Throws<ObjectDisposedException>(() => _ = owner.Span);
        Assert.Throws<ObjectDisposedException>(() => _ = bytesMem.Span);

        // Pinned memory remains valid and accessible until both handles are disposed
        int* pInt = (int*)directPin.Pointer;
        Assert.Equal(42, pInt[0]);
        Assert.Equal(99, pInt[1]);
        pInt[1] = 777;

        byte* pByte = (byte*)bytesPin.Pointer;
        Assert.Equal(777, *(int*)(pByte + sizeof(int)));

        directPin.Dispose();
        // Still pinned by bytesPin
        Assert.Equal(777, *(int*)(pByte + sizeof(int)));

        bytesPin.Dispose();
    }

    [Fact]
    public void NativeMemoryOwner_AsBytesMemory_ThrowsObjectDisposedAfterOwnerDispose()
    {
        var owner = new NativeMemoryOwner<int>(8);
        owner.Span[0] = 7;

        ReadOnlyMemory<byte> roMem = owner.AsBytesMemory();
        Memory<byte> rwMem = owner.AsWritableBytesMemory();

        owner.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = roMem.Span);
        Assert.Throws<ObjectDisposedException>(() => roMem.Pin());
        Assert.Throws<ObjectDisposedException>(() => _ = rwMem.Span);
        Assert.Throws<ObjectDisposedException>(() => rwMem.Pin());
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ReadOnlyMemory<byte> CreateBytesViewAndDropOwner()
    {
        var owner = new NativeMemoryOwner<int>(8);
        owner.Span[0] = 0x11223344;
        owner.Span[7] = 0x55667788;
        return owner.AsBytesMemory();
    }

    [Fact]
    public void NativeMemoryOwner_AsBytesMemory_KeepsOwnerAliveAcrossGC()
    {
        ReadOnlyMemory<byte> mem = CreateBytesViewAndDropOwner();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        ReadOnlySpan<int> ints = MemoryMarshal.Cast<byte, int>(mem.Span);
        Assert.Equal(0x11223344, ints[0]);
        Assert.Equal(0x55667788, ints[7]);
    }

    [Fact]
    public unsafe void NativeMemoryOwner_And_Series_ByteLengthOverflow_ThrowsOverflowException()
    {
        byte dummy = 0;
        // 300_000_000 * sizeof(double) = 2_400_000_000 > int.MaxValue (zero actual 2.4 GB allocation)
        using var nonOwning = new NativeMemoryOwner<double>(&dummy, 300_000_000, ownsMemory: false);
        Assert.Throws<OverflowException>(() => nonOwning.AsBytesMemory());
        Assert.Throws<OverflowException>(() => nonOwning.AsWritableBytesMemory());

        using var series = new Float64Series("huge_logical", 300_000_000, new NativeMemoryOwner<double>(&dummy, 300_000_000, ownsMemory: false), new ValidityMask(0));
        Assert.Throws<OverflowException>(() => series.AsBytesMemory());
    }

    // -------------------------------------------------------------------------
    // Feature #2: MmfMemoryOwnerColumn Hardening
    // -------------------------------------------------------------------------

    [Fact]
    public void MmfMemoryOwnerColumn_ZeroLength_SucceedsAndDisposesCleanly()
    {
        string tempFile = Path.Combine(Path.GetTempPath(), $"polaris_mmf_zero_{Guid.NewGuid():N}.bin");
        try
        {
            using (var col = new MmfMemoryOwnerColumn<int>(tempFile, 0))
            {
                Assert.True(col.Memory.IsEmpty);
            }
            Assert.True(File.Exists(tempFile));
            File.Delete(tempFile);
            Assert.False(File.Exists(tempFile));

            using (var series = Int32Series.FromMmf("empty", tempFile, 0))
            {
                Assert.Equal(0, series.Length);
                Assert.True(series.Memory.IsEmpty);
            }

            var disposedCol = new MmfMemoryOwnerColumn<int>(tempFile, 0);
            disposedCol.Dispose();
            Assert.Throws<ObjectDisposedException>(() => _ = disposedCol.Memory);
            File.Delete(tempFile);
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void MmfMemoryOwnerColumn_FailedInit_DoesNotLeakFileHandle()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"polaris_mmf_dir_{Guid.NewGuid():N}");
        string tempFile = Path.Combine(Path.GetTempPath(), $"polaris_mmf_lock_{Guid.NewGuid():N}.bin");
        Directory.CreateDirectory(tempDir);
        try
        {
            Assert.Throws<ArgumentException>(() => new MmfMemoryOwnerColumn<int>("", 8));
            // Passing a directory path causes FileStream ctor to fail (UnauthorizedAccessException on Windows / IOException on Unix)
            Assert.ThrowsAny<Exception>(() => new MmfMemoryOwnerColumn<int>(tempDir, 8));
            Directory.Delete(tempDir);
            Assert.False(Directory.Exists(tempDir));

            // Test failed resize/open against an existing locked file
            using (var holder = new FileStream(tempFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None))
            {
                Assert.ThrowsAny<IOException>(() => new MmfMemoryOwnerColumn<int>(tempFile, 8));
            }
            File.Delete(tempFile);
            Assert.False(File.Exists(tempFile));
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
            if (File.Exists(tempFile)) try { File.Delete(tempFile); } catch { }
        }
    }

    [Fact]
    public unsafe void MmfMemoryOwnerColumn_PinOutlivesDispose_SafeUntilHandleDisposed()
    {
        string tempFile = Path.Combine(Path.GetTempPath(), $"polaris_mmf_pin_{Guid.NewGuid():N}.bin");
        try
        {
            var col = new MmfMemoryOwnerColumn<int>(tempFile, 4);
            col.Memory.Span[0] = 314;
            col.Memory.Span[3] = 271;

            MemoryHandle handle = col.Memory.Pin();
            col.Dispose();

            Assert.Throws<ObjectDisposedException>(() => _ = col.Memory);
            int* ptr = (int*)handle.Pointer;
            Assert.Equal(314, ptr[0]);
            Assert.Equal(271, ptr[3]);

            handle.Dispose();
            File.Delete(tempFile);
            Assert.False(File.Exists(tempFile));
        }
        finally
        {
            if (File.Exists(tempFile)) try { File.Delete(tempFile); } catch { }
        }
    }

    // -------------------------------------------------------------------------
    // Feature #3: ValidityMask & Series<T>.CopyTo
    // -------------------------------------------------------------------------

    [Fact]
    public unsafe void ValidityMask_GetRawBitsRef_ZeroLength_DoesNotThrow_AndPinsToNull()
    {
        var mask = new ValidityMask(0);
        ref ulong rawRef = ref mask.GetRawBitsRef();
        fixed (ulong* p = &rawRef)
        {
            Assert.True(p == null);
        }

        using var emptyInt = new Int32Series("e", 0);
        foreach (FillStrategy strategy in Enum.GetValues<FillStrategy>())
        {
            using var filled = FillNullKernels.FillNull(emptyInt, strategy);
            Assert.Equal(0, filled.Length);
        }
    }

    [Fact]
    public void ValidityMask_CopyToNullBitmap_DoesNotMutateInternalMask()
    {
        var mask = new ValidityMask(10, setAllValid: false);
        mask.SetValid(0);
        mask.SetValid(9);
        // Explicitly clear raw unused tail bits to verify CopyToNullBitmap does not mutate _mask
        mask.AsWritableSpan()[0] &= (1UL << 10) - 1UL;
        ulong rawBefore = mask.AsSpan()[0];

        byte[] dest = new byte[2];
        mask.CopyToNullBitmap(dest);

        // Unused bits 10..15 in byte 1 (bits 2..7 => 0xFC) must be set on dest
        Assert.Equal((byte)0x01, dest[0]);
        Assert.Equal((byte)(0x02 | 0xFC), dest[1]);
        // Internal backing word must remain unmutated
        Assert.Equal(rawBefore, mask.AsSpan()[0]);

        // Concurrent CopyToNullBitmap / GetNullBitmapMemory stress
        Parallel.For(0, 64, _idx =>
        {
            Span<byte> localDest = stackalloc byte[2];
            mask.CopyToNullBitmap(localDest);
            Assert.Equal((byte)(0x02 | 0xFC), localDest[1]);
            _ = mask.GetNullBitmapMemory();
        });
    }

    [Fact]
    public void ValidityMask_CopyToBulk_AlignedAndUnalignedBitOffsets_MatchesBitwiseOracle()
    {
        int[] srcLengths = [0, 1, 7, 8, 63, 64, 65, 130];
        int[] targetOffsets = [0, 1, 7, 13, 63, 64, 65, 127];

        foreach (int srcLen in srcLengths)
        {
            foreach (int targetOffset in targetOffsets)
            {
                int dstLen = targetOffset + srcLen + 19;
                for (int pattern = 0; pattern < 4; pattern++)
                {
                    var src = new ValidityMask(srcLen, setAllValid: true);
                    for (int i = 0; i < srcLen; i++)
                    {
                        bool isNull = pattern switch
                        {
                            0 => false,
                            1 => true,
                            2 => (i % 3 == 0) || (i == srcLen - 1),
                            _ => (i & 1) == 1
                        };
                        if (isNull) src.SetNull(i);
                    }

                    var dstBulk = new ValidityMask(dstLen, setAllValid: true);
                    var dstOracle = new ValidityMask(dstLen, setAllValid: true);

                    // Seed pre-existing nulls outside [targetOffset, targetOffset + srcLen)
                    if (targetOffset > 0)
                    {
                        dstBulk.SetNull(0);
                        dstOracle.SetNull(0);
                    }
                    if (targetOffset + srcLen < dstLen)
                    {
                        dstBulk.SetNull(targetOffset + srcLen);
                        dstOracle.SetNull(targetOffset + srcLen);
                        dstBulk.SetNull(dstLen - 1);
                        dstOracle.SetNull(dstLen - 1);
                    }

                    src.CopyToBulk(dstBulk, targetOffset);

                    for (int i = 0; i < srcLen; i++)
                    {
                        if (src.IsNull(i)) dstOracle.SetNull(targetOffset + i);
                    }

                    Assert.Equal(dstOracle.HasNulls, dstBulk.HasNulls);
                    Assert.Equal(dstOracle.NullCount, dstBulk.NullCount);
                    for (int i = 0; i < dstLen; i++)
                    {
                        Assert.Equal(dstOracle.IsValid(i), dstBulk.IsValid(i));
                    }
                }
            }
        }

        // Overflow guard: targetOffset near int.MaxValue must throw ArgumentOutOfRangeException
        var smallSrc = new ValidityMask(10);
        var smallDst = new ValidityMask(20);
        Assert.Throws<ArgumentOutOfRangeException>(() => smallSrc.CopyToBulk(smallDst, int.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() => smallSrc.CopyTo(smallDst, int.MaxValue));
    }

    [Fact]
    public void Series_CopyTo_PreservesValidityMask_AndOverwritesPriorTargetNulls()
    {
        using var srcWithNulls = Int32Series.FromValues("s", [10, null, 30, null]);
        using var target1 = Int32Series.FromValues("t", new int?[10]);
        using var target2 = Int32Series.FromValues("t", new int?[75]);

        srcWithNulls.CopyTo(target1, 3);
        Assert.Equal(10, target1.Get(3));
        Assert.Null(target1.Get(4));
        Assert.Equal(30, target1.Get(5));
        Assert.Null(target1.Get(6));
        Assert.Null(target1.Get(2));
        Assert.Null(target1.Get(7));

        // Overwrite pre-existing nulls at offset 65 with all-valid source
        using var allValid = new Int32Series("v", [100, 200, 300, 400]);
        target2.ValidityMask.SetNull(64);
        target2.ValidityMask.SetNull(65);
        target2.ValidityMask.SetNull(68);
        target2.ValidityMask.SetNull(70);

        allValid.CopyTo(target2, 65);
        Assert.True(target2.ValidityMask.IsNull(64));
        Assert.Equal(100, target2.Get(65));
        Assert.Equal(200, target2.Get(66));
        Assert.Equal(300, target2.Get(67));
        Assert.Equal(400, target2.Get(68));
        Assert.True(target2.ValidityMask.IsNull(70));

        // Composite ArraySeries.CopyTo preserves element-level nulls
        using var arrValues = Int32Series.FromValues("v", [1, null, null, 4]);
        using var arrSrc = new ArraySeries("a", 2, arrValues);
        using var arrDst = new ArraySeries("a", 2, new Int32Series("v", 6));
        arrSrc.CopyTo(arrDst, 1);
        Assert.Equal(1, arrDst.Values.Get(2));
        Assert.Null(arrDst.Values.Get(3));
        Assert.Null(arrDst.Values.Get(4));
        Assert.Equal(4, arrDst.Values.Get(5));

        // Composite ListSeries.CopyTo preserves element-level nulls
        using var listOffsets = new Int32Series("o", [0, 2, 3]);
        using var listValues = Int32Series.FromValues("lv", [5, null, 9]);
        using var listSrc = new ListSeries("l", listOffsets, listValues);
        using var listDst = new ListSeries("l", new Int32Series("o", 4), new Int32Series("lv", 4));
        listSrc.CopyTo(listDst, 1);
        var row1 = (object?[]?)listDst.Get(1);
        Assert.NotNull(row1);
        Assert.Equal(5, row1![0]);
        Assert.Null(row1[1]);
        var row2 = (object?[]?)listDst.Get(2);
        Assert.NotNull(row2);
        Assert.Equal(9, row2![0]);
    }

    [Fact]
    public async Task NativeMemoryOwner_ConcurrentPinAsBytesMemoryAndDispose_NeverThrowsArgumentNullException()
    {
        var unexpected = new ConcurrentQueue<Exception>();

        for (int iter = 0; iter < 400; iter++)
        {
            var owner = new NativeMemoryOwner<double>(64);
            owner.Span[0] = iter;

            using var gate = new ManualResetEventSlim(false);
            var t1 = Task.Run(() =>
            {
                gate.Wait();
                try
                {
                    using var handle = owner.Pin(0);
                }
                catch (ObjectDisposedException) { }
                catch (Exception ex) { unexpected.Enqueue(ex); }
            });
            var t2 = Task.Run(() =>
            {
                gate.Wait();
                try
                {
                    var ro = owner.AsBytesMemory();
                    using var handle = ro.Pin();
                }
                catch (ObjectDisposedException) { }
                catch (Exception ex) { unexpected.Enqueue(ex); }
            });
            var t3 = Task.Run(() =>
            {
                gate.Wait();
                try
                {
                    var rw = owner.AsWritableBytesMemory();
                    _ = rw.Span;
                }
                catch (ObjectDisposedException) { }
                catch (Exception ex) { unexpected.Enqueue(ex); }
            });
            var t4 = Task.Run(() =>
            {
                gate.Wait();
                owner.Dispose();
            });

            gate.Set();
            await Task.WhenAll(t1, t2, t3, t4);
            owner.Dispose();
        }

        Assert.Empty(unexpected);
    }

    [Fact]
    public void ListSeries_CopyTo_Utf8AndBinaryValues_AppendSpliceNullsAndOverflow()
    {
        // 1. Utf8StringSeries backing: grow + append + splice + child/list nulls
        using var off1 = new Int32Series("o1", [0, 2, 3]);
        using var val1 = Utf8StringSeries.FromStrings("v1", ["alpha", null, "beta"]);
        using var list1 = new ListSeries("l1", off1, val1);
        list1.ValidityMask.SetValid(0);
        list1.ValidityMask.SetValid(1);

        using var off2 = new Int32Series("o2", [0, 0, 2]);
        using var val2 = Utf8StringSeries.FromStrings("v2", [null, "gamma"]);
        using var list2 = new ListSeries("l2", off2, val2);
        list2.ValidityMask.SetNull(0);
        list2.ValidityMask.SetValid(1);

        // Append into empty target
        using var target = new ListSeries("t", new Int32Series("to", 5), new Utf8StringSeries("tv", 0));
        list1.CopyTo(target, 0);
        list2.CopyTo(target, 2);

        Assert.Equal([0, 2, 3, 3, 5], target.Offsets.Memory.Span.ToArray());
        Assert.True(target.ValidityMask.IsValid(0));
        Assert.True(target.ValidityMask.IsValid(1));
        Assert.True(target.ValidityMask.IsNull(2));
        Assert.True(target.ValidityMask.IsValid(3));

        var r0 = Assert.IsType<object?[]>(target.Get(0));
        Assert.Equal("alpha", r0[0]);
        Assert.Null(r0[1]);
        var r1 = Assert.IsType<object?[]>(target.Get(1));
        Assert.Equal("beta", r1[0]);
        Assert.Null(target.Get(2));
        var r3 = Assert.IsType<object?[]>(target.Get(3));
        Assert.Null(r3[0]);
        Assert.Equal("gamma", r3[1]);

        // Splice/overwrite at offset=1 (prefix preserved, existing trailing bytes in target.Values safely replaced)
        using var offSplice = new Int32Series("os", [0, 2]);
        using var valSplice = Utf8StringSeries.FromStrings("vs", ["delta", null]);
        using var listSplice = new ListSeries("ls", offSplice, valSplice);
        listSplice.CopyTo(target, 1);

        Assert.Equal(2, target.Offsets.Memory.Span[1]);
        Assert.Equal(4, target.Offsets.Memory.Span[2]);
        var s0 = Assert.IsType<object?[]>(target.Get(0));
        Assert.Equal("alpha", s0[0]);
        Assert.Null(s0[1]);
        var s1 = Assert.IsType<object?[]>(target.Get(1));
        Assert.Equal("delta", s1[0]);
        Assert.Null(s1[1]);

        // Pre-sized in-place fast-path for Utf8StringSeries
        using var preSizedTarget = new ListSeries("pt", new Int32Series("pto", 4), new Utf8StringSeries("ptv", 5));
        list1.CopyTo(preSizedTarget, 0);
        var p0 = Assert.IsType<object?[]>(preSizedTarget.Get(0));
        Assert.Equal("alpha", p0[0]);
        Assert.Null(p0[1]);
        var p1 = Assert.IsType<object?[]>(preSizedTarget.Get(1));
        Assert.Equal("beta", p1[0]);

        // StringKernels.Split output CopyTo
        using var rawStrings = Utf8StringSeries.FromStrings("s", ["a,b", null, "c"]);
        using var splitList = StringKernels.Split(rawStrings, ",");
        using var splitTarget = (ListSeries)splitList.CloneEmpty(splitList.Length);
        splitList.CopyTo(splitTarget, 0);
        Assert.Equal(["a", "b"], Assert.IsType<object?[]>(splitTarget.Get(0)));
        Assert.Empty(Assert.IsType<object?[]>(splitTarget.Get(1)));
        Assert.Equal(["c"], Assert.IsType<object?[]>(splitTarget.Get(2)));

        // BinarySeries backing: append + splice + nulls
        using var binOff1 = new Int32Series("bo1", [0, 2]);
        using var binVal1 = new BinarySeries("bv1", new byte[]?[] { new byte[] { 1, 2 }, null });
        using var binList1 = new ListSeries("bl1", binOff1, binVal1);
        using var binOff2 = new Int32Series("bo2", [0, 1]);
        using var binVal2 = new BinarySeries("bv2", new byte[]?[] { new byte[] { 9 } });
        using var binList2 = new ListSeries("bl2", binOff2, binVal2);
        using var binTarget = new ListSeries("bt", new Int32Series("bto", 3), new BinarySeries("btv", 0, 0));
        binList1.CopyTo(binTarget, 0);
        binList2.CopyTo(binTarget, 1);

        var br0 = Assert.IsType<object?[]>(binTarget.Get(0));
        Assert.Equal(new byte[] { 1, 2 }, Assert.IsType<byte[]>(br0[0]));
        Assert.Null(br0[1]);
        var br1 = Assert.IsType<object?[]>(binTarget.Get(1));
        Assert.Equal(new byte[] { 9 }, Assert.IsType<byte[]>(br1[0]));

        // Overflow guard (int.MaxValue must throw ArgumentOutOfRangeException, not IndexOutOfRangeException)
        Assert.Throws<ArgumentOutOfRangeException>(() => list1.CopyTo(target, int.MaxValue));
    }

    [Fact]
    public void ArraySeries_And_StructSeries_CopyTo_ValidateMismatchWidthAndOverflow()
    {
        using var arrSrc = new ArraySeries("a", 2, Int32Series.FromValues("v", [1, 2, 3, 4]));
        using var arrWrongWidth = new ArraySeries("b", 3, Int32Series.FromValues("v", [0, 0, 0, 0, 0, 0]));
        using var arrDst = new ArraySeries("c", 2, new Int32Series("v", 6));
        using var wrongType = new Int32Series("x", 4);

        Assert.Throws<InvalidOperationException>(() => arrSrc.CopyTo(wrongType, 0));
        Assert.Throws<ArgumentException>(() => arrSrc.CopyTo(arrWrongWidth, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => arrSrc.CopyTo(arrDst, int.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() => arrSrc.CopyTo(arrDst, -1));

        using var structSrc = new StructSeries("s", [new Int32Series("f1", [10, 20])]);
        using var structDst = new StructSeries("d", [new Int32Series("f1", 4)]);
        using var structWrongFields = new StructSeries("w", [new Int32Series("f1", 4), new Int32Series("f2", 4)]);

        Assert.Throws<InvalidOperationException>(() => structSrc.CopyTo(wrongType, 0));
        Assert.Throws<ArgumentException>(() => structSrc.CopyTo(structWrongFields, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => structSrc.CopyTo(structDst, int.MaxValue));

        // ValidityMask self-copy at offset 0 is a safe no-op; non-zero throws ArgumentOutOfRangeException
        var mask = new ValidityMask(10);
        mask.SetNull(2);
        mask.SetNull(7);
        mask.CopyRangeTo(mask, 0);
        Assert.True(mask.IsNull(2));
        Assert.True(mask.IsNull(7));
        Assert.True(mask.IsValid(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => mask.CopyRangeTo(mask, 1));
    }

    [Fact]
    public void ListSeries_CopyTo_MiddleOverwrite_Int32Utf8AndBinary_PreservesPrefixSuffixNullsAndAtomicity()
    {
        // 1. Int32Series backing: shorter, longer (grow), empty, null rows, and self-copy
        {
            static ListSeries BuildInt32List(int?[][] rows, bool[]? rowNulls = null)
            {
                var offsets = new int[rows.Length + 1];
                var flat = new System.Collections.Generic.List<int?>();
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = flat.Count;
                    foreach (var v in rows[i]) flat.Add(v);
                }
                offsets[rows.Length] = flat.Count;
                var list = new ListSeries("li", new Int32Series("o", offsets), Int32Series.FromValues("v", flat.ToArray()));
                if (rowNulls != null)
                {
                    for (int i = 0; i < rowNulls.Length; i++)
                        if (rowNulls[i]) list.ValidityMask.SetNull(i);
                }
                return list;
            }

            // (a) Shorter overwrite in middle: [1,2], [3,4], [5,6] -> overwrite row 1 with [99]
            using (var target = BuildInt32List([[1, 2], [3, 4], [5, 6]]))
            using (var shorter = BuildInt32List([[99]]))
            {
                shorter.CopyTo(target, 1);
                Assert.Equal([0, 2, 3, 5], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { 1, 2 }, Assert.IsType<object?[]>(target.Get(0)));
                Assert.Equal(new object?[] { 99 }, Assert.IsType<object?[]>(target.Get(1)));
                Assert.Equal(new object?[] { 5, 6 }, Assert.IsType<object?[]>(target.Get(2)));
            }

            // (b) Longer overwrite (grow path) in middle: [1,2], [3], [5,6] -> overwrite row 1 with [10, null, 30]
            using (var target = BuildInt32List([[1, 2], [3], [5, 6]]))
            using (var longer = BuildInt32List([[10, null, 30]]))
            {
                longer.CopyTo(target, 1);
                Assert.Equal([0, 2, 5, 7], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { 1, 2 }, Assert.IsType<object?[]>(target.Get(0)));
                var mid = Assert.IsType<object?[]>(target.Get(1));
                Assert.Equal(10, mid[0]);
                Assert.Null(mid[1]);
                Assert.Equal(30, mid[2]);
                Assert.Equal(new object?[] { 5, 6 }, Assert.IsType<object?[]>(target.Get(2)));
            }

            // (c) Empty list overwrite (totalSrcValues == 0) in middle: [1,2], [3,4], [5,6] -> overwrite row 1 with []
            using (var target = BuildInt32List([[1, 2], [3, 4], [5, 6]]))
            using (var empty = BuildInt32List([[]]))
            {
                empty.CopyTo(target, 1);
                Assert.Equal([0, 2, 2, 4], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { 1, 2 }, Assert.IsType<object?[]>(target.Get(0)));
                Assert.Empty(Assert.IsType<object?[]>(target.Get(1)));
                Assert.Equal(new object?[] { 5, 6 }, Assert.IsType<object?[]>(target.Get(2)));
            }

            // (d) Null row in source (even with non-empty source offsets) + null suffix row in target
            using (var target = BuildInt32List([[1, 2], [3, 4], [], [7]], [false, false, true, false]))
            using (var srcWithNullRow = BuildInt32List([[99, 100], [88]], [true, false]))
            {
                srcWithNullRow.CopyTo(target, 1);
                Assert.Equal([0, 2, 2, 3, 4], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { 1, 2 }, Assert.IsType<object?[]>(target.Get(0)));
                Assert.Null(target.Get(1));
                Assert.Equal(new object?[] { 88 }, Assert.IsType<object?[]>(target.Get(2)));
                Assert.Equal(new object?[] { 7 }, Assert.IsType<object?[]>(target.Get(3)));
            }

            // (e) Self-copy
            using (var self = BuildInt32List([[1, 2], [3]], [false, true]))
            {
                self.CopyTo(self, 0);
                Assert.Equal(new object?[] { 1, 2 }, Assert.IsType<object?[]>(self.Get(0)));
                Assert.Null(self.Get(1));
            }
        }

        // 2. Utf8StringSeries backing: shorter, longer (grow), empty, nulls, and self-copy
        {
            static ListSeries BuildUtf8List(string?[][] rows, bool[]? rowNulls = null)
            {
                var offsets = new int[rows.Length + 1];
                var flat = new System.Collections.Generic.List<string?>();
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = flat.Count;
                    foreach (var v in rows[i]) flat.Add(v);
                }
                offsets[rows.Length] = flat.Count;
                var list = new ListSeries("lu", new Int32Series("o", offsets), Utf8StringSeries.FromStrings("v", flat.ToArray()));
                if (rowNulls != null)
                {
                    for (int i = 0; i < rowNulls.Length; i++)
                        if (rowNulls[i]) list.ValidityMask.SetNull(i);
                }
                return list;
            }

            // Shorter overwrite in middle
            using (var target = BuildUtf8List([["aa", "bb"], ["cc", "dd"], ["ee", null]]))
            using (var shorter = BuildUtf8List([["x"]]))
            {
                shorter.CopyTo(target, 1);
                Assert.Equal([0, 2, 3, 5], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { "aa", "bb" }, Assert.IsType<object?[]>(target.Get(0)));
                Assert.Equal(new object?[] { "x" }, Assert.IsType<object?[]>(target.Get(1)));
                Assert.Equal(new object?[] { "ee", null }, Assert.IsType<object?[]>(target.Get(2)));
            }

            // Longer overwrite (grow) in middle
            using (var target = BuildUtf8List([["aa"], ["bb"], ["cc", "dd"]]))
            using (var longer = BuildUtf8List([["longer1", null, "longer3"]]))
            {
                longer.CopyTo(target, 1);
                Assert.Equal([0, 1, 4, 6], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { "aa" }, Assert.IsType<object?[]>(target.Get(0)));
                Assert.Equal(new object?[] { "longer1", null, "longer3" }, Assert.IsType<object?[]>(target.Get(1)));
                Assert.Equal(new object?[] { "cc", "dd" }, Assert.IsType<object?[]>(target.Get(2)));
            }

            // Empty overwrite in middle
            using (var target = BuildUtf8List([["aa"], ["bb", "cc"], ["dd"]]))
            using (var empty = BuildUtf8List([[]]))
            {
                empty.CopyTo(target, 1);
                Assert.Equal([0, 1, 1, 2], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { "aa" }, Assert.IsType<object?[]>(target.Get(0)));
                Assert.Empty(Assert.IsType<object?[]>(target.Get(1)));
                Assert.Equal(new object?[] { "dd" }, Assert.IsType<object?[]>(target.Get(2)));
            }

            // Null row + self-copy
            using (var self = BuildUtf8List([["p1"], [null, "p2"]], [false, false]))
            {
                self.CopyTo(self, 0);
                Assert.Equal(new object?[] { "p1" }, Assert.IsType<object?[]>(self.Get(0)));
                Assert.Equal(new object?[] { null, "p2" }, Assert.IsType<object?[]>(self.Get(1)));
            }
        }

        // 3. BinarySeries backing: shorter, longer (grow), empty, nulls, and self-copy
        {
            static ListSeries BuildBinList(byte[]?[][] rows, bool[]? rowNulls = null)
            {
                var offsets = new int[rows.Length + 1];
                var flat = new System.Collections.Generic.List<byte[]?>();
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = flat.Count;
                    foreach (var v in rows[i]) flat.Add(v);
                }
                offsets[rows.Length] = flat.Count;
                var list = new ListSeries("lb", new Int32Series("o", offsets), new BinarySeries("v", flat.ToArray()));
                if (rowNulls != null)
                {
                    for (int i = 0; i < rowNulls.Length; i++)
                        if (rowNulls[i]) list.ValidityMask.SetNull(i);
                }
                return list;
            }

            // Shorter overwrite in middle
            using (var target = BuildBinList([[new byte[] { 1 }, new byte[] { 2 }], [new byte[] { 3 }, new byte[] { 4 }], [new byte[] { 5, 6 }, null]]))
            using (var shorter = BuildBinList([[new byte[] { 9 }]]))
            {
                shorter.CopyTo(target, 1);
                Assert.Equal([0, 2, 3, 5], target.Offsets.Memory.Span.ToArray());
                var s2 = Assert.IsType<object?[]>(target.Get(2));
                Assert.Equal(new byte[] { 5, 6 }, Assert.IsType<byte[]>(s2[0]));
                Assert.Null(s2[1]);
                var s1 = Assert.IsType<object?[]>(target.Get(1));
                Assert.Equal(new byte[] { 9 }, Assert.IsType<byte[]>(s1[0]));
            }

            // Longer overwrite in middle
            using (var target = BuildBinList([[new byte[] { 1 }], [new byte[] { 2 }], [new byte[] { 7, 8 }]]))
            using (var longer = BuildBinList([[new byte[] { 10, 11 }, null, new byte[] { 12 }]]))
            {
                longer.CopyTo(target, 1);
                Assert.Equal([0, 1, 4, 5], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new byte[] { 7, 8 }, Assert.IsType<byte[]>(Assert.IsType<object?[]>(target.Get(2))[0]));
                Assert.Null(Assert.IsType<object?[]>(target.Get(1))[1]);
            }

            // Empty overwrite in middle
            using (var target = BuildBinList([[new byte[] { 1 }], [new byte[] { 2 }, new byte[] { 3 }], [new byte[] { 4 }]]))
            using (var empty = BuildBinList([[]]))
            {
                empty.CopyTo(target, 1);
                Assert.Equal([0, 1, 1, 2], target.Offsets.Memory.Span.ToArray());
                Assert.Empty(Assert.IsType<object?[]>(target.Get(1)));
                Assert.Equal(new byte[] { 4 }, Assert.IsType<byte[]>(Assert.IsType<object?[]>(target.Get(2))[0]));
            }
        }

        // 4. Inner-type mismatch atomicity: target Offsets and ValidityMask must remain completely unmutated on exception
        {
            using var srcInt = new ListSeries("si", new Int32Series("o", [0, 2]), new Int32Series("v", [10, 20]));
            using var dstUtf8 = new ListSeries("du", new Int32Series("o", [0, 1, 2]), Utf8StringSeries.FromStrings("v", ["a", "b"]));
            dstUtf8.ValidityMask.SetNull(0);
            int[] offsetsBefore = dstUtf8.Offsets.Memory.Span.ToArray();
            bool null0Before = dstUtf8.ValidityMask.IsNull(0);
            bool valid1Before = dstUtf8.ValidityMask.IsValid(1);

            Assert.Throws<InvalidOperationException>(() => srcInt.CopyTo(dstUtf8, 0));
            Assert.Equal(offsetsBefore, dstUtf8.Offsets.Memory.Span.ToArray());
            Assert.Equal(null0Before, dstUtf8.ValidityMask.IsNull(0));
            Assert.Equal(valid1Before, dstUtf8.ValidityMask.IsValid(1));
            Assert.Null(dstUtf8.Get(0));
            Assert.Equal(new object?[] { "b" }, Assert.IsType<object?[]>(dstUtf8.Get(1)));

            using var dstFloat = new ListSeries("df", new Int32Series("o", [0, 1, 2]), new Float64Series("v", [1.5, 2.5]));
            int[] floatOffsetsBefore = dstFloat.Offsets.Memory.Span.ToArray();
            Assert.Throws<InvalidOperationException>(() => srcInt.CopyTo(dstFloat, 0));
            Assert.Equal(floatOffsetsBefore, dstFloat.Offsets.Memory.Span.ToArray());
            Assert.Equal(new object?[] { 1.5 }, Assert.IsType<object?[]>(dstFloat.Get(0)));
        }
    }

    [Fact]
    public void ListSeries_NestedListAndCategoricalRevMap_CompatibilityChecksAreRecursiveAndAtomic()
    {
        // 1. Nested ListSeries<ListSeries<Int32Series>> vs ListSeries<ListSeries<Utf8StringSeries>>
        {
            static ListSeries BuildNestedIntList() =>
                new("src",
                    new Int32Series("o_outer", [0, 2]),
                    new ListSeries("inner",
                        new Int32Series("o_inner", [0, 2, 3]),
                        new Int32Series("v", [1, 2, 3])));

            static ListSeries BuildEmptyNestedIntList() =>
                new("src_empty",
                    new Int32Series("o_outer", [0, 0]),
                    new ListSeries("inner",
                        new Int32Series("o_inner", [0]),
                        new Int32Series("v", [])));

            static ListSeries BuildNestedUtf8List() =>
                new("dst",
                    new Int32Series("o_outer", [0, 1]),
                    new ListSeries("inner",
                        new Int32Series("o_inner", [0, 1]),
                        Utf8StringSeries.FromStrings("v", ["keep"])));

            using var srcGrow = BuildNestedIntList();
            using var srcEmpty = BuildEmptyNestedIntList();
            using var dst = BuildNestedUtf8List();
            dst.ValidityMask.SetNull(0);

            var dstOffsetsBefore = dst.Offsets.Memory.Span.ToArray();
            var dstValuesBefore = dst.Values;
            var innerBefore = Assert.IsType<ListSeries>(dst.Values);
            var innerValuesBefore = innerBefore.Values;
            var innerOffsetsBefore = innerBefore.Offsets.Memory.Span.ToArray();

            // Grow / full-overwrite path
            Assert.Throws<InvalidOperationException>(() => srcGrow.CopyTo(dst, 0));
            Assert.Same(dstValuesBefore, dst.Values);
            Assert.Same(innerValuesBefore, Assert.IsType<ListSeries>(dst.Values).Values);
            Assert.Equal(dstOffsetsBefore, dst.Offsets.Memory.Span.ToArray());
            Assert.Equal(innerOffsetsBefore, Assert.IsType<ListSeries>(dst.Values).Offsets.Memory.Span.ToArray());
            Assert.True(dst.ValidityMask.IsNull(0));
            Assert.Equal("keep", Assert.IsType<Utf8StringSeries>(innerValuesBefore).GetString(0));

            // Empty overwrite path
            Assert.Throws<InvalidOperationException>(() => srcEmpty.CopyTo(dst, 0));
            Assert.Same(dstValuesBefore, dst.Values);
            Assert.Same(innerValuesBefore, Assert.IsType<ListSeries>(dst.Values).Values);
            Assert.Equal(dstOffsetsBefore, dst.Offsets.Memory.Span.ToArray());
            Assert.True(dst.ValidityMask.IsNull(0));
        }

        // 2. CategoricalSeries RevMap mismatch atomicity inside ListSeries (and matching RevMap succeeds)
        {
            using var catSrcMismatch = new ListSeries("sc",
                new Int32Series("o", [0, 1]),
                new CategoricalSeries("cv", [0u], ["X"]));
            using var catDst = new ListSeries("dc",
                new Int32Series("o", [0, 1]),
                new CategoricalSeries("cv", [0u], ["A"]));

            var offsetsBefore = catDst.Offsets.Memory.Span.ToArray();
            var valuesBefore = catDst.Values;
            Assert.Throws<InvalidOperationException>(() => catSrcMismatch.CopyTo(catDst, 0));
            Assert.Same(valuesBefore, catDst.Values);
            Assert.Equal(offsetsBefore, catDst.Offsets.Memory.Span.ToArray());
            Assert.Equal(new object?[] { "A" }, Assert.IsType<object?[]>(catDst.Get(0)));

            // Matching RevMap succeeds
            using var catSrcMatch = new ListSeries("scm",
                new Int32Series("o", [0, 2]),
                new CategoricalSeries("cv", [1u, 0u], ["A", "B"]));
            using var catDstMatch = new ListSeries("dcm",
                new Int32Series("o", [0, 1]),
                new CategoricalSeries("cv", [0u], ["A", "B"]));
            catSrcMatch.CopyTo(catDstMatch, 0);
            Assert.Equal(new object?[] { "B", "A" }, Assert.IsType<object?[]>(catDstMatch.Get(0)));
        }
    }

    [Fact]
    public void StructSeries_And_ArraySeries_CopyTo_UpfrontAtomicity_OnNestedMismatch()
    {
        // Field 0 matches (Int32Series), Field 1 mismatches (ListSeries<Int32> vs ListSeries<Utf8> or Categorical RevMap mismatch)
        using var src = new StructSeries("s", [
            new Int32Series("f0", [999]),
            new ListSeries("f1", new Int32Series("o", [0, 1]), new Int32Series("v", [42]))
        ]);
        using var dst = new StructSeries("d", [
            new Int32Series("f0", [1, 2]),
            new ListSeries("f1", new Int32Series("o", [0, 1, 2]), Utf8StringSeries.FromStrings("v", ["a", "b"]))
        ]);
        dst.ValidityMask.SetNull(1);

        Assert.Throws<InvalidOperationException>(() => src.CopyTo(dst, 1));
        // Field 0 and ValidityMask must NOT be partially mutated!
        Assert.Equal(1, dst.Fields[0].Get(0));
        Assert.Equal(2, dst.Fields[0].Get(1));
        Assert.True(dst.ValidityMask.IsValid(0));
        Assert.True(dst.ValidityMask.IsNull(1));

        // Categorical RevMap mismatch in field 1 must also be atomic
        using var srcCat = new StructSeries("sc", [
            new Int32Series("f0", [777]),
            new CategoricalSeries("f1", [0u], ["X"])
        ]);
        using var dstCat = new StructSeries("dc", [
            new Int32Series("f0", [10, 20]),
            new CategoricalSeries("f1", [0u, 0u], ["A"])
        ]);
        Assert.Throws<InvalidOperationException>(() => srcCat.CopyTo(dstCat, 0));
        Assert.Equal(10, dstCat.Fields[0].Get(0));
        Assert.Equal("A", dstCat.Fields[1].Get(0));

        // ArraySeries nested ListSeries inner mismatch atomicity
        using var arrSrc = new ArraySeries("as", 1, new ListSeries("l", new Int32Series("o", [0, 1]), new Int32Series("v", [1])));
        using var arrDst = new ArraySeries("ad", 1, new ListSeries("l", new Int32Series("o", [0, 1, 2]), Utf8StringSeries.FromStrings("v", ["p", "q"])));
        arrDst.ValidityMask.SetNull(0);
        Assert.Throws<InvalidOperationException>(() => arrSrc.CopyTo(arrDst, 1));
        Assert.True(arrDst.ValidityMask.IsNull(0));
        Assert.Equal(new object?[] { "q" }, Assert.IsType<object?[]>(arrDst.Values.Get(1)));
    }

    [Fact]
    public void ArraySeries_And_StructSeries_CopyTo_SupportsUtf8AndBinary_WithLargePayloadsAndNulls()
    {
        string long1 = new string('L', 2500);
        string long2 = new string('M', 1800);
        byte[] bigBin1 = new byte[2200];
        bigBin1[0] = 0xAA;
        bigBin1[^1] = 0x55;

        // 1. Direct ArraySeries<Utf8StringSeries>.CopyTo middle overwrite
        using (var src = new ArraySeries("a", 2, Utf8StringSeries.FromStrings("v", [long1, null])))
        using (var dst = new ArraySeries("d", 2, Utf8StringSeries.FromStrings("v", ["p0", "p1", "old0", "old1", "s0", long2])))
        {
            dst.ValidityMask.SetNull(2);
            src.CopyTo(dst, 1);

            Assert.Equal(["p0", "p1"], Assert.IsType<object?[]>(dst.Get(0)));
            var row1 = Assert.IsType<object?[]>(dst.Get(1));
            Assert.Equal(long1, row1[0]);
            Assert.Null(row1[1]);
            Assert.Null(dst.Get(2));
            Assert.Equal("s0", dst.Values.Get(4));
            Assert.Equal(long2, dst.Values.Get(5));
        }

        // 2. Direct ArraySeries<BinarySeries>.CopyTo middle overwrite
        using (var src = new ArraySeries("a", 2, new BinarySeries("v", [bigBin1, null])))
        using (var dst = new ArraySeries("d", 2, new BinarySeries("v", [new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 }, new byte[] { 5 }, new byte[] { 6 }])))
        {
            src.CopyTo(dst, 1);

            Assert.Equal(new byte[] { 1 }, Assert.IsType<byte[]>(Assert.IsType<object?[]>(dst.Get(0))[0]));
            var mid = Assert.IsType<object?[]>(dst.Get(1));
            Assert.Equal(bigBin1, Assert.IsType<byte[]>(mid[0]));
            Assert.Null(mid[1]);
            Assert.Equal(new byte[] { 6 }, Assert.IsType<byte[]>(Assert.IsType<object?[]>(dst.Get(2))[1]));
        }

        // 3. Direct StructSeries<Int32Series, Utf8StringSeries, BinarySeries>.CopyTo middle overwrite
        using (var src = new StructSeries("s", [
            Int32Series.FromValues("id", [100, null]),
            Utf8StringSeries.FromStrings("txt", [long1, null]),
            new BinarySeries("bin", [null, bigBin1])
        ]))
        using (var dst = new StructSeries("d", [
            Int32Series.FromValues("id", [1, 2, 3, 4]),
            Utf8StringSeries.FromStrings("txt", ["p0", "o1", "o2", long2]),
            new BinarySeries("bin", [new byte[] { 9 }, new byte[] { 8 }, new byte[] { 7 }, new byte[] { 6 }])
        ]))
        {
            src.ValidityMask.SetNull(1);
            src.CopyTo(dst, 1);

            var r0 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(dst.Get(0));
            Assert.Equal(1, r0["id"]);
            Assert.Equal("p0", r0["txt"]);
            Assert.Equal(new byte[] { 9 }, Assert.IsType<byte[]>(r0["bin"]));

            var r1 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(dst.Get(1));
            Assert.Equal(100, r1["id"]);
            Assert.Equal(long1, r1["txt"]);
            Assert.Null(r1["bin"]);

            Assert.Null(dst.Get(2));

            var r3 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(dst.Get(3));
            Assert.Equal(4, r3["id"]);
            Assert.Equal(long2, r3["txt"]);
            Assert.Equal(bigBin1, Assert.IsType<byte[]>(dst.Fields[2].Get(2)));
        }

        // 4. Nested ArraySeries<ArraySeries<Utf8StringSeries>>.CopyTo
        using (var srcNested = new ArraySeries("na", 1, new ArraySeries("inner", 2, Utf8StringSeries.FromStrings("v", [long1, "mid"]))))
        using (var dstNested = new ArraySeries("nd", 1, new ArraySeries("inner", 2, Utf8StringSeries.FromStrings("v", ["a", "b", "c", "d", "e", long2]))))
        {
            srcNested.CopyTo(dstNested, 1);
            Assert.Equal(["a", "b"], Assert.IsType<object?[]>(Assert.IsType<object?[]>(dstNested.Get(0))[0]));
            Assert.Equal([long1, "mid"], Assert.IsType<object?[]>(Assert.IsType<object?[]>(dstNested.Get(1))[0]));
            Assert.Equal(["e", long2], Assert.IsType<object?[]>(Assert.IsType<object?[]>(dstNested.Get(2))[0]));
        }
    }

    [Fact]
    public void ListSeries_NestedArrayAndStructWithUtf8AndBinary_MiddleOverwritesAndLargeStrings()
    {
        string big1 = new string('A', 2200);
        string big2 = new string('B', 2400);
        string big3 = new string('C', 2600);
        byte[] bin1 = new byte[1500];
        bin1[0] = 7;
        bin1[^1] = 9;
        byte[] bin2 = new byte[1700];
        bin2[0] = 3;
        bin2[^1] = 4;

        // 1. ListSeries<ArraySeries<Utf8StringSeries>>: grow, shrink, empty, nulls, >1KB strings
        {
            static ListSeries BuildListArrayUtf8((string? a, string? b)[][] rows, bool[]? rowNulls = null)
            {
                var offsets = new int[rows.Length + 1];
                var flat = new System.Collections.Generic.List<string?>();
                int totalPairs = 0;
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = totalPairs;
                    foreach (var pair in rows[i])
                    {
                        flat.Add(pair.a);
                        flat.Add(pair.b);
                        totalPairs++;
                    }
                }
                offsets[rows.Length] = totalPairs;
                var list = new ListSeries("lau", new Int32Series("o", offsets), new ArraySeries("arr", 2, Utf8StringSeries.FromStrings("v", flat.ToArray())));
                if (rowNulls != null)
                {
                    for (int i = 0; i < rowNulls.Length; i++)
                        if (rowNulls[i]) list.ValidityMask.SetNull(i);
                }
                return list;
            }

            // Grow in middle with >1KB strings in prefix, src, and suffix
            using (var target = BuildListArrayUtf8([[("p", big1)], [("old1", "old2")], [(big2, null)]]))
            using (var grow = BuildListArrayUtf8([[("m1", big3), (null, "m2")]]))
            {
                grow.CopyTo(target, 1);
                Assert.Equal([0, 1, 3, 4], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(new object?[] { "p", big1 }, Assert.IsType<object?[]>(Assert.IsType<object?[]>(target.Get(0))[0]));
                var mid = Assert.IsType<object?[]>(target.Get(1));
                Assert.Equal(2, mid.Length);
                Assert.Equal(new object?[] { "m1", big3 }, Assert.IsType<object?[]>(mid[0]));
                Assert.Equal(new object?[] { null, "m2" }, Assert.IsType<object?[]>(mid[1]));
                Assert.Equal(new object?[] { big2, null }, Assert.IsType<object?[]>(Assert.IsType<object?[]>(target.Get(2))[0]));
            }

            // Shrink + empty overwrite + null row preservation
            using (var target = BuildListArrayUtf8([[("a", "b")], [("c", "d"), ("e", "f")], [], [("g", "h")]], [false, false, true, false]))
            using (var empty = BuildListArrayUtf8([[]]))
            {
                empty.CopyTo(target, 1);
                Assert.Equal([0, 1, 1, 1, 2], target.Offsets.Memory.Span.ToArray());
                Assert.Empty(Assert.IsType<object?[]>(target.Get(1)));
                Assert.Null(target.Get(2));
                Assert.Equal(new object?[] { "g", "h" }, Assert.IsType<object?[]>(Assert.IsType<object?[]>(target.Get(3))[0]));
            }
        }

        // 2. ListSeries<ArraySeries<BinarySeries>>: grow, shrink, empty, >1KB blobs
        {
            static ListSeries BuildListArrayBin((byte[]? a, byte[]? b)[][] rows)
            {
                var offsets = new int[rows.Length + 1];
                var flat = new System.Collections.Generic.List<byte[]?>();
                int totalPairs = 0;
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = totalPairs;
                    foreach (var pair in rows[i])
                    {
                        flat.Add(pair.a);
                        flat.Add(pair.b);
                        totalPairs++;
                    }
                }
                offsets[rows.Length] = totalPairs;
                return new ListSeries("lab", new Int32Series("o", offsets), new ArraySeries("arr", 2, new BinarySeries("v", flat.ToArray())));
            }

            using (var target = BuildListArrayBin([[(bin1, null)], [(new byte[] { 1 }, new byte[] { 2 })], [(new byte[] { 5 }, bin2)]]))
            using (var grow = BuildListArrayBin([[(new byte[] { 9 }, new byte[] { 10 }), (bin2, bin1)]]))
            {
                grow.CopyTo(target, 1);
                Assert.Equal([0, 1, 3, 4], target.Offsets.Memory.Span.ToArray());
                Assert.Equal(bin1, Assert.IsType<byte[]>(Assert.IsType<object?[]>(Assert.IsType<object?[]>(target.Get(0))[0])[0]));
                Assert.Equal(bin1, Assert.IsType<byte[]>(Assert.IsType<object?[]>(Assert.IsType<object?[]>(target.Get(1))[1])[1]));
                Assert.Equal(bin2, Assert.IsType<byte[]>(Assert.IsType<object?[]>(Assert.IsType<object?[]>(target.Get(2))[0])[1]));
            }
        }

        // 3. ListSeries<StructSeries<Int32Series, Utf8StringSeries>>: grow, shrink, empty, nulls, >1KB strings
        {
            static ListSeries BuildListStructUtf8((int? id, string? txt)[][] rows, bool[]? rowNulls = null)
            {
                var offsets = new int[rows.Length + 1];
                var ids = new System.Collections.Generic.List<int?>();
                var txts = new System.Collections.Generic.List<string?>();
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = ids.Count;
                    foreach (var item in rows[i])
                    {
                        ids.Add(item.id);
                        txts.Add(item.txt);
                    }
                }
                offsets[rows.Length] = ids.Count;
                var st = new StructSeries("st", [Int32Series.FromValues("id", ids.ToArray()), Utf8StringSeries.FromStrings("txt", txts.ToArray())]);
                var list = new ListSeries("lsu", new Int32Series("o", offsets), st);
                if (rowNulls != null)
                {
                    for (int i = 0; i < rowNulls.Length; i++)
                        if (rowNulls[i]) list.ValidityMask.SetNull(i);
                }
                return list;
            }

            // Grow in middle with >1KB strings
            using (var target = BuildListStructUtf8([[(1, big1)], [(2, "old")], [(3, big2)]]))
            using (var grow = BuildListStructUtf8([[(10, big3), (null, "second")]]))
            {
                grow.CopyTo(target, 1);
                Assert.Equal([0, 1, 3, 4], target.Offsets.Memory.Span.ToArray());
                var r0 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(Assert.IsType<object?[]>(target.Get(0))[0]);
                Assert.Equal(1, r0["id"]);
                Assert.Equal(big1, r0["txt"]);

                var mid = Assert.IsType<object?[]>(target.Get(1));
                Assert.Equal(2, mid.Length);
                var m0 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(mid[0]);
                var m1 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(mid[1]);
                Assert.Equal(10, m0["id"]);
                Assert.Equal(big3, m0["txt"]);
                Assert.Null(m1["id"]);
                Assert.Equal("second", m1["txt"]);

                var r2 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(Assert.IsType<object?[]>(target.Get(2))[0]);
                Assert.Equal(3, r2["id"]);
                Assert.Equal(big2, r2["txt"]);
            }

            // Empty overwrite in middle
            using (var target = BuildListStructUtf8([[(1, "a")], [(2, "b"), (3, "c")], [(4, "d")]], [false, false, false]))
            using (var empty = BuildListStructUtf8([[]]))
            {
                empty.CopyTo(target, 1);
                Assert.Equal([0, 1, 1, 2], target.Offsets.Memory.Span.ToArray());
                Assert.Empty(Assert.IsType<object?[]>(target.Get(1)));
                var r2 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(Assert.IsType<object?[]>(target.Get(2))[0]);
                Assert.Equal(4, r2["id"]);
                Assert.Equal("d", r2["txt"]);
            }
        }

        // 4. ListSeries<StructSeries<Int32Series, BinarySeries>>: grow & shrink with >1KB binary blobs
        {
            static ListSeries BuildListStructBin((int? id, byte[]? bin)[][] rows)
            {
                var offsets = new int[rows.Length + 1];
                var ids = new System.Collections.Generic.List<int?>();
                var bins = new System.Collections.Generic.List<byte[]?>();
                for (int i = 0; i < rows.Length; i++)
                {
                    offsets[i] = ids.Count;
                    foreach (var item in rows[i])
                    {
                        ids.Add(item.id);
                        bins.Add(item.bin);
                    }
                }
                offsets[rows.Length] = ids.Count;
                var st = new StructSeries("st", [Int32Series.FromValues("id", ids.ToArray()), new BinarySeries("bin", bins.ToArray())]);
                return new ListSeries("lsb", new Int32Series("o", offsets), st);
            }

            using (var target = BuildListStructBin([[(1, bin1)], [(2, new byte[] { 1 }), (3, new byte[] { 2 })], [(4, bin2)]]))
            using (var grow = BuildListStructBin([[(10, bin2), (20, null), (30, bin1)]]))
            {
                grow.CopyTo(target, 1);
                Assert.Equal([0, 1, 4, 5], target.Offsets.Memory.Span.ToArray());
                var r0 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(Assert.IsType<object?[]>(target.Get(0))[0]);
                Assert.Equal(bin1, Assert.IsType<byte[]>(r0["bin"]));
                var mid = Assert.IsType<object?[]>(target.Get(1));
                Assert.Equal(3, mid.Length);
                Assert.Null(Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(mid[1])["bin"]);
                Assert.Equal(bin1, Assert.IsType<byte[]>(Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(mid[2])["bin"]));
                var r2 = Assert.IsType<System.Collections.Generic.Dictionary<string, object?>>(Assert.IsType<object?[]>(target.Get(2))[0]);
                Assert.Equal(bin2, Assert.IsType<byte[]>(r2["bin"]));
            }
        }
    }
}
