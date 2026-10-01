namespace Glacier.Polaris.Tests;

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.IO;
using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;
using Xunit;

public class ArrowStorageSynthesisTests
{
    [Fact]
    public void All16Types_RoundTrip_ZeroDataLoss_NullMasksPreserved()
    {
        const int rowCount = 100;
        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var i8 = new sbyte?[rowCount];
        var u8 = new byte?[rowCount];
        var i16 = new short?[rowCount];
        var u16 = new ushort?[rowCount];
        var i32 = new int?[rowCount];
        var u32 = new uint?[rowCount];
        var i64 = new long?[rowCount];
        var u64 = new ulong?[rowCount];
        var f32 = new float?[rowCount];
        var f64 = new double?[rowCount];
        var bl = new bool?[rowCount];
        var utf8 = new string?[rowCount];
        var bin = new byte[]?[rowCount];
        var date = new int?[rowCount];
        var dt = new long?[rowCount];
        var dur = new long?[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            bool isNull = (i % 5 == 0); // null every 5th row
            if (isNull) continue;

            i8[i] = (sbyte)(i - 50);
            u8[i] = (byte)(i * 2);
            i16[i] = (short)(i * 10 - 200);
            u16[i] = (ushort)(i * 100);
            i32[i] = i * 1000 - 50000;
            u32[i] = (uint)(i * 10000);
            i64[i] = (long)i * 1_000_000_000L;
            u64[i] = (ulong)i * 2_000_000_000UL;
            f32[i] = i * 1.5f;
            f64[i] = i * 3.141592653589793;
            bl[i] = (i % 2 == 1);
            utf8[i] = $"str_val_{i}";
            bin[i] = new byte[] { (byte)i, (byte)(i + 1), (byte)(i + 2) };
            date[i] = 19000 + i; // days since 1970-01-01
            dt[i] = 1700000000_000_000_000L + (long)i * 1_000_000_000L; // ns timestamp
            dur[i] = (long)i * 60_000_000_000L; // ns duration
        }

        var df = new DataFrame(new ISeries[]
        {
            Int8Series.FromValues("c_i8", i8),
            UInt8Series.FromValues("c_u8", u8),
            Int16Series.FromValues("c_i16", i16),
            UInt16Series.FromValues("c_u16", u16),
            Int32Series.FromValues("c_i32", i32),
            UInt32Series.FromValues("c_u32", u32),
            Int64Series.FromValues("c_i64", i64),
            UInt64Series.FromValues("c_u64", u64),
            Float32Series.FromValues("c_f32", f32),
            Float64Series.FromValues("c_f64", f64),
            BooleanSeries.FromValues("c_bl", bl),
            Utf8StringSeries.FromStrings("c_utf8", utf8),
            new BinarySeries("c_bin", bin),
            DateSeries.FromValues("c_date", date),
            DatetimeSeries.FromValues("c_dt", dt),
            DurationSeries.FromValues("c_dur", dur)
        });

        // 1. Serialize to Arrow IPC stream
        using var ms = new MemoryStream();
        df.ToArrowIpc(ms, leaveOpen: true);
        Assert.True(ms.Length > 0);

        // 2. Deserialize from Arrow IPC stream
        ms.Position = 0;
        var roundtripped = DataFrame.FromArrowIpc(ms);

        // 3. Verify schema and dimensions
        Assert.Equal(rowCount, roundtripped.RowCount);
        Assert.Equal(16, roundtripped.Columns.Count);

        // 4. Verify all 16 columns and data fidelity
        for (int i = 0; i < rowCount; i++)
        {
            bool expectedNull = (i % 5 == 0);

            for (int c = 0; c < 16; c++)
            {
                var col = roundtripped.Columns[c];
                Assert.Equal(expectedNull, col.ValidityMask.IsNull(i));
            }

            if (!expectedNull)
            {
                Assert.Equal(i8[i], (sbyte)roundtripped.GetColumn("c_i8").Get(i)!);
                Assert.Equal(u8[i], (byte)roundtripped.GetColumn("c_u8").Get(i)!);
                Assert.Equal(i16[i], (short)roundtripped.GetColumn("c_i16").Get(i)!);
                Assert.Equal(u16[i], (ushort)roundtripped.GetColumn("c_u16").Get(i)!);
                Assert.Equal(i32[i], (int)roundtripped.GetColumn("c_i32").Get(i)!);
                Assert.Equal(u32[i], (uint)roundtripped.GetColumn("c_u32").Get(i)!);
                Assert.Equal(i64[i], (long)roundtripped.GetColumn("c_i64").Get(i)!);
                Assert.Equal(u64[i], (ulong)roundtripped.GetColumn("c_u64").Get(i)!);
                Assert.Equal(f32[i], (float)roundtripped.GetColumn("c_f32").Get(i)!);
                Assert.Equal(f64[i]!.Value, (double)roundtripped.GetColumn("c_f64").Get(i)!, 9);
                Assert.Equal(bl[i], (bool)roundtripped.GetColumn("c_bl").Get(i)!);
                Assert.Equal(utf8[i], (string)roundtripped.GetColumn("c_utf8").Get(i)!);

                var roundtripBin = (byte[])roundtripped.GetColumn("c_bin").Get(i)!;
                Assert.Equal(bin[i], roundtripBin);

                Assert.Equal(date[i], (int)roundtripped.GetColumn("c_date").Get(i)!);
                Assert.Equal(dt[i], (long)roundtripped.GetColumn("c_dt").Get(i)!);
                Assert.Equal(dur[i], (long)roundtripped.GetColumn("c_dur").Get(i)!);
            }
        }
    }

    [Fact]
    public void ValidityMask_LittleEndian_BitBlit_RoundtripPreservation()
    {
        int[] testLengths = { 1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 1000 };

        foreach (int len in testLengths)
        {
            var originalMask = new ValidityMask(len, setAllValid: true);
            var rng = new Random(len * 37);

            // Set random bits as null
            for (int i = 0; i < len; i++)
            {
                if (rng.Next(3) == 0) // ~33% nulls
                {
                    originalMask.SetNull(i);
                }
            }

            // Get NullBitmap memory (little-endian byte array)
            var nullBitmap = originalMask.GetNullBitmapMemory();

            // Reconstruct mask via bit-blit
            var restoredMask = ValidityMask.FromNullBitmap(len, nullBitmap.Span);

            Assert.Equal(originalMask.NullCount, restoredMask.NullCount);
            Assert.Equal(originalMask.HasNulls, restoredMask.HasNulls);

            for (int i = 0; i < len; i++)
            {
                Assert.Equal(originalMask.IsValid(i), restoredMask.IsValid(i));
                Assert.Equal(originalMask.IsNull(i), restoredMask.IsNull(i));
            }
        }
    }

    [Fact]
    public void NativeMemoryOwner_64ByteAlignment_Allocation()
    {
        using var owner = new NativeMemoryOwner<int>(1024);
        Assert.Equal(1024, owner.Length);

        unsafe
        {
            fixed (int* ptr = owner.Span)
            {
                long addr = (long)ptr;
                Assert.True(addr % 64 == 0, $"NativeMemoryOwner pointer 0x{addr:X} must be 64-byte aligned");
            }
        }

        // Test writing and reading
        for (int i = 0; i < owner.Length; i++)
        {
            owner.Span[i] = i * 42;
        }

        for (int i = 0; i < owner.Length; i++)
        {
            Assert.Equal(i * 42, owner.Memory.Span[i]);
        }
    }

    [Fact]
    public void LargeScale_1MRows_ZeroCopyRoundTrip_Throughput()
    {
        const int rowCount = 1_000_000;
        var i32Owner = new NativeMemoryOwner<int>(rowCount);
        var f64Owner = new NativeMemoryOwner<double>(rowCount);
        var mask = new ValidityMask(rowCount, setAllValid: true);

        // Fill data directly into unmanaged memory
        var i32Span = i32Owner.Span;
        var f64Span = f64Owner.Span;
        for (int i = 0; i < rowCount; i++)
        {
            i32Span[i] = i;
            f64Span[i] = i * 0.5;
        }

        // Set sparse nulls (every 1000th row)
        for (int i = 0; i < rowCount; i += 1000)
        {
            mask.SetNull(i);
        }

        var col1 = new Int32Series("id", rowCount, i32Owner, mask);
        var col2 = new Float64Series("val", rowCount, f64Owner, mask);
        var df = new DataFrame(new ISeries[] { col1, col2 });

        // Stream to MemoryStream
        using var ms = new MemoryStream(rowCount * 16);
        var sw = Stopwatch.StartNew();
        df.ToArrowIpc(ms, leaveOpen: true);
        sw.Stop();

        long bytesWritten = ms.Length;
        double serializeSec = sw.Elapsed.TotalSeconds;
        double serializeThroughput = rowCount / Math.Max(serializeSec, 0.000001);

        // Stream back from MemoryStream
        ms.Position = 0;
        sw.Restart();
        var roundtripped = DataFrame.FromArrowIpc(ms);
        sw.Stop();

        double deserializeSec = sw.Elapsed.TotalSeconds;
        double deserializeThroughput = rowCount / Math.Max(deserializeSec, 0.000001);

        // Verify dimensions and data
        Assert.Equal(rowCount, roundtripped.RowCount);
        Assert.Equal(2, roundtripped.Columns.Count);

        var rI32 = roundtripped.GetColumn("id");
        var rF64 = roundtripped.GetColumn("val");

        // Spot check boundaries and arbitrary points
        int[] checkIndices = { 0, 1, 999, 1000, 1001, 500000, 999999 };
        foreach (int idx in checkIndices)
        {
            bool expectedNull = (idx % 1000 == 0);
            Assert.Equal(expectedNull, rI32.ValidityMask.IsNull(idx));
            Assert.Equal(expectedNull, rF64.ValidityMask.IsNull(idx));

            if (!expectedNull)
            {
                Assert.Equal(idx, (int)rI32.Get(idx)!);
                Assert.Equal(idx * 0.5, (double)rF64.Get(idx)!);
            }
        }

        // Output verified throughput metrics
        Assert.True(bytesWritten > 0);
        Assert.True(serializeThroughput > 0);
        Assert.True(deserializeThroughput > 0);
    }

    [Fact]
    public void ArrowStreamWriter_HotPath_ZeroHeapAllocation()
    {
        const int rowCount = 100_000;
        var owner = new NativeMemoryOwner<int>(rowCount);
        for (int i = 0; i < rowCount; i++) owner.Span[i] = i;

        var mask = new ValidityMask(rowCount, setAllValid: true);
        var col = new Int32Series("col0", rowCount, owner, mask);
        var df = new DataFrame(new ISeries[] { col });
        var batch = df.ToGlacierRecordBatch();

        // Warm up JIT
        using (var warmupWriter = new ArrowStreamWriter(Stream.Null, leaveOpen: true))
        {
            warmupWriter.WriteSchema(batch.Schema);
            warmupWriter.WriteRecordBatch(batch);
        }

        // Measure allocations on batch streaming hot path
        using (var writer = new ArrowStreamWriter(Stream.Null, leaveOpen: true))
        {
            GC.Collect(2, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
            long allocBefore = GC.GetAllocatedBytesForCurrentThread();

            writer.WriteRecordBatch(batch);

            long allocAfter = GC.GetAllocatedBytesForCurrentThread();
            long delta = allocAfter - allocBefore;

            // ArrowStreamWriter streams unmanaged memory spans directly into the output stream
            // without allocating intermediate MemoryStream or ToArray() byte buffers.
            Assert.True(delta == 0, $"Expected 0 bytes allocated on WriteRecordBatch hot path, but got {delta} bytes");
        }
    }
}
