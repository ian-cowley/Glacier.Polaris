#if USE_LOCAL_GLACIER_FORMATS
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Glacier.Formats.Arrow;
using Glacier.Formats.Parquet;
using Glacier.Polaris.Data;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Xunit;

using FormatsParquetReader = Glacier.Formats.Parquet.ParquetReader;
using FormatsParquetWriter = Glacier.Formats.Parquet.ParquetWriter;
using PolarisParquetReader = Glacier.Polaris.IO.ParquetReader;

namespace Glacier.Polaris.Tests;

public class ParquetFormatsInteropTests
{
    [Theory]
    [InlineData(CompressionCodec.Uncompressed)]
    [InlineData(CompressionCodec.Snappy)]
    public async Task GlacierFormats_Write_To_ParquetNet_And_PolarisParquetReader_Roundtrips(CompressionCodec codec)
    {
        var schema = new ArrowSchema([
            new ArrowField("flag", ArrowType.Boolean, isNullable: false),
            new ArrowField("id", ArrowType.Int32, isNullable: false),
            new ArrowField("big", ArrowType.Int64, isNullable: false),
            new ArrowField("ratio", ArrowType.Float, isNullable: false),
            new ArrowField("score", ArrowType.Double, isNullable: false),
            new ArrowField("dept", ArrowType.Utf8, isNullable: true),
            new ArrowField("opt_val", ArrowType.Int32, isNullable: true)
        ]);

        var b1 = MakeBatch(schema,
            [true, false, true],
            [1, 2, 3],
            [10000000001L, 10000000002L, 10000000003L],
            [1.5f, 2.5f, 3.5f],
            [10.25, 20.5, 30.75],
            ["core", null, "search"],
            [10, null, 30]);

        var b2 = MakeBatch(schema,
            [false, true],
            [4, 5],
            [10000000004L, 10000000005L],
            [4.5f, 5.5f],
            [40.25, 50.5],
            ["infra", "ml"],
            [null, 50]);

        byte[] parquetBytes;
        using (var ms = new MemoryStream())
        {
            FormatsParquetWriter.WriteStream(ms, [b1, b2], codec);
            parquetBytes = ms.ToArray();
        }

        // 1. Direct Parquet.Net reader check (verifies metadata, CreatedBy, schema, and values)
        using (var readMs = new MemoryStream(parquetBytes, writable: false))
        using (var pqReader = await Parquet.ParquetReader.CreateAsync(readMs))
        {
            Assert.Equal("Glacier.Formats 1.0", pqReader.Metadata?.CreatedBy);
            Assert.Equal(2, pqReader.RowGroupCount);

            var dataFields = pqReader.Schema.GetDataFields();
            Assert.Equal(7, dataFields.Length);

            using var rg0 = pqReader.OpenRowGroupReader(0);
            Assert.Equal(3, rg0.RowCount);
            var idCol0 = await rg0.ReadColumnAsync(dataFields[1]);
            Assert.Equal(new[] { 1, 2, 3 }, (int[])idCol0.Data);
            var deptCol0 = await rg0.ReadColumnAsync(dataFields[5]);
            Assert.Equal(new string?[] { "core", null, "search" }, (string?[])deptCol0.Data);
            var optCol0 = await rg0.ReadColumnAsync(dataFields[6]);
            Assert.Equal(new int?[] { 10, null, 30 }, (int?[])optCol0.Data);

            using var rg1 = pqReader.OpenRowGroupReader(1);
            Assert.Equal(2, rg1.RowCount);
            var flagCol1 = await rg1.ReadColumnAsync(dataFields[0]);
            Assert.Equal(new[] { false, true }, (bool[])flagCol1.Data);
            var optCol1 = await rg1.ReadColumnAsync(dataFields[6]);
            Assert.Equal(new int?[] { null, 50 }, (int?[])optCol1.Data);
        }

        // 2. Polaris IO.ParquetReader check
        string tempPath = Path.Combine(Path.GetTempPath(), $"glacier_interop_{Guid.NewGuid():N}.parquet");
        try
        {
            await File.WriteAllBytesAsync(tempPath, parquetBytes);
            var frames = new List<DataFrame>();
            await foreach (var df in new PolarisParquetReader(tempPath).ReadAsync())
            {
                frames.Add(df);
            }

            using var fullDf = DataFrame.Concat(frames);
            Assert.Equal(5, fullDf.RowCount);
            Assert.Equal(1, fullDf.GetColumn("id").Get(0));
            Assert.Equal(5, fullDf.GetColumn("id").Get(4));
            Assert.Equal("core", fullDf.GetColumn("dept").Get(0));
            Assert.Null(fullDf.GetColumn("dept").Get(1));
            Assert.Equal("ml", fullDf.GetColumn("dept").Get(4));
            Assert.Null(fullDf.GetColumn("opt_val").Get(3));
            Assert.Equal(50, fullDf.GetColumn("opt_val").Get(4));
        }
        finally
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }
    }

    [Theory]
    [InlineData(CompressionMethod.None)]
    [InlineData(CompressionMethod.Snappy)]
    public async Task ParquetNet_WritePlain_To_GlacierFormatsParquetReader_Roundtrips(CompressionMethod compression)
    {
        var flagField = new DataField<bool>("flag");
        var idField = new DataField<int>("id");
        var optField = new DataField<int?>("opt_num");
        var scoreField = new DataField<double>("score");
        var nameField = new DataField<string>("name", nullable: true);
        var pqSchema = new ParquetSchema(flagField, idField, optField, scoreField, nameField);

        var options = new ParquetOptions
        {
            UseDictionaryEncoding = false,
            UseDeltaBinaryPackedEncoding = false
        };
        using var ms = new MemoryStream();
        using (var writer = await Parquet.ParquetWriter.CreateAsync(pqSchema, ms, options))
        {
            writer.CompressionMethod = compression;
            using var rg = writer.CreateRowGroup();
            await rg.WriteColumnAsync(new DataColumn(flagField, new[] { true, false, true, false }));
            await rg.WriteColumnAsync(new DataColumn(idField, new[] { 10, 20, 30, 40 }));
            await rg.WriteColumnAsync(new DataColumn(optField, new int?[] { 100, null, 300, null }));
            await rg.WriteColumnAsync(new DataColumn(scoreField, new[] { 1.25, 2.5, 3.75, 5.0 }));
            await rg.WriteColumnAsync(new DataColumn(nameField, new string?[] { "alpha", "beta", null, "delta" }));
        }

        var batch = FormatsParquetReader.ReadBytes(ms.ToArray());
        Assert.Equal(4, batch.RowCount);
        Assert.Equal(5, batch.ColumnCount);

        Assert.True(batch.Column(0).GetBoolean(0));
        Assert.False(batch.Column(0).GetBoolean(1));
        Assert.Equal(20, batch.Column(1).GetInt32(1));
        Assert.Equal(100, batch.Column(2).GetInt32(0));
        Assert.True(batch.Column(2).IsNull(1));
        Assert.Equal(300, batch.Column(2).GetInt32(2));
        Assert.True(batch.Column(2).IsNull(3));
        Assert.Equal(3.75, batch.Column(3).GetDouble(2), 4);
        Assert.Equal("alpha", batch.Column(4).GetString(0));
        Assert.True(batch.Column(4).IsNull(2));
        Assert.Equal("delta", batch.Column(4).GetString(3));
    }

    private static ArrowRecordBatch MakeBatch(
        ArrowSchema schema,
        bool[] flags,
        int[] ids,
        long[] bigs,
        float[] floats,
        double[] doubles,
        string?[] depts,
        int?[] optVals)
    {
        int n = ids.Length;
        byte[] flagBytes = new byte[(n + 7) >> 3];
        for (int i = 0; i < n; i++)
        {
            if (flags[i]) flagBytes[i >> 3] |= (byte)(1 << (i & 7));
        }

        byte[] deptNulls = new byte[(n + 7) >> 3];
        int deptNullCount = 0;
        int[] deptOffsets = new int[n + 1];
        using var deptMs = new MemoryStream();
        for (int i = 0; i < n; i++)
        {
            deptOffsets[i] = (int)deptMs.Position;
            if (depts[i] is string s)
            {
                deptNulls[i >> 3] |= (byte)(1 << (i & 7));
                deptMs.Write(Encoding.UTF8.GetBytes(s));
            }
            else
            {
                deptNullCount++;
            }
        }
        deptOffsets[n] = (int)deptMs.Position;

        byte[] optNulls = new byte[(n + 7) >> 3];
        int optNullCount = 0;
        int[] optData = new int[n];
        for (int i = 0; i < n; i++)
        {
            if (optVals[i] is int v)
            {
                optNulls[i >> 3] |= (byte)(1 << (i & 7));
                optData[i] = v;
            }
            else
            {
                optNullCount++;
            }
        }

        return new ArrowRecordBatch(schema, n, [
            new ArrowColumn(schema.GetField(0), n, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, flagBytes),
            new ArrowColumn(schema.GetField(1), n, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(ids.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(2), n, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(bigs.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(3), n, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(floats.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(4), n, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(doubles.AsSpan()).ToArray()),
            new ArrowColumn(schema.GetField(5), n, deptNullCount, deptNullCount > 0 ? deptNulls : ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(deptOffsets.AsSpan()).ToArray(), deptMs.ToArray()),
            new ArrowColumn(schema.GetField(6), n, optNullCount, optNullCount > 0 ? optNulls : ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, MemoryMarshal.AsBytes(optData.AsSpan()).ToArray())
        ]);
    }
}
#endif
