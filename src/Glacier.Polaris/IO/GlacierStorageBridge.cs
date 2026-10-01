namespace Glacier.Polaris.IO;

using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.Memory;
using Glacier.Storage.Arrow;

/// <summary>
/// Direct zero-copy, zero-heap-allocation bridge between Glacier.Polaris DataFrames and the
/// first-party Glacier.Storage engine. Provides pure C# streaming Arrow IPC serialization and
/// deserialization across all 16 supported Polaris types with bit-blitted validity masks.
/// </summary>
public static class GlacierStorageBridge
{
    /// <summary>
    /// Writes a DataFrame to an Arrow IPC stream using the zero-allocation Glacier.Storage engine.
    /// </summary>
    public static void ToArrowIpc(this DataFrame df, Stream destinationStream, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(df);
        ArgumentNullException.ThrowIfNull(destinationStream);

        var batch = df.ToGlacierRecordBatch();
        using var writer = new ArrowStreamWriter(destinationStream, leaveOpen);
        writer.WriteSchema(batch.Schema);
        writer.WriteRecordBatch(batch);
        writer.WriteEndOfStream();
    }

    /// <summary>
    /// Backward-compatible alias for ToArrowIpc.
    /// </summary>
    public static void WriteGlacierArrowIpc(this DataFrame df, Stream stream, bool leaveOpen = false)
    {
        ToArrowIpc(df, stream, leaveOpen);
    }

    /// <summary>
    /// Reads a DataFrame from an Arrow IPC stream using the Glacier.Storage pure C# engine.
    /// </summary>
    public static DataFrame FromArrowIpc(Stream sourceStream, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(sourceStream);

        using var reader = new ArrowStreamReader(sourceStream, leaveOpen);
        var batches = new List<DataFrame>();

        while (reader.HasMoreBatches)
        {
            var batch = reader.ReadNextRecordBatch();
            if (batch == null) break;
            batches.Add(FromGlacierRecordBatch(batch));
        }

        if (batches.Count == 0)
        {
            if (reader.Schema != null)
            {
                var emptyCols = new List<ISeries>(reader.Schema.FieldCount);
                for (int i = 0; i < reader.Schema.FieldCount; i++)
                {
                    var f = reader.Schema.GetField(i);
                    emptyCols.Add(CreateEmptySeries(f.Name, f.DataType.Id));
                }
                return new DataFrame(emptyCols);
            }
            return new DataFrame();
        }

        if (batches.Count == 1) return batches[0];
        return DataFrame.Concat(batches);
    }

    /// <summary>
    /// Creates a Glacier.Storage ArrowRecordBatch from a Glacier.Polaris DataFrame with zero-copy column views.
    /// </summary>
    public static ArrowRecordBatch ToGlacierRecordBatch(this DataFrame df)
    {
        ArgumentNullException.ThrowIfNull(df);

        int colCount = df.Columns.Count;
        var columns = new List<ArrowColumn>(colCount);
        var fields = new List<ArrowField>(colCount);

        for (int i = 0; i < colCount; i++)
        {
            var col = df.Columns[i];
            var arrowCol = col.ToArrowColumn();
            columns.Add(arrowCol);
            fields.Add(arrowCol.Field);
        }

        var schema = new ArrowSchema(fields);
        return new ArrowRecordBatch(schema, df.RowCount, columns);
    }

    /// <summary>
    /// Constructs a Glacier.Polaris DataFrame directly from a Glacier.Storage ArrowRecordBatch
    /// using unmanaged NativeMemoryOwner backing for all 16 supported column types.
    /// </summary>
    public static DataFrame FromGlacierRecordBatch(ArrowRecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        int colCount = batch.Columns.Count;
        var columns = new List<ISeries>(colCount);

        for (int i = 0; i < colCount; i++)
        {
            var col = batch.Columns[i];
            columns.Add(DeserializeColumn(col));
        }

        return new DataFrame(columns);
    }

    private static ISeries DeserializeColumn(ArrowColumn col)
    {
        int length = col.Length;
        var typeId = col.Field.DataType.Id;
        string name = col.Field.Name;

        // Build validity mask from NullBitmap via little-endian bit-blit
        ValidityMask mask;
        if (col.NullCount > 0 && !col.NullBitmap.IsEmpty)
        {
            mask = ValidityMask.FromNullBitmap(length, col.NullBitmap.Span);
        }
        else
        {
            mask = new ValidityMask(length, setAllValid: true);
        }

        switch (typeId)
        {
            case ArrowTypeId.Int8:
            {
                var owner = new NativeMemoryOwner<sbyte>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(sbyte))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new Int8Series(name, length, owner, mask);
            }
            case ArrowTypeId.UInt8:
            {
                var owner = new NativeMemoryOwner<byte>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length)]
                        .CopyTo(owner.Span);
                }
                return new UInt8Series(name, length, owner, mask);
            }
            case ArrowTypeId.Int16:
            {
                var owner = new NativeMemoryOwner<short>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(short))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new Int16Series(name, length, owner, mask);
            }
            case ArrowTypeId.UInt16:
            {
                var owner = new NativeMemoryOwner<ushort>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(ushort))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new UInt16Series(name, length, owner, mask);
            }
            case ArrowTypeId.Int32:
            {
                var owner = new NativeMemoryOwner<int>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(int))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new Int32Series(name, length, owner, mask);
            }
            case ArrowTypeId.UInt32:
            {
                var owner = new NativeMemoryOwner<uint>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(uint))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new UInt32Series(name, length, owner, mask);
            }
            case ArrowTypeId.Int64:
            {
                var owner = new NativeMemoryOwner<long>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(long))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new Int64Series(name, length, owner, mask);
            }
            case ArrowTypeId.UInt64:
            {
                var owner = new NativeMemoryOwner<ulong>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(ulong))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new UInt64Series(name, length, owner, mask);
            }
            case ArrowTypeId.Float:
            {
                var owner = new NativeMemoryOwner<float>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(float))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new Float32Series(name, length, owner, mask);
            }
            case ArrowTypeId.Double:
            {
                var owner = new NativeMemoryOwner<double>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(double))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new Float64Series(name, length, owner, mask);
            }
            case ArrowTypeId.Boolean:
            {
                var owner = new NativeMemoryOwner<bool>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    var dataSpan = col.DataBuffer.Span;
                    var boolSpan = owner.Span;
                    for (int i = 0; i < length; i++)
                    {
                        boolSpan[i] = (dataSpan[i >> 3] & (1 << (i & 7))) != 0;
                    }
                }
                return new BooleanSeries(name, length, owner, mask);
            }
            case ArrowTypeId.Date32:
            {
                var owner = new NativeMemoryOwner<int>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(int))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new DateSeries(name, length, owner, mask);
            }
            case ArrowTypeId.Timestamp:
            {
                var owner = new NativeMemoryOwner<long>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(long))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new DatetimeSeries(name, length, owner, mask);
            }
            case ArrowTypeId.Duration:
            {
                var owner = new NativeMemoryOwner<long>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(long))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new DurationSeries(name, length, owner, mask);
            }
            case ArrowTypeId.Utf8:
            {
                var offsetsOwner = new NativeMemoryOwner<int>(length + 1);
                if (!col.OffsetsBuffer.IsEmpty)
                {
                    col.OffsetsBuffer.Span[..Math.Min(col.OffsetsBuffer.Length, (length + 1) * sizeof(int))]
                        .CopyTo(MemoryMarshal.AsBytes(offsetsOwner.Span));
                }
                var dataOwner = new NativeMemoryOwner<byte>(col.DataBuffer.Length);
                if (!col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span.CopyTo(dataOwner.Span);
                }
                return new Utf8StringSeries(name, length, offsetsOwner, dataOwner, mask);
            }
            case ArrowTypeId.Binary:
            {
                var offsetsOwner = new NativeMemoryOwner<int>(length + 1);
                if (!col.OffsetsBuffer.IsEmpty)
                {
                    col.OffsetsBuffer.Span[..Math.Min(col.OffsetsBuffer.Length, (length + 1) * sizeof(int))]
                        .CopyTo(MemoryMarshal.AsBytes(offsetsOwner.Span));
                }
                var dataOwner = new NativeMemoryOwner<byte>(col.DataBuffer.Length);
                if (!col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span.CopyTo(dataOwner.Span);
                }
                return new BinarySeries(name, length, offsetsOwner, dataOwner, mask);
            }
            case ArrowTypeId.Decimal128:
            {
                var owner = new NativeMemoryOwner<decimal>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(decimal))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new DecimalSeries(name, length, owner, mask);
            }
            case ArrowTypeId.Time64:
            {
                var owner = new NativeMemoryOwner<long>(length);
                if (length > 0 && !col.DataBuffer.IsEmpty)
                {
                    col.DataBuffer.Span[..Math.Min(col.DataBuffer.Length, length * sizeof(long))]
                        .CopyTo(MemoryMarshal.AsBytes(owner.Span));
                }
                return new TimeSeries(name, length, owner, mask);
            }
            case ArrowTypeId.Null:
            {
                return new NullSeries(name, length);
            }
            default:
            {
                var offsetsOwner = new NativeMemoryOwner<int>(length + 1);
                var dataOwner = new NativeMemoryOwner<byte>(col.DataBuffer.Length);
                if (!col.DataBuffer.IsEmpty) col.DataBuffer.Span.CopyTo(dataOwner.Span);
                return new BinarySeries(name, length, offsetsOwner, dataOwner, mask);
            }
        }
    }

    private static ISeries CreateEmptySeries(string name, ArrowTypeId typeId) => typeId switch
    {
        ArrowTypeId.Int8 => new Int8Series(name, 0),
        ArrowTypeId.UInt8 => new UInt8Series(name, 0),
        ArrowTypeId.Int16 => new Int16Series(name, 0),
        ArrowTypeId.UInt16 => new UInt16Series(name, 0),
        ArrowTypeId.Int32 => new Int32Series(name, 0),
        ArrowTypeId.UInt32 => new UInt32Series(name, 0),
        ArrowTypeId.Int64 => new Int64Series(name, 0),
        ArrowTypeId.UInt64 => new UInt64Series(name, 0),
        ArrowTypeId.Float => new Float32Series(name, 0),
        ArrowTypeId.Double => new Float64Series(name, 0),
        ArrowTypeId.Boolean => new BooleanSeries(name, 0),
        ArrowTypeId.Date32 => new DateSeries(name, 0),
        ArrowTypeId.Timestamp => new DatetimeSeries(name, 0),
        ArrowTypeId.Duration => new DurationSeries(name, 0),
        ArrowTypeId.Time64 => new TimeSeries(name, 0),
        ArrowTypeId.Utf8 => new Utf8StringSeries(name, 0),
        ArrowTypeId.Binary => new BinarySeries(name, 0, 0),
        ArrowTypeId.Decimal128 => new DecimalSeries(name, 0),
        ArrowTypeId.Null => new NullSeries(name, 0),
        _ => new BinarySeries(name, 0, 0)
    };
}
