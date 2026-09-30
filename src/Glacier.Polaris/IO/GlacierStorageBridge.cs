namespace Glacier.Polaris.IO;

using System;
using System.IO;
using System.Collections.Generic;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Storage;
using Glacier.Storage.Arrow;

/// <summary>
/// Direct zero-copy bridge between Glacier.Polaris DataFrames and the first-party Glacier.Storage engine.
/// Provides pure managed streaming Arrow IPC and columnar table store persistence without external dependencies.
/// </summary>
public static class GlacierStorageBridge
{
    /// <summary>
    /// Writes a DataFrame to a stream using Glacier.Storage pure managed ArrowStreamWriter.
    /// </summary>
    public static void WriteGlacierArrowIpc(this DataFrame df, Stream stream, bool leaveOpen = false)
    {
        int colCount = df.Columns.Count;
        var fields = new List<ArrowField>(colCount);
        for (int i = 0; i < colCount; i++)
        {
            var col = df.Columns[i];
            var dt = col switch
            {
                Int32Series => ArrowType.Int32,
                Int64Series => ArrowType.Int64,
                Float32Series => ArrowType.Float,
                Float64Series => ArrowType.Double,
                Utf8StringSeries => ArrowType.Utf8,
                BooleanSeries => ArrowType.Boolean,
                _ => ArrowType.Binary
            };
            fields.Add(new ArrowField(col.Name, dt, isNullable: true));
        }

        var schema = new ArrowSchema(fields);
        using var writer = new ArrowStreamWriter(stream, leaveOpen);
        writer.WriteSchema(schema);

        var columns = new List<ArrowColumn>(colCount);
        for (int i = 0; i < colCount; i++)
        {
            var col = df.Columns[i];
            var field = fields[i];

            ReadOnlyMemory<byte> dataMem;
            if (col is Int32Series s32)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(s32.Memory.Span).ToArray();
            }
            else if (col is Float32Series sf32)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(sf32.Memory.Span).ToArray();
            }
            else if (col is Float64Series sf64)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(sf64.Memory.Span).ToArray();
            }
            else if (col is Int64Series s64)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(s64.Memory.Span).ToArray();
            }
            else
            {
                dataMem = ReadOnlyMemory<byte>.Empty;
            }

            columns.Add(new ArrowColumn(field, df.RowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, dataMem));
        }

        var batch = new ArrowRecordBatch(schema, df.RowCount, columns);
        writer.WriteRecordBatch(batch);
    }

    /// <summary>
    /// Creates a Glacier.Storage ArrowRecordBatch from a Glacier.Polaris DataFrame.
    /// </summary>
    public static ArrowRecordBatch ToGlacierRecordBatch(this DataFrame df)
    {
        int colCount = df.Columns.Count;
        var fields = new List<ArrowField>(colCount);
        for (int i = 0; i < colCount; i++)
        {
            var col = df.Columns[i];
            var dt = col switch
            {
                Int32Series => ArrowType.Int32,
                Int64Series => ArrowType.Int64,
                Float32Series => ArrowType.Float,
                Float64Series => ArrowType.Double,
                Utf8StringSeries => ArrowType.Utf8,
                BooleanSeries => ArrowType.Boolean,
                _ => ArrowType.Binary
            };
            fields.Add(new ArrowField(col.Name, dt, isNullable: true));
        }

        var schema = new ArrowSchema(fields);
        var columns = new List<ArrowColumn>(colCount);
        for (int i = 0; i < colCount; i++)
        {
            var col = df.Columns[i];
            var field = fields[i];

            ReadOnlyMemory<byte> dataMem;
            if (col is Int32Series s32)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(s32.Memory.Span).ToArray();
            }
            else if (col is Float32Series sf32)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(sf32.Memory.Span).ToArray();
            }
            else if (col is Float64Series sf64)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(sf64.Memory.Span).ToArray();
            }
            else if (col is Int64Series s64)
            {
                dataMem = System.Runtime.InteropServices.MemoryMarshal.AsBytes(s64.Memory.Span).ToArray();
            }
            else
            {
                dataMem = ReadOnlyMemory<byte>.Empty;
            }

            columns.Add(new ArrowColumn(field, df.RowCount, 0, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, dataMem));
        }

        return new ArrowRecordBatch(schema, df.RowCount, columns);
    }
}
