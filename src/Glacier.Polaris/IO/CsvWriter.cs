using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.IO
{
    internal static class CsvWriter
    {
        private const int FlushThresholdBytes = 65536;

        public static void Write(DataFrame df, string filePath, char separator = ',', bool includeHeader = true, bool includeBom = false)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);

            if (includeBom)
            {
                fs.WriteByte(0xEF);
                fs.WriteByte(0xBB);
                fs.WriteByte(0xBF);
            }

            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;
            if (rowCount == 0 || colCount == 0) return;

            var buffer = new ArrayBufferWriter<byte>(65536);

            if (includeHeader)
            {
                WriteHeaderRow(df, separator, buffer);
            }

            WriteDataRows(df, separator, buffer, fs);
            if (buffer.WrittenCount > 0)
            {
                fs.Write(buffer.WrittenSpan);
                buffer.Clear();
            }
        }

        internal static async Task WriteStreamAsync(
            IAsyncEnumerable<DataFrame> batches,
            string filePath,
            char separator = ',',
            bool includeHeader = true,
            bool includeBom = false,
            CancellationToken ct = default)
        {
            await using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.SequentialScan);

            if (includeBom)
            {
                ReadOnlyMemory<byte> bom = new byte[] { 0xEF, 0xBB, 0xBF };
                await fs.WriteAsync(bom, ct).ConfigureAwait(false);
            }

            var buffer = new ArrayBufferWriter<byte>(65536);
            bool headerWritten = false;

            await foreach (var df in batches.WithCancellation(ct).ConfigureAwait(false))
            {
                int colCount = df.Columns.Count;
                if (colCount == 0) continue;

                if (includeHeader && !headerWritten)
                {
                    WriteHeaderRow(df, separator, buffer);
                    headerWritten = true;
                }

                if (df.RowCount > 0)
                {
                    var readers = new ColumnReader[colCount];
                    for (int i = 0; i < colCount; i++)
                        readers[i] = new ColumnReader(df.Columns[i]);

                    for (int r = 0; r < df.RowCount; r++)
                    {
                        WriteFormattedValue(in readers[0], r, separator, buffer);
                        for (int c = 1; c < colCount; c++)
                        {
                            WriteByte(buffer, (byte)separator);
                            WriteFormattedValue(in readers[c], r, separator, buffer);
                        }
                        WriteByte(buffer, (byte)'\n');

                        if (buffer.WrittenCount >= FlushThresholdBytes)
                        {
                            await fs.WriteAsync(buffer.WrittenMemory, ct).ConfigureAwait(false);
                            buffer.Clear();
                        }
                    }
                }
            }

            if (buffer.WrittenCount > 0)
            {
                await fs.WriteAsync(buffer.WrittenMemory, ct).ConfigureAwait(false);
                buffer.Clear();
            }
        }

        private static void WriteHeaderRow(DataFrame df, char separator, ArrayBufferWriter<byte> buffer)
        {
            int colCount = df.Columns.Count;
            WriteEncodedCsvField(df.Columns[0].Name, separator, buffer);
            for (int i = 1; i < colCount; i++)
            {
                WriteByte(buffer, (byte)separator);
                WriteEncodedCsvField(df.Columns[i].Name, separator, buffer);
            }
            WriteByte(buffer, (byte)'\n');
        }

        private static void WriteDataRows(DataFrame df, char separator, ArrayBufferWriter<byte> buffer, FileStream fs)
        {
            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;
            var readers = new ColumnReader[colCount];
            for (int i = 0; i < colCount; i++)
                readers[i] = new ColumnReader(df.Columns[i]);

            for (int r = 0; r < rowCount; r++)
            {
                WriteFormattedValue(in readers[0], r, separator, buffer);
                for (int c = 1; c < colCount; c++)
                {
                    WriteByte(buffer, (byte)separator);
                    WriteFormattedValue(in readers[c], r, separator, buffer);
                }
                WriteByte(buffer, (byte)'\n');

                if (buffer.WrittenCount >= FlushThresholdBytes)
                {
                    fs.Write(buffer.WrittenSpan);
                    buffer.Clear();
                }
            }
        }

        private static void WriteByte(ArrayBufferWriter<byte> buffer, byte b)
        {
            var span = buffer.GetSpan(1);
            span[0] = b;
            buffer.Advance(1);
        }

        private static void WriteUtf8String(ArrayBufferWriter<byte> buffer, string text)
        {
            if (text.Length == 0) return;
            int maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
            var span = buffer.GetSpan(maxBytes);
            int written = Encoding.UTF8.GetBytes(text, span);
            buffer.Advance(written);
        }

        private static void WriteEncodedCsvField(string field, char separator, ArrayBufferWriter<byte> buffer)
        {
            bool needsQuoting = false;
            for (int i = 0; i < field.Length; i++)
            {
                char c = field[i];
                if (c == separator || c == '"' || c == '\n' || c == '\r')
                {
                    needsQuoting = true;
                    break;
                }
            }

            if (!needsQuoting)
            {
                WriteUtf8String(buffer, field);
                return;
            }

            WriteByte(buffer, (byte)'"');
            Span<char> ch = stackalloc char[1];
            for (int i = 0; i < field.Length; i++)
            {
                char c = field[i];
                if (c == '"')
                {
                    WriteByte(buffer, (byte)'"');
                    WriteByte(buffer, (byte)'"');
                }
                else
                {
                    ch[0] = c;
                    var dst = buffer.GetSpan(4);
                    int written = Encoding.UTF8.GetBytes(ch, dst);
                    buffer.Advance(written);
                }
            }
            WriteByte(buffer, (byte)'"');
        }

        private static void WriteFormattedValue(in ColumnReader reader, int row, char separator, ArrayBufferWriter<byte> buffer)
        {
            var bytes = reader.GetFormattedValue(row, separator);
            if (bytes.Count > 0)
            {
                var span = buffer.GetSpan(bytes.Count);
                bytes.CopyTo(span);
                buffer.Advance(bytes.Count);
            }
        }

        private struct ColumnReader
        {
            private readonly ISeries _series;
            private readonly Data.Int32Series? _i32;
            private readonly Data.Int64Series? _i64;
            private readonly Data.Float64Series? _f64;
            private readonly Data.Utf8StringSeries? _utf8;
            private readonly Data.BooleanSeries? _bool;
            private readonly Data.Int8Series? _i8;
            private readonly Data.Int16Series? _i16;
            private readonly Data.UInt8Series? _u8;
            private readonly Data.UInt16Series? _u16;
            private readonly Data.UInt32Series? _u32;
            private readonly Data.UInt64Series? _u64;
            private readonly Data.Float32Series? _f32;
            private readonly Data.DateSeries? _date;
            private readonly Data.DatetimeSeries? _datetime;
            private readonly Data.DurationSeries? _duration;
            private readonly Data.DecimalSeries? _decimal;
            private readonly Data.TimeSeries? _time;

            public ColumnReader(ISeries series)
            {
                _series = series;
                _i32 = series as Data.Int32Series;
                _i64 = series as Data.Int64Series;
                _f64 = series as Data.Float64Series;
                _utf8 = series as Data.Utf8StringSeries;
                _bool = series as Data.BooleanSeries;
                _i8 = series as Data.Int8Series;
                _i16 = series as Data.Int16Series;
                _u8 = series as Data.UInt8Series;
                _u16 = series as Data.UInt16Series;
                _u32 = series as Data.UInt32Series;
                _u64 = series as Data.UInt64Series;
                _f32 = series as Data.Float32Series;
                _date = series as Data.DateSeries;
                _datetime = series as Data.DatetimeSeries;
                _duration = series as Data.DurationSeries;
                _decimal = series as Data.DecimalSeries;
                _time = series as Data.TimeSeries;
            }

            public List<byte> GetFormattedValue(int row, char separator)
            {
                var result = new List<byte>();

                if (_series.ValidityMask.IsNull(row))
                {
                    return result; // empty = null
                }

                if (_i32 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_i32.Memory.Span[row].ToString()));
                }
                else if (_i64 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_i64.Memory.Span[row].ToString()));
                }
                else if (_f64 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_f64.Memory.Span[row].ToString(System.Globalization.CultureInfo.InvariantCulture)));
                }
                else if (_utf8 != null)
                {
                    var span = _utf8.GetStringSpan(row);
                    // Check if value needs quoting
                    bool needsQuoting = false;
                    for (int i = 0; i < span.Length; i++)
                    {
                        byte b = span[i];
                        if (b == separator || b == '"' || b == '\n' || b == '\r')
                        {
                            needsQuoting = true;
                            break;
                        }
                    }

                    if (needsQuoting)
                    {
                        result.Add((byte)'"');
                        for (int i = 0; i < span.Length; i++)
                        {
                            byte b = span[i];
                            if (b == '"') result.Add((byte)'"'); // escape
                            result.Add(b);
                        }
                        result.Add((byte)'"');
                    }
                    else
                    {
                        for (int i = 0; i < span.Length; i++)
                            result.Add(span[i]);
                    }
                }
                else if (_bool != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_bool.Memory.Span[row] ? "true" : "false"));
                }
                else if (_i8 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_i8.Memory.Span[row].ToString()));
                }
                else if (_i16 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_i16.Memory.Span[row].ToString()));
                }
                else if (_u8 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_u8.Memory.Span[row].ToString()));
                }
                else if (_u16 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_u16.Memory.Span[row].ToString()));
                }
                else if (_u32 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_u32.Memory.Span[row].ToString()));
                }
                else if (_u64 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_u64.Memory.Span[row].ToString()));
                }
                else if (_f32 != null)
                {
                    result.AddRange(Encoding.UTF8.GetBytes(_f32.Memory.Span[row].ToString(System.Globalization.CultureInfo.InvariantCulture)));
                }
                else if (_date != null)
                {
                    int days = _date.Memory.Span[row];
                    var epoch = new DateOnly(1970, 1, 1);
                    int minDays = DateOnly.MinValue.DayNumber - epoch.DayNumber;
                    int maxDays = DateOnly.MaxValue.DayNumber - epoch.DayNumber;
                    var dateOnly = days <= minDays ? DateOnly.MinValue
                        : days >= maxDays ? DateOnly.MaxValue
                        : epoch.AddDays(days);
                    result.AddRange(Encoding.UTF8.GetBytes(dateOnly.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture)));
                }
                else if (_datetime != null)
                {
                    var dt = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(_datetime.Memory.Span[row] * 10);
                    result.AddRange(Encoding.UTF8.GetBytes(dt.ToString("yyyy-MM-dd HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture)));
                }
                else if (_duration != null)
                {
                    var ts = TimeSpan.FromTicks(_duration.Memory.Span[row] / 100);
                    result.AddRange(Encoding.UTF8.GetBytes(ts.ToString()));
                }
                else if (_decimal != null)
                {
                    var val = _decimal.GetValue(row);
                    if (val.HasValue)
                    {
                        result.AddRange(Encoding.UTF8.GetBytes(val.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                    }
                }
                else if (_time != null)
                {
                    long nanos = _time.Memory.Span[row];
                    var ts = new TimeSpan(nanos / 100); // 1 tick = 100ns
                    result.AddRange(Encoding.UTF8.GetBytes(ts.ToString()));
                }
                else
                {
                    // Fallback: use ToString()
                    var obj = _series.Get(row);
                    if (obj != null)
                        result.AddRange(Encoding.UTF8.GetBytes(obj.ToString()!));
                }

                return result;
            }
        }
    }
}
