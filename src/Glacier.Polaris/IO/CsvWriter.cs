using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.IO
{
    internal static class CsvWriter
    {
        public static void Write(DataFrame df, string filePath, char separator = ',', bool includeHeader = true, bool includeBom = false)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);

            if (includeBom)
            {
                fs.WriteByte(0xEF);
                fs.WriteByte(0xBB);
                fs.WriteByte(0xBF);
            }

            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;
            if (rowCount == 0 || colCount == 0) return;

            // Write header
            if (includeHeader)
            {
                var headerBytes = EncodeCsvField(df.Columns[0].Name, separator);
                for (int i = 1; i < colCount; i++)
                {
                    headerBytes.Add((byte)separator);
                    headerBytes.AddRange(EncodeCsvField(df.Columns[i].Name, separator));
                }
                headerBytes.Add((byte)'\n');
                fs.Write(headerBytes.ToArray(), 0, headerBytes.Count);
            }

            // Prepare column readers
            var readers = new ColumnReader[colCount];
            for (int i = 0; i < colCount; i++)
            {
                readers[i] = new ColumnReader(df.Columns[i]);
            }

            // Write rows
            var rowBytes = new List<byte>(1024);
            for (int r = 0; r < rowCount; r++)
            {
                rowBytes.Clear();

                // First column
                rowBytes.AddRange(readers[0].GetFormattedValue(r, separator));

                // Remaining columns
                for (int c = 1; c < colCount; c++)
                {
                    rowBytes.Add((byte)separator);
                    rowBytes.AddRange(readers[c].GetFormattedValue(r, separator));
                }
                rowBytes.Add((byte)'\n');
                fs.Write(rowBytes.ToArray(), 0, rowBytes.Count);
            }
        }

        private static List<byte> EncodeCsvField(string field, char separator)
        {
            var result = new List<byte>();

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
                result.AddRange(Encoding.UTF8.GetBytes(field));
                return result;
            }

            // Encode with quoting
            result.Add((byte)'"');
            for (int i = 0; i < field.Length; i++)
            {
                char c = field[i];
                if (c == '"')
                {
                    // Escape double-quote by doubling it
                    result.Add((byte)'"');
                    result.Add((byte)'"');
                }
                else
                {
                    result.AddRange(Encoding.UTF8.GetBytes(new[] { c }));
                }
            }
            result.Add((byte)'"');
            return result;
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
                    var dateOnly = DateOnly.FromDayNumber((int)_date.Memory.Span[row]);
                    result.AddRange(Encoding.UTF8.GetBytes(dateOnly.ToString("yyyy-MM-dd")));
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
