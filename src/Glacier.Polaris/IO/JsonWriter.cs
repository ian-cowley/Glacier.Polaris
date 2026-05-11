using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.IO
{
    internal static class JsonWriter
    {
        public static void Write(DataFrame df, string filePath, bool ndjson = true)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            using var writer = new Utf8JsonWriter(fs, new JsonWriterOptions { Indented = !ndjson });

            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;

            if (rowCount == 0 && colCount == 0) return;

            // Prepare column readers
            var readers = new ColumnReader[colCount];
            for (int i = 0; i < colCount; i++)
            {
                readers[i] = new ColumnReader(df.Columns[i]);
            }

            if (ndjson)
            {
                // NDJSON: one JSON object per line
                for (int r = 0; r < rowCount; r++)
                {
                    writer.Reset();
                    writer.WriteStartObject();
                    for (int c = 0; c < colCount; c++)
                    {
                        readers[c].WriteValue(writer, r);
                    }
                    writer.WriteEndObject();
                    writer.Flush();
                    fs.WriteByte((byte)'\n');
                }
            }
            else
            {
                // Pretty-printed JSON array of objects
                writer.WriteStartArray();
                for (int r = 0; r < rowCount; r++)
                {
                    writer.WriteStartObject();
                    for (int c = 0; c < colCount; c++)
                    {
                        readers[c].WriteValue(writer, r);
                    }
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
            }
        }

        private struct ColumnReader
        {
            private readonly string _name;
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
            private readonly Data.ListSeries? _list;
            private readonly Data.StructSeries? _struct;

            public ColumnReader(ISeries series)
            {
                _name = series.Name;
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
                _list = series as Data.ListSeries;
                _struct = series as Data.StructSeries;
            }

            public void WriteValue(Utf8JsonWriter writer, int row)
            {
                if (_series.ValidityMask.IsNull(row))
                {
                    writer.WriteNull(_name);
                    return;
                }

                if (_i32 != null) writer.WriteNumber(_name, _i32.Memory.Span[row]);
                else if (_i64 != null) writer.WriteNumber(_name, _i64.Memory.Span[row]);
                else if (_f64 != null) writer.WriteNumber(_name, _f64.Memory.Span[row]);
                else if (_utf8 != null)
                {
                    var span = _utf8.GetStringSpan(row);
                    writer.WriteString(_name, Encoding.UTF8.GetString(span));
                }
                else if (_bool != null) writer.WriteBoolean(_name, _bool.Memory.Span[row]);
                else if (_i8 != null) writer.WriteNumber(_name, _i8.Memory.Span[row]);
                else if (_i16 != null) writer.WriteNumber(_name, _i16.Memory.Span[row]);
                else if (_u8 != null) writer.WriteNumber(_name, _u8.Memory.Span[row]);
                else if (_u16 != null) writer.WriteNumber(_name, _u16.Memory.Span[row]);
                else if (_u32 != null) writer.WriteNumber(_name, _u32.Memory.Span[row]);
                else if (_u64 != null) writer.WriteNumber(_name, _u64.Memory.Span[row]);
                else if (_f32 != null) writer.WriteNumber(_name, _f32.Memory.Span[row]);
                else if (_date != null)
                {
                    var dateOnly = DateOnly.FromDayNumber((int)_date.Memory.Span[row]);
                    writer.WriteString(_name, dateOnly.ToString("yyyy-MM-dd"));
                }
                else if (_datetime != null)
                {
                    var dt = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(_datetime.Memory.Span[row] * 10);
                    writer.WriteString(_name, dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ"));
                }
                else if (_duration != null)
                {
                    var ts = TimeSpan.FromTicks(_duration.Memory.Span[row] / 100);
                    writer.WriteString(_name, ts.ToString());
                }
                else if (_decimal != null)
                {
                    var val = _decimal.GetValue(row);
                    if (val.HasValue) writer.WriteNumber(_name, val.Value);
                    else writer.WriteNull(_name);
                }
                else if (_time != null)
                {
                    long nanos = _time.Memory.Span[row];
                    var ts = new TimeSpan(nanos / 100);
                    writer.WriteString(_name, ts.ToString());
                }
                else if (_list != null)
                {
                    var listVal = _list.Get(row);
                    writer.WritePropertyName(_name);
                    if (listVal == null)
                    {
                        writer.WriteNullValue();
                    }
                    else if (listVal is Array arr)
                    {
                        writer.WriteStartArray();
                        foreach (var item in arr)
                        {
                            WriteObjectValue(writer, item);
                        }
                        writer.WriteEndArray();
                    }
                    else
                    {
                        writer.WriteStartArray();
                        writer.WriteEndArray();
                    }
                }
                else if (_struct != null)
                {
                    writer.WritePropertyName(_name);
                    writer.WriteStartObject();
                    foreach (var field in _struct.Fields)
                    {
                        var fieldReader = new ColumnReader(field);
                        fieldReader.WriteNestedValue(writer, row);
                    }
                    writer.WriteEndObject();
                }
                else
                {
                    var obj = _series.Get(row);
                    writer.WritePropertyName(_name);
                    WriteObjectValue(writer, obj);
                }
            }

            // For struct fields, write value without property name
            private void WriteNestedValue(Utf8JsonWriter writer, int row)
            {
                if (_series.ValidityMask.IsNull(row))
                {
                    writer.WriteNull(_name);
                    return;
                }

                if (_i32 != null) writer.WriteNumber(_name, _i32.Memory.Span[row]);
                else if (_i64 != null) writer.WriteNumber(_name, _i64.Memory.Span[row]);
                else if (_f64 != null) writer.WriteNumber(_name, _f64.Memory.Span[row]);
                else if (_utf8 != null) writer.WriteString(_name, Encoding.UTF8.GetString(_utf8.GetStringSpan(row)));
                else if (_bool != null) writer.WriteBoolean(_name, _bool.Memory.Span[row]);
                else if (_decimal != null)
                {
                    var val = _decimal.GetValue(row);
                    if (val.HasValue) writer.WriteNumber(_name, val.Value);
                    else writer.WriteNull(_name);
                }
                else writer.WriteNull(_name);
            }

            private static void WriteObjectValue(Utf8JsonWriter writer, object? value)
            {
                if (value == null) { writer.WriteNullValue(); }
                else if (value is int iv) writer.WriteNumberValue(iv);
                else if (value is long lv) writer.WriteNumberValue(lv);
                else if (value is double dv) writer.WriteNumberValue(dv);
                else if (value is float fv) writer.WriteNumberValue(fv);
                else if (value is decimal dcv) writer.WriteNumberValue(dcv);
                else if (value is bool bv) writer.WriteBooleanValue(bv);
                else if (value is string sv) writer.WriteStringValue(sv);
                else writer.WriteStringValue(value.ToString());
            }
        }
    }
}
