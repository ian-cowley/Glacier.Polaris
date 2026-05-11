using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Glacier.Polaris.IO
{
    internal static class ParquetWriter
    {
        public static void Write(DataFrame df, string filePath)
        {
            WriteAsync(df, filePath).GetAwaiter().GetResult();
        }

        private static async Task WriteAsync(DataFrame df, string filePath, CancellationToken ct = default)
        {
            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;

            using var fileStream = File.OpenWrite(filePath);

            if (rowCount == 0 || colCount == 0)
            {
                // Write empty schema
                var emptySchema = new ParquetSchema();
                using var emptyWriter = await Parquet.ParquetWriter.CreateAsync(emptySchema, fileStream, cancellationToken: ct);
                return;
            }

            // Build schema from DataFrame columns
            var dataFields = new List<DataField>();
            var fieldData = new List<ColData>();

            foreach (var col in df.Columns)
            {
                var (field, data) = CreateParquetColumn(col);
                dataFields.Add(field);
                fieldData.Add(data);
            }

            var schema = new ParquetSchema(dataFields);

            using var writer = await Parquet.ParquetWriter.CreateAsync(schema, fileStream, cancellationToken: ct);

            // Write as a single row group
            using var rowGroup = writer.CreateRowGroup();

            for (int i = 0; i < colCount; i++)
            {
                var dataColumn = new DataColumn(dataFields[i], fieldData[i].GetValues(rowCount));
                await rowGroup.WriteColumnAsync(dataColumn, ct);
            }
        }

        private static (DataField field, ColData data) CreateParquetColumn(ISeries series)
        {
            if (series is Int32Series i32)
                return (new DataField<int?>(series.Name), new Int32ColData(i32));
            if (series is Int64Series i64)
                return (new DataField<long?>(series.Name), new Int64ColData(i64));
            if (series is Float64Series f64)
                return (new DataField<double?>(series.Name), new Float64ColData(f64));
            if (series is Utf8StringSeries u8)
                return (new DataField<string>(series.Name), new StringColData(u8));
            if (series is BooleanSeries bl)
                return (new DataField<bool?>(series.Name), new BoolColData(bl));
            if (series is Int8Series i8)
                return (new DataField<int>(series.Name), new Int32ColData(FromInt8(i8)));
            if (series is Int16Series i16)
                return (new DataField<int>(series.Name), new Int32ColData(FromInt16(i16)));
            if (series is UInt8Series u8s)
                return (new DataField<int>(series.Name), new Int32ColData(FromUInt8(u8s)));
            if (series is UInt16Series u16)
                return (new DataField<int>(series.Name), new Int32ColData(FromUInt16(u16)));
            if (series is UInt32Series u32)
                return (new DataField<long>(series.Name), new Int64ColData(FromUInt32(u32)));
            if (series is Float32Series f32)
                return (new DataField<float?>(series.Name), new Float32ColData(f32));
            if (series is DateSeries date)
                return (new DataField<DateTime>(series.Name), new DateColData(date));
            if (series is DatetimeSeries dt)
                return (new DataField<DateTime>(series.Name), new DatetimeColData(dt));
            if (series is DecimalSeries dec)
                return (new DataField<decimal>(series.Name), new DecimalColData(dec));
            if (series is TimeSeries time)
                return (new DataField<TimeSpan>(series.Name), new TimeColData(time));
            if (series is DurationSeries dur)
                return (new DataField<TimeSpan>(series.Name), new DurationColData(dur));

            // Fallback: treat as string
            var strings = new string?[series.Length];
            for (int i = 0; i < series.Length; i++)
                strings[i] = series.ValidityMask.IsNull(i) ? null : series.Get(i)?.ToString();
            return (new DataField<string>(series.Name), new StringColData(Utf8StringSeries.FromStrings(series.Name, strings)));

            static Int32Series FromInt8(Int8Series s)
            {
                var result = new Int32Series(s.Name, s.Length);
                for (int i = 0; i < s.Length; i++)
                {
                    if (!s.ValidityMask.IsNull(i))
                        result.Memory.Span[i] = s.Memory.Span[i];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }

            static Int32Series FromInt16(Int16Series s)
            {
                var result = new Int32Series(s.Name, s.Length);
                for (int i = 0; i < s.Length; i++)
                {
                    if (!s.ValidityMask.IsNull(i))
                        result.Memory.Span[i] = s.Memory.Span[i];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }

            static Int32Series FromUInt8(UInt8Series s)
            {
                var result = new Int32Series(s.Name, s.Length);
                for (int i = 0; i < s.Length; i++)
                {
                    if (!s.ValidityMask.IsNull(i))
                        result.Memory.Span[i] = s.Memory.Span[i];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }

            static Int32Series FromUInt16(UInt16Series s)
            {
                var result = new Int32Series(s.Name, s.Length);
                for (int i = 0; i < s.Length; i++)
                {
                    if (!s.ValidityMask.IsNull(i))
                        result.Memory.Span[i] = s.Memory.Span[i];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }

            static Int64Series FromUInt32(UInt32Series s)
            {
                var result = new Int64Series(s.Name, s.Length);
                for (int i = 0; i < s.Length; i++)
                {
                    if (!s.ValidityMask.IsNull(i))
                        result.Memory.Span[i] = s.Memory.Span[i];
                    else
                        result.ValidityMask.SetNull(i);
                }
                return result;
            }
        }

        private abstract class ColData
        {
            public abstract Array GetValues(int length);
        }

        private sealed class Int32ColData : ColData
        {
            private readonly Int32Series _s;
            public Int32ColData(Int32Series s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new int?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.Memory.Span[i];
                return result;
            }
        }

        private sealed class Int64ColData : ColData
        {
            private readonly Int64Series _s;
            public Int64ColData(Int64Series s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new long?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.Memory.Span[i];
                return result;
            }
        }

        private sealed class Float64ColData : ColData
        {
            private readonly Float64Series _s;
            public Float64ColData(Float64Series s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new double?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.Memory.Span[i];
                return result;
            }
        }

        private sealed class Float32ColData : ColData
        {
            private readonly Float32Series _s;
            public Float32ColData(Float32Series s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new float?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.Memory.Span[i];
                return result;
            }
        }

        private sealed class StringColData : ColData
        {
            private readonly Utf8StringSeries _s;
            public StringColData(Utf8StringSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new string?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.GetString(i);
                return result;
            }
        }

        private sealed class BoolColData : ColData
        {
            private readonly BooleanSeries _s;
            public BoolColData(BooleanSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new bool?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.Memory.Span[i];
                return result;
            }
        }

        private sealed class DateColData : ColData
        {
            private readonly DateSeries _s;
            public DateColData(DateSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new DateTime?[length];
                for (int i = 0; i < length; i++)
                {
                    if (_s.ValidityMask.IsNull(i))
                    {
                        result[i] = null;
                    }
                    else
                    {
                        var dateOnly = DateOnly.FromDayNumber((int)_s.Memory.Span[i]);
                        result[i] = dateOnly.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
                    }
                }
                return result;
            }
        }

        private sealed class DatetimeColData : ColData
        {
            private readonly DatetimeSeries _s;
            public DatetimeColData(DatetimeSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new DateTime?[length];
                for (int i = 0; i < length; i++)
                {
                    if (_s.ValidityMask.IsNull(i))
                    {
                        result[i] = null;
                    }
                    else
                    {
                        result[i] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(_s.Memory.Span[i] * 10);
                    }
                }
                return result;
            }
        }

        private sealed class DecimalColData : ColData
        {
            private readonly DecimalSeries _s;
            public DecimalColData(DecimalSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new decimal?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.GetValue(i);
                return result;
            }
        }

        private sealed class TimeColData : ColData
        {
            private readonly TimeSeries _s;
            public TimeColData(TimeSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new TimeSpan?[length];
                for (int i = 0; i < length; i++)
                {
                    if (_s.ValidityMask.IsNull(i))
                    {
                        result[i] = null;
                    }
                    else
                    {
                        long nanos = _s.Memory.Span[i];
                        result[i] = new TimeSpan(nanos / 100);
                    }
                }
                return result;
            }
        }

        private sealed class DurationColData : ColData
        {
            private readonly DurationSeries _s;
            public DurationColData(DurationSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new TimeSpan?[length];
                for (int i = 0; i < length; i++)
                {
                    if (_s.ValidityMask.IsNull(i))
                    {
                        result[i] = null;
                    }
                    else
                    {
                        result[i] = TimeSpan.FromTicks(_s.Memory.Span[i] / 100);
                    }
                }
                return result;
            }
        }
    }
}
