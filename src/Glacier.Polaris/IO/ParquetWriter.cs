using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
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
            Task.Run(() => WriteAsync(df, filePath)).GetAwaiter().GetResult();
        }

        public static async Task WriteAsync(DataFrame df, string filePath, CancellationToken ct = default)
        {
            int rowCount = df.RowCount;
            int colCount = df.Columns.Count;

            using var fileStream = File.Create(filePath);

            if (rowCount == 0 || colCount == 0)
            {
                if (colCount == 0)
                {
                    var emptySchema = new ParquetSchema();
                    using var emptyWriter = await Parquet.ParquetWriter.CreateAsync(emptySchema, fileStream, cancellationToken: ct).ConfigureAwait(false);
                    return;
                }

                var emptyFields = new List<DataField>(colCount);
                foreach (var col in df.Columns)
                {
                    var (field, _) = CreateParquetColumn(col, forceNullable: false);
                    emptyFields.Add(field);
                }
                using var schemaWriter = await Parquet.ParquetWriter.CreateAsync(new ParquetSchema(emptyFields), fileStream, cancellationToken: ct).ConfigureAwait(false);
                return;
            }

            // Build schema from DataFrame columns
            var dataFields = new List<DataField>(colCount);
            var fieldData = new List<ColData>(colCount);

            foreach (var col in df.Columns)
            {
                var (field, data) = CreateParquetColumn(col, forceNullable: false);
                dataFields.Add(field);
                fieldData.Add(data);
            }

            var schema = new ParquetSchema(dataFields);

            using var writer = await Parquet.ParquetWriter.CreateAsync(schema, fileStream, cancellationToken: ct).ConfigureAwait(false);

            // Write as a single row group
            using var rowGroup = writer.CreateRowGroup();

            var dataColumns = new DataColumn[colCount];
            System.Threading.Tasks.Parallel.For(0, colCount, i =>
            {
                dataColumns[i] = new DataColumn(dataFields[i], fieldData[i].GetValues(rowCount));
            });

            for (int i = 0; i < colCount; i++)
            {
                await rowGroup.WriteColumnAsync(dataColumns[i], ct).ConfigureAwait(false);
            }
        }

        internal static async Task WriteStreamAsync(IAsyncEnumerable<DataFrame> batches, string filePath, CancellationToken ct = default)
        {
            using var fileStream = File.Create(filePath);
            Parquet.ParquetWriter? writer = null;
            DataField[]? dataFields = null;

            try
            {
                await foreach (var df in batches.WithCancellation(ct).ConfigureAwait(false))
                {
                    int colCount = df.Columns.Count;
                    if (colCount == 0)
                    {
                        continue;
                    }

                    if (writer == null)
                    {
                        dataFields = new DataField[colCount];
                        for (int i = 0; i < colCount; i++)
                        {
                            var (field, _) = CreateParquetColumn(df.Columns[i], forceNullable: true);
                            dataFields[i] = field;
                        }

                        var schema = new ParquetSchema(dataFields);
                        writer = await Parquet.ParquetWriter.CreateAsync(schema, fileStream, cancellationToken: ct).ConfigureAwait(false);
                    }

                    int rowCount = df.RowCount;
                    if (rowCount == 0)
                    {
                        continue;
                    }

                    var fieldData = new ColData[colCount];
                    for (int i = 0; i < colCount; i++)
                    {
                        var (_, data) = CreateParquetColumn(df.Columns[i], forceNullable: true);
                        fieldData[i] = data;
                    }

                    var dataColumns = new DataColumn[colCount];
                    System.Threading.Tasks.Parallel.For(0, colCount, i =>
                    {
                        dataColumns[i] = new DataColumn(dataFields![i], fieldData[i].GetValues(rowCount));
                    });

                    using var rowGroup = writer.CreateRowGroup();
                    for (int i = 0; i < colCount; i++)
                    {
                        await rowGroup.WriteColumnAsync(dataColumns[i], ct).ConfigureAwait(false);
                    }
                }

                if (writer == null)
                {
                    var emptySchema = new ParquetSchema();
                    using var emptyWriter = await Parquet.ParquetWriter.CreateAsync(emptySchema, fileStream, cancellationToken: ct).ConfigureAwait(false);
                }
            }
            finally
            {
                writer?.Dispose();
            }
        }

        private static (DataField field, ColData data) CreateParquetColumn(ISeries series, bool forceNullable)
        {
            bool hasNulls = forceNullable || series.ValidityMask.HasNulls;

            if (series is Int32Series i32)
            {
                return hasNulls
                    ? (new DataField<int?>(series.Name), new Int32ColData(i32))
                    : (new DataField<int>(series.Name), new NonNullInt32ColData(i32));
            }
            if (series is Int64Series i64)
            {
                return hasNulls
                    ? (new DataField<long?>(series.Name), new Int64ColData(i64))
                    : (new DataField<long>(series.Name), new NonNullInt64ColData(i64));
            }
            if (series is Float64Series f64)
            {
                return hasNulls
                    ? (new DataField<double?>(series.Name), new Float64ColData(f64))
                    : (new DataField<double>(series.Name), new NonNullFloat64ColData(f64));
            }
            if (series is Float32Series f32)
            {
                return hasNulls
                    ? (new DataField<float?>(series.Name), new Float32ColData(f32))
                    : (new DataField<float>(series.Name), new NonNullFloat32ColData(f32));
            }
            if (series is BooleanSeries bl)
            {
                return hasNulls
                    ? (new DataField<bool?>(series.Name), new BoolColData(bl))
                    : (new DataField<bool>(series.Name), new NonNullBoolColData(bl));
            }
            if (series is Utf8StringSeries u8)
            {
                return hasNulls
                    ? (new DataField<string>(series.Name, nullable: true), new StringColData(u8))
                    : (new DataField<string>(series.Name, nullable: false), new NonNullStringColData(u8));
            }
            if (series is Int8Series i8)
            {
                var widened = FromInt8(i8);
                return hasNulls
                    ? (new DataField<int?>(series.Name), new Int32ColData(widened))
                    : (new DataField<int>(series.Name), new NonNullInt32ColData(widened));
            }
            if (series is Int16Series i16)
            {
                var widened = FromInt16(i16);
                return hasNulls
                    ? (new DataField<int?>(series.Name), new Int32ColData(widened))
                    : (new DataField<int>(series.Name), new NonNullInt32ColData(widened));
            }
            if (series is UInt8Series u8s)
            {
                var widened = FromUInt8(u8s);
                return hasNulls
                    ? (new DataField<int?>(series.Name), new Int32ColData(widened))
                    : (new DataField<int>(series.Name), new NonNullInt32ColData(widened));
            }
            if (series is UInt16Series u16)
            {
                var widened = FromUInt16(u16);
                return hasNulls
                    ? (new DataField<int?>(series.Name), new Int32ColData(widened))
                    : (new DataField<int>(series.Name), new NonNullInt32ColData(widened));
            }
            if (series is UInt32Series u32)
            {
                var widened = FromUInt32(u32);
                return hasNulls
                    ? (new DataField<long?>(series.Name), new Int64ColData(widened))
                    : (new DataField<long>(series.Name), new NonNullInt64ColData(widened));
            }
            if (series is UInt64Series u64)
            {
                return hasNulls
                    ? (new DataField<ulong?>(series.Name), new UInt64ColData(u64))
                    : (new DataField<ulong>(series.Name), new NonNullUInt64ColData(u64));
            }
            if (series is BinarySeries bin)
            {
                return hasNulls
                    ? (new DataField<byte[]>(series.Name, nullable: true), new BinaryColData(bin))
                    : (new DataField<byte[]>(series.Name, nullable: false), new NonNullBinaryColData(bin));
            }
            if (series is DateSeries date)
            {
                return hasNulls
                    ? (new DataField<DateOnly?>(series.Name), new DateColData(date))
                    : (new DataField<DateOnly>(series.Name), new NonNullDateColData(date));
            }
            if (series is DatetimeSeries dt)
            {
                return hasNulls
                    ? (new DataField<DateTime?>(series.Name), new DatetimeColData(dt))
                    : (new DataField<DateTime>(series.Name), new NonNullDatetimeColData(dt));
            }
            if (series is DecimalSeries dec)
            {
                return hasNulls
                    ? (new DataField<decimal?>(series.Name), new DecimalColData(dec))
                    : (new DataField<decimal>(series.Name), new NonNullDecimalColData(dec));
            }
            if (series is TimeSeries time)
            {
                return hasNulls
                    ? (new DataField<TimeSpan?>(series.Name), new TimeColData(time))
                    : (new DataField<TimeSpan>(series.Name), new NonNullTimeColData(time));
            }
            if (series is DurationSeries dur)
            {
                return hasNulls
                    ? (new DataField<TimeSpan?>(series.Name), new DurationColData(dur))
                    : (new DataField<TimeSpan>(series.Name), new NonNullDurationColData(dur));
            }
            if (series is NullSeries)
            {
                return (new DataField<string>(series.Name, nullable: true), new NullStringColData());
            }
            if (series is EnumSeries or CategoricalSeries)
            {
                var strings = new string?[series.Length];
                bool anyNull = hasNulls;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsNull(i))
                    {
                        strings[i] = null;
                        anyNull = true;
                    }
                    else
                    {
                        string? val = series.Get(i)?.ToString();
                        strings[i] = val;
                        if (val == null) anyNull = true;
                    }
                }
                return anyNull
                    ? (new DataField<string>(series.Name, nullable: true), new StringArrayColData(strings))
                    : (new DataField<string>(series.Name, nullable: false), new NonNullStringArrayColData(strings));
            }
            if (series is ListSeries or ArraySeries or StructSeries)
            {
                var strings = new string?[series.Length];
                bool anyNull = hasNulls;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsNull(i))
                    {
                        strings[i] = null;
                        anyNull = true;
                    }
                    else
                    {
                        strings[i] = SerializeNestedValueToJson(series, i);
                    }
                }
                return anyNull
                    ? (new DataField<string>(series.Name, nullable: true), new StringArrayColData(strings))
                    : (new DataField<string>(series.Name, nullable: false), new NonNullStringArrayColData(strings));
            }

            // Fallback: treat as string with accurate nullability
            {
                var strings = new string?[series.Length];
                bool anyNull = hasNulls;
                for (int i = 0; i < series.Length; i++)
                {
                    if (series.ValidityMask.IsNull(i))
                    {
                        strings[i] = null;
                        anyNull = true;
                    }
                    else
                    {
                        string? val = series.Get(i)?.ToString();
                        strings[i] = val;
                        if (val == null) anyNull = true;
                    }
                }
                return anyNull
                    ? (new DataField<string>(series.Name, nullable: true), new StringArrayColData(strings))
                    : (new DataField<string>(series.Name, nullable: false), new NonNullStringArrayColData(strings));
            }

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

        private static readonly DateOnly UnixEpochDate = new(1970, 1, 1);
        private static readonly int MinUnixDays = DateOnly.MinValue.DayNumber - UnixEpochDate.DayNumber;
        private static readonly int MaxUnixDays = DateOnly.MaxValue.DayNumber - UnixEpochDate.DayNumber;

        private static DateOnly UnixDaysToDateOnly(int days)
        {
            if (days <= MinUnixDays) return DateOnly.MinValue;
            if (days >= MaxUnixDays) return DateOnly.MaxValue;
            return UnixEpochDate.AddDays(days);
        }

        private sealed class DateColData : ColData
        {
            private readonly DateSeries _s;
            public DateColData(DateSeries s) => _s = s;

            public override Array GetValues(int length)
            {
                var result = new DateOnly?[length];
                for (int i = 0; i < length; i++)
                {
                    result[i] = _s.ValidityMask.IsNull(i)
                        ? null
                        : UnixDaysToDateOnly(_s.Memory.Span[i]);
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

        private sealed class NonNullInt32ColData : ColData
        {
            private readonly Int32Series _s;
            public NonNullInt32ColData(Int32Series s) => _s = s;
            public override Array GetValues(int length) => _s.Memory.Span.Slice(0, length).ToArray();
        }

        private sealed class NonNullInt64ColData : ColData
        {
            private readonly Int64Series _s;
            public NonNullInt64ColData(Int64Series s) => _s = s;
            public override Array GetValues(int length) => _s.Memory.Span.Slice(0, length).ToArray();
        }

        private sealed class NonNullFloat64ColData : ColData
        {
            private readonly Float64Series _s;
            public NonNullFloat64ColData(Float64Series s) => _s = s;
            public override Array GetValues(int length) => _s.Memory.Span.Slice(0, length).ToArray();
        }

        private sealed class NonNullFloat32ColData : ColData
        {
            private readonly Float32Series _s;
            public NonNullFloat32ColData(Float32Series s) => _s = s;
            public override Array GetValues(int length) => _s.Memory.Span.Slice(0, length).ToArray();
        }

        private sealed class NonNullBoolColData : ColData
        {
            private readonly BooleanSeries _s;
            public NonNullBoolColData(BooleanSeries s) => _s = s;
            public override Array GetValues(int length) => _s.Memory.Span.Slice(0, length).ToArray();
        }

        private sealed class NonNullStringColData : ColData
        {
            private readonly Utf8StringSeries _s;
            public NonNullStringColData(Utf8StringSeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new string[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.GetString(i) ?? string.Empty;
                return result;
            }
        }

        private sealed class NonNullDateColData : ColData
        {
            private readonly DateSeries _s;
            public NonNullDateColData(DateSeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new DateOnly[length];
                for (int i = 0; i < length; i++)
                {
                    result[i] = UnixDaysToDateOnly(_s.Memory.Span[i]);
                }
                return result;
            }
        }

        private sealed class NonNullDatetimeColData : ColData
        {
            private readonly DatetimeSeries _s;
            public NonNullDatetimeColData(DatetimeSeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new DateTime[length];
                for (int i = 0; i < length; i++)
                {
                    result[i] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(_s.Memory.Span[i] * 10);
                }
                return result;
            }
        }

        private sealed class NonNullDecimalColData : ColData
        {
            private readonly DecimalSeries _s;
            public NonNullDecimalColData(DecimalSeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new decimal[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.GetValue(i) ?? 0m;
                return result;
            }
        }

        private sealed class NonNullTimeColData : ColData
        {
            private readonly TimeSeries _s;
            public NonNullTimeColData(TimeSeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new TimeSpan[length];
                for (int i = 0; i < length; i++)
                {
                    long nanos = _s.Memory.Span[i];
                    result[i] = new TimeSpan(nanos / 100);
                }
                return result;
            }
        }

        private sealed class NonNullDurationColData : ColData
        {
            private readonly DurationSeries _s;
            public NonNullDurationColData(DurationSeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new TimeSpan[length];
                for (int i = 0; i < length; i++)
                {
                    result[i] = TimeSpan.FromTicks(_s.Memory.Span[i] / 100);
                }
                return result;
            }
        }

        private sealed class UInt64ColData : ColData
        {
            private readonly UInt64Series _s;
            public UInt64ColData(UInt64Series s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new ulong?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.Memory.Span[i];
                return result;
            }
        }

        private sealed class NonNullUInt64ColData : ColData
        {
            private readonly UInt64Series _s;
            public NonNullUInt64ColData(UInt64Series s) => _s = s;
            public override Array GetValues(int length) => _s.Memory.Span.Slice(0, length).ToArray();
        }

        private sealed class BinaryColData : ColData
        {
            private readonly BinarySeries _s;
            public BinaryColData(BinarySeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new byte[]?[length];
                for (int i = 0; i < length; i++)
                    result[i] = _s.ValidityMask.IsNull(i) ? null : _s.GetSpan(i).ToArray();
                return result;
            }
        }

        private sealed class NonNullBinaryColData : ColData
        {
            private readonly BinarySeries _s;
            public NonNullBinaryColData(BinarySeries s) => _s = s;
            public override Array GetValues(int length)
            {
                var result = new byte[length][];
                for (int i = 0; i < length; i++)
                    result[i] = _s.GetSpan(i).ToArray();
                return result;
            }
        }

        private sealed class NullStringColData : ColData
        {
            public override Array GetValues(int length) => new string?[length];
        }

        private sealed class StringArrayColData : ColData
        {
            private readonly string?[] _values;
            public StringArrayColData(string?[] values) => _values = values;
            public override Array GetValues(int length) => _values.Length == length ? _values : _values[..length];
        }

        private sealed class NonNullStringArrayColData : ColData
        {
            private readonly string[] _values;
            public NonNullStringArrayColData(string?[] values)
            {
                _values = new string[values.Length];
                for (int i = 0; i < values.Length; i++)
                    _values[i] = values[i] ?? string.Empty;
            }
            public override Array GetValues(int length) => _values.Length == length ? _values : _values[..length];
        }

        private static string SerializeNestedValueToJson(ISeries series, int row)
        {
            using var ms = new MemoryStream();
            using (var json = new Utf8JsonWriter(ms))
            {
                WriteSeriesElementJson(json, series, row);
            }
            return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
        }

        private static void WriteSeriesElementJson(Utf8JsonWriter json, ISeries series, int row)
        {
            if (series.ValidityMask.IsNull(row))
            {
                json.WriteNullValue();
                return;
            }

            switch (series)
            {
                case ListSeries list:
                {
                    var offsets = list.Offsets.Memory.Span;
                    int start = offsets[row];
                    int end = offsets[row + 1];
                    json.WriteStartArray();
                    for (int j = start; j < end; j++)
                        WriteSeriesElementJson(json, list.Values, j);
                    json.WriteEndArray();
                    break;
                }
                case ArraySeries arr:
                {
                    int baseIdx = row * arr.Width;
                    json.WriteStartArray();
                    for (int j = 0; j < arr.Width; j++)
                        WriteSeriesElementJson(json, arr.Values, baseIdx + j);
                    json.WriteEndArray();
                    break;
                }
                case StructSeries st:
                {
                    json.WriteStartObject();
                    foreach (var field in st.Fields)
                    {
                        json.WritePropertyName(field.Name);
                        WriteSeriesElementJson(json, field, row);
                    }
                    json.WriteEndObject();
                    break;
                }
                case Int32Series i32:
                    json.WriteNumberValue(i32.Memory.Span[row]);
                    break;
                case Int64Series i64:
                    json.WriteNumberValue(i64.Memory.Span[row]);
                    break;
                case UInt64Series u64:
                    json.WriteNumberValue(u64.Memory.Span[row]);
                    break;
                case UInt32Series u32:
                    json.WriteNumberValue(u32.Memory.Span[row]);
                    break;
                case Int16Series i16:
                    json.WriteNumberValue(i16.Memory.Span[row]);
                    break;
                case UInt16Series u16:
                    json.WriteNumberValue(u16.Memory.Span[row]);
                    break;
                case Int8Series i8:
                    json.WriteNumberValue(i8.Memory.Span[row]);
                    break;
                case UInt8Series u8:
                    json.WriteNumberValue(u8.Memory.Span[row]);
                    break;
                case Float64Series f64:
                    if (double.IsFinite(f64.Memory.Span[row]))
                        json.WriteNumberValue(f64.Memory.Span[row]);
                    else
                        json.WriteStringValue(f64.Memory.Span[row].ToString(CultureInfo.InvariantCulture));
                    break;
                case Float32Series f32:
                    if (float.IsFinite(f32.Memory.Span[row]))
                        json.WriteNumberValue(f32.Memory.Span[row]);
                    else
                        json.WriteStringValue(f32.Memory.Span[row].ToString(CultureInfo.InvariantCulture));
                    break;
                case DecimalSeries dec:
                    json.WriteNumberValue(dec.Memory.Span[row]);
                    break;
                case BooleanSeries b:
                    json.WriteBooleanValue(b.Memory.Span[row]);
                    break;
                case Utf8StringSeries u8s:
                    json.WriteStringValue(u8s.GetStringSpan(row));
                    break;
                case BinarySeries bin:
                    json.WriteBase64StringValue(bin.GetSpan(row));
                    break;
                case DateSeries dt:
                    json.WriteStringValue(UnixDaysToDateOnly(dt.Memory.Span[row]).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                    break;
                case DatetimeSeries dtm:
                    json.WriteStringValue(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(dtm.Memory.Span[row] * 10).ToString("O", CultureInfo.InvariantCulture));
                    break;
                default:
                    var val = series.Get(row);
                    if (val == null) json.WriteNullValue();
                    else json.WriteStringValue(Convert.ToString(val, CultureInfo.InvariantCulture));
                    break;
            }
        }
    }
}
