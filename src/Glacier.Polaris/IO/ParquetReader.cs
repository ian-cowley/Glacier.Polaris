using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// High-performance Parquet reader utilizing Parquet.Net for columnar extraction.
    /// </summary>
    public sealed class ParquetReader
    {
        private readonly string _filePath;
        private readonly string[]? _columns;

        public ParquetReader(string filePath, string[]? columns = null)
        {
            _filePath = filePath;
            _columns = columns;
        }

        public static string[] PeekHeaders(string filePath)
        {
            using var fileStream = File.OpenRead(filePath);
            using var reader = Parquet.ParquetReader.CreateAsync(fileStream).GetAwaiter().GetResult();
            return reader.Schema.GetDataFields().Select(f => f.Name).ToArray();
        }

        public async IAsyncEnumerable<DataFrame> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var fileStream = File.OpenRead(_filePath);
            using var reader = await Parquet.ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken).ConfigureAwait(false);

            var fields = reader.Schema.GetDataFields();
            if (_columns != null && _columns.Length > 0)
            {
                fields = fields.Where(f => _columns.Contains(f.Name)).ToArray();
            }

            if (reader.RowGroupCount <= 1)
            {
                for (int i = 0; i < reader.RowGroupCount; i++)
                {
                    yield return await ReadRowGroupAsync(reader, i, fields, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                var token = cts.Token;
                var channel = System.Threading.Channels.Channel.CreateBounded<DataFrame>(new System.Threading.Channels.BoundedChannelOptions(2)
                {
                    SingleWriter = true,
                    SingleReader = true,
                    FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
                });

                var producerTask = Task.Run(async () =>
                {
                    try
                    {
                        for (int i = 0; i < reader.RowGroupCount; i++)
                        {
                            token.ThrowIfCancellationRequested();
                            var df = await ReadRowGroupAsync(reader, i, fields, token).ConfigureAwait(false);
                            await channel.Writer.WriteAsync(df, token).ConfigureAwait(false);
                        }
                        channel.Writer.Complete();
                    }
                    catch (Exception ex)
                    {
                        channel.Writer.Complete(ex is OperationCanceledException ? null : ex);
                    }
                }, token);

                try
                {
                    await foreach (var df in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                    {
                        yield return df;
                    }
                }
                finally
                {
                    cts.Cancel();
                    try { await producerTask.ConfigureAwait(false); } catch { }
                }
            }
        }

        private static async Task<DataFrame> ReadRowGroupAsync(
            Parquet.ParquetReader reader,
            int rowGroupIndex,
            IReadOnlyList<DataField> fields,
            CancellationToken cancellationToken)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            int rowCount = (int)rowGroupReader.RowCount;
            var columns = new List<ISeries>(fields.Count);

            foreach (var field in fields)
            {
                var dataColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken).ConfigureAwait(false);
                var series = ConvertDataColumn(field, dataColumn, rowCount);
                if (series != null)
                {
                    columns.Add(series);
                }
            }

            return new DataFrame(columns);
        }

        private static ISeries? ConvertDataColumn(DataField field, DataColumn dataColumn, int rowCount)
        {
            var clrType = field.ClrType;

            if (clrType == typeof(int) || clrType == typeof(int?))
            {
                var series = new Int32Series(field.Name, rowCount);
                if (dataColumn.Data is int?[] nullableInts)
                {
                    var defLevels = dataColumn.DefinitionLevels;
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (defLevels != null && defLevels[j] == 0)
                            series.ValidityMask.SetNull(j);
                        else
                        {
                            series.Memory.Span[j] = nullableInts[j] ?? 0;
                            series.ValidityMask.SetValid(j);
                        }
                    }
                }
                else if (dataColumn.Data is int[] nonNullInts)
                {
                    nonNullInts.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(long) || clrType == typeof(long?))
            {
                var series = new Int64Series(field.Name, rowCount);
                if (dataColumn.Data is long?[] nullableLongs)
                {
                    var defLevels = dataColumn.DefinitionLevels;
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (defLevels != null && defLevels[j] == 0)
                            series.ValidityMask.SetNull(j);
                        else
                        {
                            series.Memory.Span[j] = nullableLongs[j] ?? 0;
                            series.ValidityMask.SetValid(j);
                        }
                    }
                }
                else if (dataColumn.Data is long[] nonNullLongs)
                {
                    nonNullLongs.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(double) || clrType == typeof(double?))
            {
                var series = new Float64Series(field.Name, rowCount);
                if (dataColumn.Data is double?[] nullableDoubles)
                {
                    var defLevels = dataColumn.DefinitionLevels;
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (defLevels != null && defLevels[j] == 0)
                            series.ValidityMask.SetNull(j);
                        else
                        {
                            series.Memory.Span[j] = nullableDoubles[j] ?? 0;
                            series.ValidityMask.SetValid(j);
                        }
                    }
                }
                else if (dataColumn.Data is double[] nonNullDoubles)
                {
                    nonNullDoubles.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(float) || clrType == typeof(float?))
            {
                var series = new Float32Series(field.Name, rowCount);
                if (dataColumn.Data is float?[] nullableFloats)
                {
                    var defLevels = dataColumn.DefinitionLevels;
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (defLevels != null && defLevels[j] == 0)
                            series.ValidityMask.SetNull(j);
                        else
                        {
                            series.Memory.Span[j] = nullableFloats[j] ?? 0f;
                            series.ValidityMask.SetValid(j);
                        }
                    }
                }
                else if (dataColumn.Data is float[] nonNullFloats)
                {
                    nonNullFloats.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(bool) || clrType == typeof(bool?))
            {
                var series = new BooleanSeries(field.Name, rowCount);
                if (dataColumn.Data is bool?[] nullableBools)
                {
                    var defLevels = dataColumn.DefinitionLevels;
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (defLevels != null && defLevels[j] == 0)
                            series.ValidityMask.SetNull(j);
                        else
                        {
                            series.Memory.Span[j] = nullableBools[j] ?? false;
                            series.ValidityMask.SetValid(j);
                        }
                    }
                }
                else if (dataColumn.Data is bool[] nonNullBools)
                {
                    nonNullBools.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(string))
            {
                if (dataColumn.Data is string?[] strArray)
                {
                    return Utf8StringSeries.FromStrings(field.Name, strArray);
                }
                return null;
            }

            if (clrType == typeof(sbyte) || clrType == typeof(sbyte?))
            {
                var series = new Int8Series(field.Name, rowCount);
                if (dataColumn.Data is sbyte?[] nullableSbytes)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableSbytes[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableSbytes[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is sbyte[] nonNullSbytes)
                {
                    nonNullSbytes.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(byte) || clrType == typeof(byte?))
            {
                var series = new UInt8Series(field.Name, rowCount);
                if (dataColumn.Data is byte?[] nullableBytes)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableBytes[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableBytes[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is byte[] nonNullBytes)
                {
                    nonNullBytes.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(short) || clrType == typeof(short?))
            {
                var series = new Int16Series(field.Name, rowCount);
                if (dataColumn.Data is short?[] nullableShorts)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableShorts[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableShorts[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is short[] nonNullShorts)
                {
                    nonNullShorts.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(ushort) || clrType == typeof(ushort?))
            {
                var series = new UInt16Series(field.Name, rowCount);
                if (dataColumn.Data is ushort?[] nullableUshorts)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableUshorts[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableUshorts[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is ushort[] nonNullUshorts)
                {
                    nonNullUshorts.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(uint) || clrType == typeof(uint?))
            {
                var series = new UInt32Series(field.Name, rowCount);
                if (dataColumn.Data is uint?[] nullableUints)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableUints[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableUints[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is uint[] nonNullUints)
                {
                    nonNullUints.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(ulong) || clrType == typeof(ulong?))
            {
                var series = new UInt64Series(field.Name, rowCount);
                if (dataColumn.Data is ulong?[] nullableUlongs)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableUlongs[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableUlongs[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is ulong[] nonNullUlongs)
                {
                    nonNullUlongs.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(DateTime) || clrType == typeof(DateTime?))
            {
                var series = new DatetimeSeries(field.Name, rowCount);
                var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                if (dataColumn.Data is DateTime?[] nullableDates)
                {
                    var defLevels = dataColumn.DefinitionLevels;
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (defLevels != null && defLevels[j] == 0)
                            series.ValidityMask.SetNull(j);
                        else if (nullableDates[j].HasValue)
                        {
                            series.Memory.Span[j] = (nullableDates[j]!.Value.ToUniversalTime().Ticks - epoch.Ticks) / 10;
                            series.ValidityMask.SetValid(j);
                        }
                        else
                        {
                            series.ValidityMask.SetNull(j);
                        }
                    }
                }
                else if (dataColumn.Data is DateTime[] nonNullDates)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        series.Memory.Span[j] = (nonNullDates[j].ToUniversalTime().Ticks - epoch.Ticks) / 10;
                    }
                }
                return series;
            }

            if (clrType == typeof(DateTimeOffset) || clrType == typeof(DateTimeOffset?))
            {
                var series = new DatetimeSeries(field.Name, rowCount);
                var epoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
                if (dataColumn.Data is DateTimeOffset?[] nullableOffsets)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableOffsets[j].HasValue)
                        {
                            series.Memory.Span[j] = (nullableOffsets[j]!.Value.ToUniversalTime().Ticks - epoch.Ticks) / 10;
                            series.ValidityMask.SetValid(j);
                        }
                        else
                        {
                            series.ValidityMask.SetNull(j);
                        }
                    }
                }
                else if (dataColumn.Data is DateTimeOffset[] nonNullOffsets)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        series.Memory.Span[j] = (nonNullOffsets[j].ToUniversalTime().Ticks - epoch.Ticks) / 10;
                    }
                }
                return series;
            }

            if (clrType == typeof(DateOnly) || clrType == typeof(DateOnly?))
            {
                var series = new DateSeries(field.Name, rowCount);
                var epoch = new DateOnly(1970, 1, 1);
                if (dataColumn.Data is DateOnly?[] nullableDates)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableDates[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableDates[j]!.Value.DayNumber - epoch.DayNumber;
                            series.ValidityMask.SetValid(j);
                        }
                        else
                        {
                            series.ValidityMask.SetNull(j);
                        }
                    }
                }
                else if (dataColumn.Data is DateOnly[] nonNullDates)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        series.Memory.Span[j] = nonNullDates[j].DayNumber - epoch.DayNumber;
                    }
                }
                return series;
            }

            if (clrType == typeof(decimal) || clrType == typeof(decimal?))
            {
                var series = new DecimalSeries(field.Name, rowCount);
                if (dataColumn.Data is decimal?[] nullableDecs)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableDecs[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableDecs[j]!.Value;
                            series.ValidityMask.SetValid(j);
                        }
                        else
                            series.ValidityMask.SetNull(j);
                    }
                }
                else if (dataColumn.Data is decimal[] nonNullDecs)
                {
                    nonNullDecs.CopyTo(series.Memory);
                }
                return series;
            }

            if (clrType == typeof(TimeSpan) || clrType == typeof(TimeSpan?))
            {
                var series = new DurationSeries(field.Name, rowCount);
                if (dataColumn.Data is TimeSpan?[] nullableTimes)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        if (nullableTimes[j].HasValue)
                        {
                            series.Memory.Span[j] = nullableTimes[j]!.Value.Ticks * 100;
                            series.ValidityMask.SetValid(j);
                        }
                        else
                        {
                            series.ValidityMask.SetNull(j);
                        }
                    }
                }
                else if (dataColumn.Data is TimeSpan[] nonNullTimes)
                {
                    for (int j = 0; j < rowCount; j++)
                    {
                        series.Memory.Span[j] = nonNullTimes[j].Ticks * 100;
                    }
                }
                return series;
            }

            return null;
        }
    }
}
