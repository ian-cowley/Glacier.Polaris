using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Parquet;
using Parquet.Data;

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
            using var reader = await Parquet.ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken);

            var fields = reader.Schema.GetDataFields();
            if (_columns != null && _columns.Length > 0)
            {
                fields = fields.Where(f => _columns.Contains(f.Name)).ToArray();
            }

            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using var rowGroupReader = reader.OpenRowGroupReader(i);
                var columns = new List<ISeries>();

                foreach (var field in fields)
                {
                    var dataColumn = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
                    
                    if (field.ClrType == typeof(int) || field.ClrType == typeof(int?))
                    {
                        var series = new Int32Series(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is int?[] nullableInts)
                        {
                            var defLevels = dataColumn.DefinitionLevels;
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is int[] nonNullInts)
                        {
                            nonNullInts.CopyTo(series.Memory);
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(long) || field.ClrType == typeof(long?))
                    {
                        var series = new Int64Series(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is long?[] nullableLongs)
                        {
                            var defLevels = dataColumn.DefinitionLevels;
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is long[] nonNullLongs)
                        {
                            nonNullLongs.CopyTo(series.Memory);
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(double) || field.ClrType == typeof(double?))
                    {
                        var series = new Float64Series(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is double?[] nullableDoubles)
                        {
                            var defLevels = dataColumn.DefinitionLevels;
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is double[] nonNullDoubles)
                        {
                            nonNullDoubles.CopyTo(series.Memory);
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(float) || field.ClrType == typeof(float?))
                    {
                        var series = new Float32Series(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is float?[] nullableFloats)
                        {
                            var defLevels = dataColumn.DefinitionLevels;
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is float[] nonNullFloats)
                        {
                            nonNullFloats.CopyTo(series.Memory);
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(bool) || field.ClrType == typeof(bool?))
                    {
                        var series = new BooleanSeries(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is bool?[] nullableBools)
                        {
                            var defLevels = dataColumn.DefinitionLevels;
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is bool[] nonNullBools)
                        {
                            nonNullBools.CopyTo(series.Memory);
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(string))
                    {
                        if (dataColumn.Data is string?[] strArray)
                        {
                            var series = Utf8StringSeries.FromStrings(field.Name, strArray);
                            columns.Add(series);
                        }
                    }
                    else if (field.ClrType == typeof(DateTime) || field.ClrType == typeof(DateTime?))
                    {
                        var series = new DatetimeSeries(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        if (rawData is DateTime?[] nullableDates)
                        {
                            var defLevels = dataColumn.DefinitionLevels;
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is DateTime[] nonNullDates)
                        {
                            for (int j = 0; j < series.Length; j++)
                            {
                                series.Memory.Span[j] = (nonNullDates[j].ToUniversalTime().Ticks - epoch.Ticks) / 10;
                            }
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(decimal) || field.ClrType == typeof(decimal?))
                    {
                        var series = new DecimalSeries(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is decimal?[] nullableDecs)
                        {
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is decimal[] nonNullDecs)
                        {
                            nonNullDecs.CopyTo(series.Memory);
                        }
                        columns.Add(series);
                    }
                    else if (field.ClrType == typeof(TimeSpan) || field.ClrType == typeof(TimeSpan?))
                    {
                        var series = new DurationSeries(field.Name, (int)rowGroupReader.RowCount);
                        var rawData = dataColumn.Data;
                        if (rawData is TimeSpan?[] nullableTimes)
                        {
                            for (int j = 0; j < series.Length; j++)
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
                        else if (rawData is TimeSpan[] nonNullTimes)
                        {
                            for (int j = 0; j < series.Length; j++)
                            {
                                series.Memory.Span[j] = nonNullTimes[j].Ticks * 100;
                            }
                        }
                        columns.Add(series);
                    }
                }

                yield return new DataFrame(columns);
            }
        }
    }
}
