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
                    else if (field.ClrType == typeof(string))
                    {
                        var strings = (string[])dataColumn.Data;
                        var series = new Utf8StringSeries(field.Name, strings);
                        columns.Add(series);
                    }
                }

                yield return new DataFrame(columns);
            }
        }
    }
}
