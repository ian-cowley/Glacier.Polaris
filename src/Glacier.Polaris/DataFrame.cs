using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;

namespace Glacier.Polaris
{
    public enum JoinType
    {
        Inner,
        Left,
        Outer,
        Cross,
        AsOf,
        Semi,
        Anti
    }

    public class DataFrame
    {
        public List<ISeries> Columns { get; }
        public int RowCount => Columns.Count > 0 ? Columns[0].Length : 0;

        public virtual void Dispose()
        {
            foreach (var col in Columns) col.Dispose();
        }

        public DataFrame()
        {
            Columns = new List<ISeries>();
        }

        public DataFrame(IEnumerable<ISeries> columns)
        {
            Columns = columns.ToList();
        }
        public LazyFrame Lazy()
        {
            return LazyFrame.FromDataFrame(this);
        }

        public DataFrame Select(params Expr[] exprs)
        {
            return Lazy().Select(exprs).Collect().GetAwaiter().GetResult();
        }

        public DataFrame Select(params string[] columnNames)
        {
            return Select(columnNames.Select(Expr.Col).ToArray());
        }

        public DataFrame Filter(Expr predicate)
        {
            return Lazy().Filter(predicate).Collect().GetAwaiter().GetResult();
        }

        public DataFrame Sort(string columnName, bool descending = false)
        {
            return Lazy().Sort(columnName, descending).Collect().GetAwaiter().GetResult();
        }

        public DataFrame Sort(string[] columnNames, bool[] descending)
        {
            return Lazy().Sort(columnNames, descending).Collect().GetAwaiter().GetResult();
        }

        public DataFrame Sort(params string[] columnNames)
        {
            return Sort(columnNames, columnNames.Select(_ => false).ToArray());
        }

        public DataFrame Limit(int n)
        {
            return Lazy().Limit(n).Collect().GetAwaiter().GetResult();
        }
        public DataFrame Transpose(bool include_header = false, string header_name = "column", string[]? column_names = null)
        {
            return Compute.TransposeKernels.Transpose(this, include_header, header_name, column_names);
        }
        public ISeries GetColumn(string name) => Columns.First(c => c.Name == name);

        public static LazyFrame ScanCsv(string filePath) => LazyFrame.ScanCsv(filePath);
        public static LazyFrame ScanJson(string filePath) => LazyFrame.ScanJson(filePath);

        public void WriteCsv(string filePath, char separator = ',', bool includeHeader = true, bool includeBom = false)
        {
            IO.CsvWriter.Write(this, filePath, separator, includeHeader, includeBom);
        }

        public void WriteParquet(string filePath)
        {
            IO.ParquetWriter.Write(this, filePath);
        }

        public void WriteJson(string filePath, bool ndjson = true)
        {
            IO.JsonWriter.Write(this, filePath, ndjson);
        }

        public void WriteIpc(string filePath)
        {
            IO.IpcWriter.Write(this, filePath);
        }

        /// <summary>
        /// Writes the DataFrame to Arrow IPC stream format.
        /// Matching Polars' DataFrame.write_ipc(stream=True) behavior.
        /// </summary>
        public void WriteIpcStream(string filePath)
        {
            IO.IpcWriter.WriteStream(this, filePath);
        }

        public async Task<DataFrame> SortBy(params string[] columnNames)
        {
            await foreach (var df in this.Lazy().CollectAsync())
            {
                return df; // Return the first (and likely only) batch
            }
            return this;
        }

        public DataFrame SortByEager(params string[] columnNames)
        {
            // Implementation logic...
            return this;
        }

        public DataFrame Join(DataFrame other, string on, JoinType type = JoinType.Inner)
        {
            Data.Int32Series? leftCol = null;
            Data.Int32Series? rightCol = null;

            if (type != JoinType.Cross)
            {
                leftCol = this.GetColumn(on) as Data.Int32Series;
                rightCol = other.GetColumn(on) as Data.Int32Series;

                if (leftCol == null || rightCol == null)
                    throw new ArgumentException("Join requires Int32 column on the specified key.");
            }

            Compute.JoinKernels.JoinResult result;
            bool hasNulls = false;

            switch (type)
            {
                case JoinType.Inner:
                    result = Compute.JoinKernels.InnerJoin(leftCol!, rightCol!);
                    break;
                case JoinType.Left:
                    result = Compute.JoinKernels.LeftJoin(leftCol!, rightCol!);
                    hasNulls = true;
                    break;
                case JoinType.Outer:
                    result = Compute.JoinKernels.OuterJoin(leftCol!, rightCol!);
                    hasNulls = true;
                    break;
                case JoinType.Cross:
                    result = Compute.JoinKernels.CrossJoin(this.RowCount, other.RowCount);
                    break;
                case JoinType.AsOf:
                    result = Compute.JoinKernels.JoinAsof(leftCol!, rightCol!);
                    hasNulls = true;
                    break;
                case JoinType.Semi:
                    result = Compute.JoinKernels.SemiJoin(leftCol!, rightCol!);
                    break;
                case JoinType.Anti:
                    result = Compute.JoinKernels.AntiJoin(leftCol!, rightCol!);
                    break;
                default:
                    throw new NotSupportedException();
            }

            var newCols = new List<ISeries>();

            // Materialize Left Dataframe
            foreach (var col in this.Columns)
            {
                newCols.Add(MaterializeColumn(col, result.LeftIndices, hasNulls));
            }

            // Materialize Right Dataframe
            // For outer joins, all right columns (including the key) get materialized.
            // The right key column is renamed if there's a name conflict.
            // Python Polars behavior: outer join includes both the left key column (from left table,
            // null for unmatched right rows) and the right key column (renamed, null for unmatched left rows).
            foreach (var col in other.Columns)
            {
                // Skip join key for non-outer, non-cross joins
                if (type != JoinType.Cross && type != JoinType.Outer && col.Name == on) continue;

                string rightNewName = col.Name;
                if (this.Columns.Any(c => c.Name == col.Name))
                {
                    rightNewName += "_right";
                }

                var rightMatCol = MaterializeColumn(col, result.RightIndices, hasNulls);
                newCols.Add(RenameSeries(rightMatCol, rightNewName));
            }

            return new DataFrame(newCols);
        }

        public DataFrame JoinInner(DataFrame other, string on) => Join(other, on, JoinType.Inner);
        public DataFrame JoinLeft(DataFrame other, string on) => Join(other, on, JoinType.Left);
        public DataFrame JoinOuter(DataFrame other, string on) => Join(other, on, JoinType.Outer);
        public DataFrame JoinAsof(DataFrame other, string on) => Join(other, on, JoinType.AsOf);
        public DataFrame JoinSemi(DataFrame other, string on) => Join(other, on, JoinType.Semi);
        public DataFrame JoinAnti(DataFrame other, string on) => Join(other, on, JoinType.Anti);
        public DataFrame JoinCross(DataFrame other) => Join(other, "", JoinType.Cross);

        private ISeries RenameSeries(ISeries series, string newName)
        {
            if (series.Name == newName) return series;
            if (series is Data.Int32Series i32) { var n = new Data.Int32Series(newName, i32.Length); i32.Memory.Span.CopyTo(n.Memory.Span); n.ValidityMask.CopyFrom(i32.ValidityMask); return n; }
            if (series is Data.Float64Series f64) { var n = new Data.Float64Series(newName, f64.Length); f64.Memory.Span.CopyTo(n.Memory.Span); n.ValidityMask.CopyFrom(f64.ValidityMask); return n; }
            if (series is Data.Utf8StringSeries u8) { var n = new Data.Utf8StringSeries(newName, u8.Length, u8.DataBytes.Length); u8.Offsets.Span.CopyTo(n.Offsets.Span); u8.DataBytes.Span.CopyTo(n.DataBytes.Span); n.ValidityMask.CopyFrom(u8.ValidityMask); return n; }
            throw new NotSupportedException();
        }

        public DataFrame Pivot(string[] index, string pivot, string values, string agg = "sum")
        {
            return Compute.PivotKernels.Pivot(this, index, pivot, values, agg);
        }

        public DataFrame Pivot(string index, string pivot, string values, string agg = "sum")
        {
            return Pivot(new[] { index }, pivot, values, agg);
        }

        public DataFrame Melt(string[] idVars, string[] valueVars, string variableName = "variable", string valueName = "value")
        {
            return Compute.MeltKernels.Melt(this, idVars, valueVars, variableName, valueName);
        }

        public GroupByBuilder GroupBy(params string[] columnNames)
        {
            return new GroupByBuilder(this, columnNames);
        }

        /// <summary>
        /// Returns a new DataFrame with duplicate rows removed.
        /// Uses first column's unique values and filters all columns to match.
        /// </summary>
        public DataFrame Unique()
        {
            if (RowCount <= 1) return this;
            var firstCol = Columns[0];
            var indices = Compute.UniqueKernels.UniqueIndices(firstCol);
            var indicesArr = indices.ToArray();
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < indicesArr.Length; i++) totalBytes += u8.GetStringSpan(indicesArr[i]).Length;
                    newCol = new Data.Utf8StringSeries(col.Name, indicesArr.Length, totalBytes);
                }
                else
                {
                    newCol = (ISeries)System.Activator.CreateInstance(col.GetType(), col.Name, indicesArr.Length)!;
                }
                col.Take(newCol, new ReadOnlySpan<int>(indicesArr));
                newCols.Add(newCol);
            }
            return new DataFrame(newCols);
        }


        public static DataFrame Concat(IEnumerable<DataFrame> chunks)

        {
            var results = chunks.Where(df => df.RowCount > 0).ToList();
            if (results.Count == 0) return new DataFrame();
            if (results.Count == 1) return results[0];

            int totalRows = results.Sum(df => df.RowCount);
            var firstDf = results[0];
            var newCols = new List<ISeries>();

            foreach (var col in firstDf.Columns)
            {
                if (col is Data.Int32Series)
                {
                    var newCol = new Data.Int32Series(col.Name, totalRows);
                    int offset = 0;
                    foreach (var df in results)
                    {
                        var c = (Data.Int32Series)df.GetColumn(col.Name);
                        c.Memory.Span.CopyTo(newCol.Memory.Span.Slice(offset));
                        offset += c.Length;
                    }
                    newCols.Add(newCol);
                }
                else if (col is Data.Float64Series)
                {
                    var newCol = new Data.Float64Series(col.Name, totalRows);
                    int offset = 0;
                    foreach (var df in results)
                    {
                        var c = (Data.Float64Series)df.GetColumn(col.Name);
                        c.Memory.Span.CopyTo(newCol.Memory.Span.Slice(offset));
                        offset += c.Length;
                    }
                    newCols.Add(newCol);
                }
                else if (col is Data.Utf8StringSeries)
                {
                    int totalBytes = results.Sum(df => ((Data.Utf8StringSeries)df.GetColumn(col.Name)).DataBytes.Length);
                    var newCol = new Data.Utf8StringSeries(col.Name, totalRows, totalBytes);
                    int rowOffset = 0;
                    int byteOffset = 0;

                    var newOffsets = newCol.Offsets.Span;
                    var newData = newCol.DataBytes.Span;

                    foreach (var df in results)
                    {
                        var c = (Data.Utf8StringSeries)df.GetColumn(col.Name);
                        var cOffsets = c.Offsets.Span;
                        var cData = c.DataBytes.Span;

                        cData.CopyTo(newData.Slice(byteOffset));
                        for (int i = 0; i < c.Length; i++)
                        {
                            newOffsets[rowOffset + i] = byteOffset + cOffsets[i];
                        }

                        rowOffset += c.Length;
                        byteOffset += cData.Length;
                    }
                    newOffsets[totalRows] = byteOffset;
                    newCols.Add(newCol);
                }
            }

            return new DataFrame(newCols);
        }
        public static DataFrame FromSqlReader(System.Data.IDataReader reader)
        {
            var columns = new List<ISeries>();
            int colCount = reader.FieldCount;

            var colData = new List<List<object?>>();
            for (int i = 0; i < colCount; i++) colData.Add(new List<object?>());

            int rowCount = 0;
            while (reader.Read())
            {
                for (int i = 0; i < colCount; i++)
                {
                    var val = reader.GetValue(i);
                    colData[i].Add(val == DBNull.Value ? null : val);
                }
                rowCount++;
            }

            for (int i = 0; i < colCount; i++)
            {
                string name = reader.GetName(i);
                var type = reader.GetFieldType(i);
                var data = colData[i];

                if (type == typeof(int)) columns.Add(Data.Int32Series.FromValues(name, data.Select(v => v == null ? (int?)null : Convert.ToInt32(v)).ToArray()));
                else if (type == typeof(double)) columns.Add(Data.Float64Series.FromValues(name, data.Select(v => v == null ? (double?)null : Convert.ToDouble(v)).ToArray()));
                else if (type == typeof(string)) columns.Add(Data.Utf8StringSeries.FromStrings(name, data.Select(v => v?.ToString()).ToArray()));
                else columns.Add(Data.Utf8StringSeries.FromStrings(name, data.Select(v => v?.ToString()).ToArray())); // Fallback
            }

            return new DataFrame(columns);
        }
        /// <summary>
        /// Materializes a column by taking rows at the given indices.
        /// If hasNulls is true, indices == -1 are treated as null entries.
        /// </summary>
        internal ISeries MaterializeColumn(ISeries source, int[] indices, bool hasNulls)
        {
            // Validation
            for (int i = 0; i < indices.Length; i++)
            {
                if (indices[i] >= source.Length)
                    throw new ArgumentOutOfRangeException(nameof(indices), $"Index {indices[i]} is out of range for series {source.Name} of length {source.Length}");
            }
            var target = (ISeries)Activator.CreateInstance(source.GetType(), source.Name, indices.Length)!;
            source.Take(target, indices);

            // For joins with null entries (outer, left, asof), mark -1 indices as null
            if (hasNulls)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    if (indices[i] == -1)
                        target.ValidityMask.SetNull(i);
                }
            }

            return target;
        }

        public RecordBatch ToArrowRecordBatch()
        {
            var schemaBuilder = new Schema.Builder();
            var arrays = new List<IArrowArray>();

            foreach (var col in Columns)
            {
                var arrowArr = col.ToArrowArray();
                schemaBuilder.Field(f => f.Name(col.Name).DataType(arrowArr.Data.DataType).Nullable(col.ValidityMask.HasNulls));
                arrays.Add(arrowArr);
            }

            return new RecordBatch(schemaBuilder.Build(), arrays, RowCount);
        }

        public RecordBatch ToArrow() => ToArrowRecordBatch();

        public static DataFrame FromArrowRecordBatch(RecordBatch batch)
        {
            var columns = new List<ISeries>();
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                var field = batch.Schema.GetFieldByIndex(i);
                var array = batch.Column(i);
                columns.Add(FromArrowArray(field.Name, array));
            }
            return new DataFrame(columns);
        }

        public static DataFrame FromArrow(RecordBatch batch) => FromArrowRecordBatch(batch);

        private static ISeries FromArrowArray(string name, IArrowArray array)
        {
            if (array is Int32Array i32)
            {
                var s = new Data.Int32Series(name, i32.Length);
                i32.Values.CopyTo(s.Memory.Span);
                for (int i = 0; i < i32.Length; i++)
                {
                    if (i32.IsNull(i)) s.ValidityMask.SetNull(i);
                    else s.ValidityMask.SetValid(i);
                }
                return s;
            }
            if (array is DoubleArray f64)
            {
                var s = new Data.Float64Series(name, f64.Length);
                f64.Values.CopyTo(s.Memory.Span);
                for (int i = 0; i < f64.Length; i++)
                {
                    if (f64.IsNull(i)) s.ValidityMask.SetNull(i);
                    else s.ValidityMask.SetValid(i);
                }
                return s;
            }
            if (array is StringArray str)
            {
                var strings = new string?[str.Length];
                for (int i = 0; i < str.Length; i++) strings[i] = str.GetString(i);
                return Data.Utf8StringSeries.FromStrings(name, strings);
            }
            if (array is BooleanArray bl)
            {
                var s = new Data.BooleanSeries(name, bl.Length);
                for (int i = 0; i < bl.Length; i++)
                {
                    var val = bl.GetValue(i);
                    if (val.HasValue)
                    {
                        s.Memory.Span[i] = val.Value;
                        s.ValidityMask.SetValid(i);
                    }
                    else s.ValidityMask.SetNull(i);
                }
                return s;
            }
            if (array is StructArray sa)
            {
                var structType = (StructType)sa.Data.DataType;
                var fields = new ISeries[sa.Fields.Count];
                for (int i = 0; i < sa.Fields.Count; i++)
                {
                    fields[i] = FromArrowArray(structType.Fields[i].Name, sa.Fields[i]);
                }
                var s = new Data.StructSeries(name, fields);
                for (int i = 0; i < sa.Length; i++)
                {
                    if (sa.IsNull(i)) s.ValidityMask.SetNull(i);
                    else s.ValidityMask.SetValid(i);
                }
                return s;
            }

            if (array is Decimal128Array dec)
            {
                var decType = (Decimal128Type)dec.Data.DataType;
                var values = new decimal?[dec.Length];
                for (int i = 0; i < dec.Length; i++)
                    values[i] = dec.IsNull(i) ? null : dec.GetValue(i);
                return new Data.DecimalSeries(name, values, decType.Precision, decType.Scale);
            }
            if (array is BinaryArray bin)
            {
                var values = new byte[]?[bin.Length];
                for (int i = 0; i < bin.Length; i++)
                {
                    if (bin.IsNull(i)) values[i] = null;
                    else
                    {
                        var slice = bin.Data.Buffers[2].Span;
                        int start = bin.ValueOffsets[i];
                        int end = bin.ValueOffsets[i + 1];
                        values[i] = slice.Slice(start, end - start).ToArray();
                    }
                }
                return new Data.BinarySeries(name, values);
            }
            if (array is NullArray nullArr)
            {
                return new Data.NullSeries(name, nullArr.Length);
            }
            if (array is Time64Array time)
            {
                var values = new long[time.Length];
                for (int i = 0; i < time.Length; i++)
                    values[i] = time.IsNull(i) ? 0 : time.GetValue(i)!.Value;
                var s = new Data.TimeSeries(name, time.Length);
                values.CopyTo(s.Memory.Span);
                for (int i = 0; i < time.Length; i++)
                {
                    if (time.IsNull(i)) s.ValidityMask.SetNull(i);
                    else s.ValidityMask.SetValid(i);
                }
                return s;
            }
            throw new NotSupportedException($"Arrow type {array.Data.DataType.Name} not supported yet.");
        }
        /// <summary>
        /// Returns a new DataFrame containing only the last n rows.
        /// </summary>
        /// <summary>
        /// Returns a new DataFrame with rows from offset (inclusive) to offset + length.
        /// </summary>
        public DataFrame Slice(int offset, int length)
        {
            if (offset < 0) offset = RowCount + offset;
            if (offset < 0) offset = 0;
            if (offset >= RowCount) return new DataFrame(Columns.Select(c => c.CloneEmpty(0)).ToList());
            int actualLength = Math.Min(length, RowCount - offset);
            if (actualLength <= 0) return new DataFrame(Columns.Select(c => c.CloneEmpty(0)).ToList());
            var indices = Enumerable.Range(offset, actualLength).ToArray();
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < indices.Length; i++) totalBytes += u8.GetStringSpan(indices[i]).Length;
                    newCol = new Data.Utf8StringSeries(col.Name, actualLength, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, actualLength)!;
                }
                col.Take(newCol, indices);
                newCols.Add(newCol);
            }
            return new DataFrame(newCols);
        }

        /// <summary>
        /// Returns a new DataFrame containing only the last n rows.
        /// </summary>
        public DataFrame Tail(int n)
        {
            if (n <= 0) return new DataFrame(Columns.Select(c => c.CloneEmpty(0)).ToList());
            if (n >= RowCount) return this;
            int start = RowCount - n;
            var indices = Enumerable.Range(start, n).ToArray();
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = u8.Offsets.Span[n + start] - u8.Offsets.Span[start];
                    newCol = new Data.Utf8StringSeries(col.Name, n, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, n)!;
                }
                col.Take(newCol, indices);
                newCols.Add(newCol);
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Returns a new DataFrame with n randomly selected rows.
        /// With replacement=true, same row can appear multiple times.
        /// </summary>
        public DataFrame Sample(int n, bool withReplacement = false)
        {
            if (n <= 0) return new DataFrame(Columns.Select(c => c.CloneEmpty(0)).ToList());
            if (n > RowCount && !withReplacement) n = RowCount;

            var rng = new Random(42); // deterministic seed for reproducibility
            var indices = new int[n];
            if (withReplacement)
            {
                for (int i = 0; i < n; i++)
                    indices[i] = rng.Next(RowCount);
            }
            else
            {
                // Fisher-Yates partial shuffle
                var pool = Enumerable.Range(0, RowCount).ToArray();
                int take = Math.Min(n, RowCount);
                for (int i = 0; i < take; i++)
                {
                    int j = rng.Next(i, RowCount);
                    (pool[i], pool[j]) = (pool[j], pool[i]);
                    indices[i] = pool[i];
                }
            }

            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = u8.Offsets.Span[indices[^1] + 1] - u8.Offsets.Span[0]; // rough estimate
                    totalBytes = Math.Max(8, totalBytes);
                    newCol = new Data.Utf8StringSeries(col.Name, n, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, n)!;
                }
                col.Take(newCol, indices);
                newCols.Add(newCol);
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Returns a statistical summary of numeric columns: count, null_count, mean, std, min, 25%, 50%, 75%, max.
        /// Matches Polars' DataFrame.describe() output format.
        /// </summary>
        public DataFrame Describe()
        {
            var statNames = new[] { "count", "null_count", "mean", "std", "min", "25%", "50%", "75%", "max" };
            var numericCols = Columns.Where(c => c is Data.Int32Series or Data.Float64Series).ToList();

            if (numericCols.Count == 0)
                return new DataFrame(new ISeries[] { Data.Utf8StringSeries.FromStrings("stat", statNames) });

            // Build stat rows: each row is a list of string values per column
            var statValues = new List<List<string?>>();

            foreach (var statName in statNames)
            {
                var row = new List<string?>();
                row.Add(statName);
                for (int i = 0; i < numericCols.Count; i++)
                {
                    var col = numericCols[i];
                    string? val = null;
                    switch (statName)
                    {
                        case "count":
                            val = (col.Length - col.ValidityMask.NullCount).ToString();
                            break;
                        case "null_count":
                            val = col.ValidityMask.NullCount.ToString();
                            break;
                        case "mean":
                            val = FormatScalar(Compute.AggregationKernels.Mean(col));
                            break;
                        case "std":
                            val = FormatScalar(Compute.AggregationKernels.Std(col));
                            break;
                        case "min":
                            val = FormatScalar(Compute.AggregationKernels.Min(col));
                            break;
                        case "25%":
                            val = FormatScalar(Compute.AggregationKernels.Quantile(col, 0.25));
                            break;
                        case "50%":
                            val = FormatScalar(Compute.AggregationKernels.Quantile(col, 0.50));
                            break;
                        case "75%":
                            val = FormatScalar(Compute.AggregationKernels.Quantile(col, 0.75));
                            break;
                        case "max":
                            val = FormatScalar(Compute.AggregationKernels.Max(col));
                            break;
                    }
                    row.Add(val);
                }
                statValues.Add(row);
            }

            var columnNames = new[] { "stat" }.Concat(numericCols.Select(c => c.Name)).ToArray();
            var seriesList = new List<ISeries>();
            for (int colIdx = 0; colIdx < columnNames.Length; colIdx++)
            {
                var colValues = statValues.Select(row => row[colIdx]).ToArray();
                seriesList.Add(Data.Utf8StringSeries.FromStrings(columnNames[colIdx], colValues));
            }
            return new DataFrame(seriesList);
        }
        private static string? FormatScalar(ISeries series)
        {
            if (series == null || series.Length == 0) return null;
            var val = series.Get(0);
            if (val == null) return null;
            if (val is double d)
                return d.ToString("G6", System.Globalization.CultureInfo.InvariantCulture);
            return val.ToString();
        }
        /// <summary>
        /// Returns a new DataFrame with rows removed where any (or all) subset columns have null values.
        /// Matching Polars' DataFrame.drop_nulls() behavior.
        /// </summary>
        public DataFrame DropNulls(string[]? subset = null, bool anyNull = true)
        {
            var cols = subset != null
                ? Columns.Where(c => subset.Contains(c.Name)).ToList()
                : Columns;
            if (cols.Count == 0) return this;

            var mask = new bool[RowCount];

            if (anyNull)
            {
                // Remove row if ANY subset column is null
                for (int i = 0; i < mask.Length; i++) mask[i] = true;
                foreach (var col in cols)
                {
                    for (int i = 0; i < col.Length; i++)
                    {
                        if (!col.ValidityMask.IsValid(i))
                            mask[i] = false;
                    }
                }
            }
            else
            {
                // Remove row only if ALL subset columns are null
                for (int i = 0; i < mask.Length; i++) mask[i] = true;
                foreach (var col in cols)
                {
                    for (int i = 0; i < col.Length; i++)
                    {
                        if (col.ValidityMask.IsValid(i))
                            mask[i] = false;
                    }
                }
                // Invert: mask is now "is this row null in ALL columns?"
                // We want rows that are NOT all-null
                mask = mask.Select(m => !m).ToArray();
            }

            var keepIndices = Enumerable.Range(0, RowCount).Where(i => mask[i]).ToArray();
            if (keepIndices.Length == RowCount) return this;
            return MaterializeRows(keepIndices);
        }        /// <summary>
                 /// Materializes a new DataFrame from the given row indices.
                 /// </summary>
        private DataFrame MaterializeRows(int[] indices)
        {
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < indices.Length; i++)
                        totalBytes += u8.GetStringSpan(indices[i]).Length;
                    newCol = new Data.Utf8StringSeries(col.Name, indices.Length, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, indices.Length)!;
                }
                col.Take(newCol, indices);
                newCols.Add(newCol);
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Fills NaN (double.NaN) values in Float64 columns with the specified value.
        /// Matching Polars' DataFrame.fill_nan() behavior.
        /// </summary>
        public DataFrame FillNan(double value)
        {
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                if (col is Data.Float64Series f64)
                {
                    var newCol = new Data.Float64Series(col.Name, f64.Length);
                    f64.Memory.Span.CopyTo(newCol.Memory.Span);
                    newCol.ValidityMask.CopyFrom(f64.ValidityMask);
                    for (int i = 0; i < f64.Length; i++)
                    {
                        if (double.IsNaN(f64.Memory.Span[i]))
                            newCol.Memory.Span[i] = value;
                    }
                    newCols.Add(newCol);
                }
                else
                {
                    newCols.Add(col);
                }
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Returns a new DataFrame with a column containing the row index (0-based) as Int32.
        /// Matching Polars' DataFrame.with_row_index() behavior.
        /// </summary>
        public DataFrame WithRowIndex(string name = "index")
        {
            var newCols = new List<ISeries>(Columns.Count + 1);
            var idxCol = new Data.Int32Series(name, RowCount);
            for (int i = 0; i < RowCount; i++) idxCol.Memory.Span[i] = i;
            newCols.Add(idxCol);
            foreach (var col in Columns)
            {
                // If a column already has this name, it gets renamed (like Polars)
                if (col.Name == name) continue;
                newCols.Add(col);
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Renames columns in the DataFrame using the provided mapping from old names to new names.
        /// Matching Polars' DataFrame.rename() behavior.
        /// </summary>
        public DataFrame Rename(Dictionary<string, string> mapping)
        {
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                if (mapping.TryGetValue(col.Name, out var newName))
                {
                    var renamed = (ISeries)Activator.CreateInstance(col.GetType(), newName, col.Length)!;
                    col.CopyTo(renamed, 0);
                    if (col is Data.Utf8StringSeries u8)
                    {
                        // Copy string data
                        var target = (Data.Utf8StringSeries)renamed;
                        u8.Offsets.Span.CopyTo(target.Offsets.Span);
                        u8.DataBytes.Span.CopyTo(target.DataBytes.Span);
                    }
                    renamed.ValidityMask.CopyFrom(col.ValidityMask);
                    newCols.Add(renamed);
                }
                else
                {
                    newCols.Add(col);
                }
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Returns a new DataFrame with the count of null values per column.
        /// Matching Polars' DataFrame.null_count() behavior.
        /// </summary>
        public DataFrame NullCount()
        {
            var statCol = Data.Utf8StringSeries.FromStrings("column", Columns.Select(c => c.Name).ToArray());
            var countCol = new Data.Int32Series("null_count", Columns.Count);
            for (int i = 0; i < Columns.Count; i++)
                countCol.Memory.Span[i] = Columns[i].ValidityMask.NullCount;
            return new DataFrame(new ISeries[] { statCol, countCol });
        }
        /// <summary>
        /// Gets the schema of the DataFrame as a dictionary of column name -> data type.
        /// Matching Polars' DataFrame.schema property.
        /// </summary>
        public Dictionary<string, Type> Schema => Columns.ToDictionary(c => c.Name, c => c.DataType);

        /// <summary>
        /// Gets the data types of each column.
        /// Matching Polars' DataFrame.dtypes property.
        /// </summary>
        public Type[] Dtypes => Columns.Select(c => c.DataType).ToArray();

        /// <summary>
        /// Returns the estimated memory usage of the DataFrame in bytes.
        /// Matching Polars' DataFrame.estimated_size() behavior.
        /// </summary>
        public long EstimatedSize()
        {
            long total = 0;
            foreach (var col in Columns)
            {
                total += EstimateColumnSize(col);
            }
            return total;
        }

        private static long EstimateColumnSize(ISeries series)
        {
            long size = 0;

            // ValidityMask: bit-packed, ulong per 64 elements
            int ulongCount = (series.Length + 63) / 64;
            size += ulongCount * sizeof(ulong);

            if (series is Data.Utf8StringSeries u8)
            {
                size += u8.DataBytes.Length;
                size += (u8.Length + 1) * sizeof(int);
            }
            else if (series is Data.BinarySeries bin)
            {
                size += bin.DataBytes.Length;
                size += (bin.Length + 1) * sizeof(int);
            }
            else if (series is Data.ListSeries list)
            {
                size += (list.Length + 1) * sizeof(int);
                size += EstimateColumnSize(list.Values);
            }
            else if (series is Data.StructSeries st)
            {
                foreach (var field in st.Fields)
                    size += EstimateColumnSize(field);
            }
            else if (series is Data.ArraySeries arr)
            {
                size += EstimateColumnSize(arr.Values);
            }
            else if (series is Data.NullSeries)
            {
                // just the validity mask
            }
            else if (series is Data.ObjectSeries)
            {
                size += series.Length * IntPtr.Size;
            }
            else if (series is Data.CategoricalSeries cat)
            {
                size += series.Length * sizeof(uint);
                foreach (var s in cat.RevMap)
                {
                    if (s != null)
                        size += System.Text.Encoding.UTF8.GetByteCount(s) + IntPtr.Size;
                }
            }
            else if (series is Data.EnumSeries en)
            {
                size += series.Length * sizeof(uint);
                foreach (var s in en.Categories)
                {
                    if (s != null)
                        size += System.Text.Encoding.UTF8.GetByteCount(s) + IntPtr.Size;
                }
            }
            else if (series is Data.DecimalSeries)
            {
                size += series.Length * 16; // decimal is 16 bytes on .NET
            }
            else
            {
                // Series<T> for numeric types - determine element size from generic type
                var type = series.GetType();
                if (type.BaseType != null && type.BaseType.IsGenericType &&
                    type.BaseType.GetGenericTypeDefinition() == typeof(Data.Series<>))
                {
                    var elemType = type.BaseType.GetGenericArguments()[0];
                    if (elemType == typeof(int) || elemType == typeof(float) || elemType == typeof(uint)) size += series.Length * 4;
                    else if (elemType == typeof(long) || elemType == typeof(double) || elemType == typeof(ulong)) size += series.Length * 8;
                    else if (elemType == typeof(short) || elemType == typeof(ushort)) size += series.Length * 2;
                    else if (elemType == typeof(sbyte) || elemType == typeof(byte)) size += series.Length * 1;
                    else if (elemType == typeof(bool)) size += series.Length / 8 + 1;
                    else size += series.Length * 4;
                }
                else
                {
                    size += series.Length * 8L;
                }
            }

            return size;
        }

        /// <summary>
        /// Converts the DataFrame to a dictionary mapping column names to lists of values.
        /// Matching Polars' DataFrame.to_dict() behavior.
        /// </summary>
        public Dictionary<string, List<object?>> ToDictionary()
        {
            var result = new Dictionary<string, List<object?>>(Columns.Count);
            foreach (var col in Columns)
            {
                var values = new List<object?>(RowCount);
                for (int i = 0; i < RowCount; i++)
                {
                    values.Add(col.Get(i));
                }
                result[col.Name] = values;
            }
            return result;
        }

        /// <summary>
        /// Converts the DataFrame to a <see cref="System.Data.DataTable"/>.
        /// Each column becomes a typed <see cref="DataColumn"/> with values copied.
        /// Nulls are represented as <see cref="DBNull.Value"/>.
        /// This is the .NET ecosystem equivalent of Python Polars' DataFrame.to_pandas().
        /// </summary>
        public System.Data.DataTable ToDataTable()
        {
            var table = new System.Data.DataTable();
            foreach (var col in Columns)
            {
                var dtCol = new System.Data.DataColumn(col.Name, col.DataType);
                table.Columns.Add(dtCol);
            }

            for (int row = 0; row < RowCount; row++)
            {
                var rowValues = new object?[Columns.Count];
                for (int c = 0; c < Columns.Count; c++)
                {
                    if (Columns[c].ValidityMask.IsValid(row))
                        rowValues[c] = Columns[c].Get(row);
                    else
                        rowValues[c] = DBNull.Value;
                }
                table.Rows.Add(rowValues);
            }

            return table;
        }

        /// <summary>
        /// Creates a deep copy of the DataFrame with independent column data.
        /// Matching Polars' DataFrame.clone() behavior.
        /// </summary>
        public DataFrame Clone()
        {
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    newCol = new Data.Utf8StringSeries(col.Name, col.Length, u8.DataBytes.Length);
                    u8.Offsets.Span.CopyTo(((Data.Utf8StringSeries)newCol).Offsets.Span);
                    u8.DataBytes.Span.CopyTo(((Data.Utf8StringSeries)newCol).DataBytes.Span);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, col.Length)!;
                    col.CopyTo(newCol, 0);
                }
                newCol.ValidityMask.CopyFrom(col.ValidityMask);
                newCols.Add(newCol);
            }
            return new DataFrame(newCols);
        }
        /// <summary>
        /// Remove all rows from the DataFrame while preserving the schema. Polars API: clear()
        /// </summary>
        public DataFrame Clear()
        {
            var newCols = new List<ISeries>(Columns.Count);
            foreach (var col in Columns)
            {
                newCols.Add(col.CloneEmpty(0));
            }
            return new DataFrame(newCols);
        }

/// <summary>
/// Shrink memory usage by reallocating columns to their actual sizes. Polars API: shrink_to_fit()
/// </summary>
public DataFrame ShrinkToFit()
{
// For now, ShrinkToFit is a no-op since our columns are already single-chunk.
// In the future, this could call Compact/TrimExcess on internal buffers.
return this;
}

/// <summary>
/// Apply a user-defined function to the DataFrame. Polars API: map()
/// </summary>
public DataFrame Map(Func<DataFrame, DataFrame> func)
{
return func(this);
}    }

    public interface ISeries : IDisposable
    {
        string Name { get; }
        void Rename(string name);
        Type DataType { get; }
        int Length { get; }
        Glacier.Polaris.Memory.ValidityMask ValidityMask { get; }
        object? Get(int i);
        ISeries CloneEmpty(int length);
        Apache.Arrow.IArrowArray ToArrowArray();
        void CopyTo(ISeries target, int offset);
        void Take(ISeries target, ReadOnlySpan<int> indices);
        void Take(ISeries target, int srcIdx, int targetIdx);
        DataFrame ValueCounts(bool sort = false, bool parallel = true);
        ISeries IsFirst();
        double Entropy();
        int ApproxNUnique();
        ISeries MapElements(Func<object?, object?> mapping, Type returnType);
    }
}
