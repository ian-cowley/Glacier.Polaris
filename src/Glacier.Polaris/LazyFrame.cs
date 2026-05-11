using System;
using System.Linq.Expressions;
using System.Collections.Generic;

namespace Glacier.Polaris
{
    /// <summary>
    /// Represents a deferred execution plan for a DataFrame.
    /// Accumulates operations as an expression tree.
    /// </summary>
    public sealed class LazyFrame
    {
        public Expression Plan { get; }

        public LazyFrame(Expression plan)
        {
            Plan = plan;
        }

        public LazyFrame Filter(Expression<Func<Expr, Expr>> predicate)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(FilterOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Quote(predicate));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Filter(Expr predicate)
        {
            return Filter(_ => predicate);
        }

        public LazyFrame Select(params Expr[] selections)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(SelectOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var arrayExpr = Expression.NewArrayInit(typeof(Expr), selections.Select(s => Expression.Constant(s, typeof(Expr))));
            var methodCall = Expression.Call(null, method, Plan, arrayExpr);
            return new LazyFrame(methodCall);
        }

        public LazyFrame Select(params Expression<Func<Expr, Expr>>[] selections)
        {
            var exprs = selections.Select(s => s.Compile().Invoke(null!)).ToArray();
            return Select(exprs);
        }

        public LazyFrame GroupBy(params string[] columns)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(GroupByOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var arrayExpr = Expression.NewArrayInit(typeof(string), Array.ConvertAll(columns, Expression.Constant));
            var methodCall = Expression.Call(null, method, Plan, arrayExpr);
            return new LazyFrame(methodCall);
        }

        public LazyFrame Agg(params Expr[] aggregations)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(AggOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var arrayExpr = Expression.NewArrayInit(typeof(Expr), aggregations.Select(s => Expression.Constant(s, typeof(Expr))));
            var methodCall = Expression.Call(null, method, Plan, arrayExpr);
            return new LazyFrame(methodCall);
        }

        public static LazyFrame ScanCsv(string filePath)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ScanCsvOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Expression.Constant(filePath), Expression.Constant(null, typeof(string[])), Expression.Constant(null, typeof(int?)));
            return new LazyFrame(methodCall);
        }

        public static LazyFrame ScanParquet(string filePath)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ScanParquetOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Expression.Constant(filePath), Expression.Constant(null, typeof(string[])));
            return new LazyFrame(methodCall);
        }

        public static LazyFrame ScanJson(string filePath)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ScanJsonOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Expression.Constant(filePath));
            return new LazyFrame(methodCall);
        }

        public static LazyFrame ScanSql(System.Data.IDbConnection connection, string sql)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ScanSqlOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Expression.Constant(connection), Expression.Constant(sql));
            return new LazyFrame(methodCall);
        }
        /// <summary>
        /// Collect the plan in batches of the specified size (streaming).
        /// Each batch is a DataFrame with at most batchSize rows.
        /// </summary>
        public async IAsyncEnumerable<DataFrame> CollectStreaming(int batchSize = 65536)
        {
            await foreach (var df in CollectAsync())
            {
                int rowCount = df.RowCount;
                if (rowCount <= batchSize)
                {
                    yield return df;
                }
                else
                {
                    int offset = 0;
                    while (offset < rowCount)
                    {
                        int length = Math.Min(batchSize, rowCount - offset);
                        var sliceCols = new List<ISeries>();
                        foreach (var col in df.Columns)
                        {
                            var sliceCol = col.CloneEmpty(length);
                            var indices = new int[length];
                            for (int i = 0; i < length; i++) indices[i] = offset + i;
                            col.Take(sliceCol, indices);
                            sliceCols.Add(sliceCol);
                        }
                        yield return new DataFrame(sliceCols);
                        offset += length;
                    }
                }
            }
        }
        public LazyFrame Join(LazyFrame other, string on, JoinType type = JoinType.Inner)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(JoinOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, other.Plan, Expression.Constant(on), Expression.Constant(type));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Limit(int n)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(LimitOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(n));
            return new LazyFrame(methodCall);
        }

        public LazyFrame WithColumns(params Expr[] columns)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(WithColumnsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var arrayExpr = Expression.NewArrayInit(typeof(Expr), columns.Select(s => Expression.Constant(s, typeof(Expr))));
            var methodCall = Expression.Call(null, method, Plan, arrayExpr);
            return new LazyFrame(methodCall);
        }

        // Dummy static methods to form the AST
        internal static LazyFrame ScanCsvOp(string filePath, string[]? columns = null, int? nRows = null) => null!;
        internal static LazyFrame ScanParquetOp(string filePath, string[]? columns = null) => null!;
        internal static LazyFrame ScanJsonOp(string filePath) => null!;
        internal static LazyFrame ScanSqlOp(System.Data.IDbConnection connection, string sql) => null!;
        internal static LazyFrame FilterOp(LazyFrame source, Expression<Func<Expr, Expr>> predicate) => null!;
        internal static LazyFrame SelectOp(LazyFrame source, Expr[] selections) => null!;
        internal static LazyFrame GroupByOp(LazyFrame source, string[] columns) => null!;
        internal static LazyFrame AggOp(LazyFrame source, Expr[] aggregations) => null!;
        internal static LazyFrame JoinOp(LazyFrame left, LazyFrame right, string on, JoinType type) => null!;
        internal static LazyFrame LimitOp(LazyFrame source, int n) => null!;
        internal static LazyFrame WithColumnsOp(LazyFrame source, Expr[] columns) => null!;

        public async Task<DataFrame> Collect()
        {
            var results = new List<DataFrame>();
            await foreach (var df in CollectAsync())
            {
                // Always include DataFrames with columns even if 0 rows (e.g. Limit(0) should preserve schema)
                if (df.RowCount > 0 || df.Columns.Count > 0)
                {
                    results.Add(df);
                }
            }

            if (results.Count == 0) return new DataFrame();
            if (results.Count == 1) return results[0];

            // Merge logic (simplified for now)
            int totalRows = results.Sum(df => df.RowCount);
            var firstDf = results[0];
            var newCols = new List<ISeries>();

            foreach (var col in firstDf.Columns)
            {
                if (col is Data.Utf8StringSeries)
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

                        // Copy data
                        cData.CopyTo(newData.Slice(byteOffset));

                        // Copy and adjust offsets
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
                else
                {
                    // Generic unmanaged series merging
                    var seriesType = col.GetType();
                    var newCol = (ISeries)Activator.CreateInstance(seriesType, col.Name, totalRows)!;

                    int offset = 0;
                    foreach (var df in results)
                    {
                        var chunkCol = df.GetColumn(col.Name);
                        chunkCol.CopyTo(newCol, offset);
                        offset += chunkCol.Length;
                    }
                    newCols.Add(newCol);
                }
            }

            return new DataFrame(newCols);
        }

        public IAsyncEnumerable<DataFrame> CollectAsync()
        {
            // The execution engine evaluates the optimized expression tree and streams the results.
            var optimizer = new QueryOptimizer();
            var optimizedPlan = optimizer.Optimize(Plan);

            var executor = new ExecutionEngine();
            return executor.ExecuteAsync(optimizedPlan);
        }
        public LazyFrame Delay(int ms)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(DelayOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(ms));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Unnest(params string[] columns)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(UnnestOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(columns));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Unpivot(string[] on, string[] index)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(UnpivotOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(on), Expression.Constant(index));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Pivot(string[] index, string pivot, string values, string aggregate = "sum")
        {
            var method = typeof(LazyFrame).GetMethod(nameof(PivotOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(index), Expression.Constant(new[] { pivot }), Expression.Constant(new[] { values }), Expression.Constant(aggregate));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Transpose(bool include_header = false, string header_name = "column", string[]? column_names = null)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(TransposeOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(include_header), Expression.Constant(header_name), Expression.Constant(column_names, typeof(string[])));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Explode(string column)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ExplodeOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(column));
            return new LazyFrame(methodCall);
        }

        public LazyFrame Sort(string columnName, bool descending = false)
        {
            return Sort(new[] { columnName }, new[] { descending });
        }

        public LazyFrame Sort(params string[] columnNames)
        {
            return Sort(columnNames, columnNames.Select(_ => false).ToArray());
        }

        public LazyFrame Sort(string[] columnNames, bool[] descending)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(SortOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(columnNames), Expression.Constant(descending));
            return new LazyFrame(methodCall);
        }

        internal static LazyFrame SortOp(LazyFrame source, string[] columnNames, bool[] descending) => null!;
        internal static LazyFrame TopKOp(LazyFrame source, string[] columnNames, bool[] descending, int k) => null!;
        internal static LazyFrame ExplodeOp(LazyFrame source, string column) => null!;
        internal static LazyFrame DelayOp(LazyFrame source, int ms) => null!;
        internal static LazyFrame UnnestOp(LazyFrame source, string[] columns) => null!;
        internal static LazyFrame UnpivotOp(LazyFrame source, string[] on, string[] index) => null!;
        internal static LazyFrame PivotOp(LazyFrame source, string[] on, string[] index, string[] values, string aggregate) => null!;
        internal static LazyFrame TransposeOp(LazyFrame source, bool include_header, string header_name, string[]? column_names) => null!;
        internal static LazyFrame DataFrameOp(DataFrame df) => null!;
        internal static LazyFrame SliceOp(LazyFrame source, int offset, int length) => null!;
        internal static LazyFrame TailOp(LazyFrame source, int n) => null!;
        internal static LazyFrame DropNullsOp(LazyFrame source, string[]? subset, bool anyNull) => null!;
        internal static LazyFrame FillNanOp(LazyFrame source, double value) => null!;
        internal static LazyFrame WithRowIndexOp(LazyFrame source, string name) => null!;
        internal static LazyFrame RenameOp(LazyFrame source, Dictionary<string, string> mapping) => null!;
        internal static LazyFrame NullCountOp(LazyFrame source) => null!;
        internal static LazyFrame FetchOp(LazyFrame source, int n) => null!;
        internal static LazyFrame SinkCsvOp(LazyFrame source, string filePath) => null!;
        internal static LazyFrame SinkParquetOp(LazyFrame source, string filePath) => null!;
        internal static LazyFrame ShiftColumnsOp(LazyFrame source, int n) => null!;
        internal static LazyFrame GroupByDynamicOp(LazyFrame source, string indexColumn, string every, string? period, string? offset, string closed, string startBy) => null!;
        internal static LazyFrame GroupByRollingOp(LazyFrame source, string indexColumn, string period, string? offset, string closed) => null!;
        internal static LazyFrame ClearOp(LazyFrame source) => null!;
        internal static LazyFrame ShrinkToFitOp(LazyFrame source) => null!;
        internal static LazyFrame RechunkOp(LazyFrame source) => null!;
        internal static LazyFrame MapOp(LazyFrame source, Func<DataFrame, DataFrame> func) => null!;

        public static LazyFrame FromDataFrame(DataFrame df)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(DataFrameOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Expression.Constant(df));
            return new LazyFrame(methodCall);
        }    /// <summary>Remove duplicate rows (distinct). Polars API: distinct()</summary>
        public LazyFrame Distinct()
        {
            var method = typeof(LazyFrame).GetMethod(nameof(UniqueOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan);
            return new LazyFrame(methodCall);
        }
        /// <summary>Remove duplicate rows (distinct).</summary>
        public LazyFrame Unique()
        {
            var method = typeof(LazyFrame).GetMethod(nameof(UniqueOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan);
            return new LazyFrame(methodCall);
        }
        internal static LazyFrame UniqueOp(LazyFrame source) => null!;
        /// <summary>Return a slice of rows (offset, length). Supports negative offset.</summary>
        public LazyFrame Slice(int offset, int length)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(SliceOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(offset), Expression.Constant(length));
            return new LazyFrame(methodCall);
        }
        /// <summary>Return the last n rows.</summary>
        public LazyFrame Tail(int n)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(TailOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(n));
            return new LazyFrame(methodCall);
        }
        /// <summary>Remove rows containing nulls. If anyNull is true (default), drops rows where any column has null; if false, drops rows where all columns have null.</summary>
        public LazyFrame DropNulls(string[]? subset = null, bool anyNull = true)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(DropNullsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(subset, typeof(string[])), Expression.Constant(anyNull));
            return new LazyFrame(methodCall);
        }
        /// <summary>Replace NaN values in float columns with the specified value.</summary>
        public LazyFrame FillNan(double value)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(FillNanOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(value));
            return new LazyFrame(methodCall);
        }
        /// <summary>Prepend a 0-based row index column with the given name.</summary>
        public LazyFrame WithRowIndex(string name = "index")
        {
            var method = typeof(LazyFrame).GetMethod(nameof(WithRowIndexOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(name));
            return new LazyFrame(methodCall);
        }
        /// <summary>Rename columns using a dictionary of old name to new name mappings.</summary>
        public LazyFrame Rename(Dictionary<string, string> mapping)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(RenameOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(mapping));
            return new LazyFrame(methodCall);
        }
        /// <summary>Return a 2-column DataFrame with per-column null counts (column name + null_count).</summary>
        public LazyFrame NullCount()
        {
            var method = typeof(LazyFrame).GetMethod(nameof(NullCountOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan);
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Fetch the first n rows (early-terminating Limit). Like Limit(n) but may be optimized differently.
        /// </summary>
        public LazyFrame Fetch(int n)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(FetchOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(n));
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Collect the plan and return a (DataFrame, timing_info) tuple where timing_info
        /// describes how long each phase took. For now, simply wraps Collect() with a stopwatch.
        /// </summary>
        public async Task<(DataFrame Result, string Profile)> Profile()
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var result = await Collect();
            sw.Stop();
            return (result, $"Total execution: {sw.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Collect the plan and write the results to a CSV file.
        /// </summary>
        public async Task SinkCsv(string filePath)
        {
            var df = await Collect();
            df.WriteCsv(filePath);
        }

        /// <summary>
        /// Collect the plan and write the results to a Parquet file.
        /// </summary>
        public async Task SinkParquet(string filePath)
        {
            var df = await Collect();
            df.WriteParquet(filePath);
        }

        /// <summary>
        /// Shift all columns by n rows. Positive n shifts downward (forward-fill with nulls).
        /// Negative n shifts upward. Delegates to ArrayKernels.Shift per column.
        /// </summary>
        public LazyFrame ShiftColumns(int n)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ShiftColumnsOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(n));
            return new LazyFrame(methodCall);
        }
        /// <summary>Shift columns by n positions (alias for ShiftColumns). Polars API: shift()</summary>
        public LazyFrame Shift(int n)
        {
            return ShiftColumns(n);
        }
        /// <summary>
        /// Collect the plan and write the results to an Arrow IPC file.
        /// </summary>
        public async Task SinkIpc(string filePath)
        {
            var df = await Collect();
            df.WriteIpc(filePath);
        }
        internal static LazyFrame SinkIpcOp(LazyFrame source, string filePath) => null!;
        internal static LazyFrame AggGroupsOp(LazyFrame source, string[] columns) => null!;
        /// <summary>
        /// Group-by dynamic (time-window-based grouping). Ported from Glacier.Polaris_OLD.
        /// </summary>
        public LazyFrame GroupByDynamic(string indexColumn, string every, string? period = null, string? offset = null, string closed = "left", string startBy = "window")
        {
            var method = typeof(LazyFrame).GetMethod(nameof(GroupByDynamicOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(indexColumn), Expression.Constant(every), Expression.Constant(period, typeof(string)), Expression.Constant(offset, typeof(string)), Expression.Constant(closed), Expression.Constant(startBy));
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Group-by rolling (sliding-window-based grouping). Ported from Glacier.Polaris_OLD.
        /// </summary>
        public LazyFrame GroupByRolling(string indexColumn, string period, string? offset = null, string closed = "right")
        {
            var method = typeof(LazyFrame).GetMethod(nameof(GroupByRollingOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(indexColumn), Expression.Constant(period), Expression.Constant(offset, typeof(string)), Expression.Constant(closed));
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Remove all rows from the DataFrame while preserving the schema. Polars API: clear()
        /// </summary>
        public LazyFrame Clear()
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ClearOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan);
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Shrink memory usage by reallocating columns to their actual sizes. Polars API: shrink_to_fit()
        /// </summary>
        public LazyFrame ShrinkToFit()
        {
            var method = typeof(LazyFrame).GetMethod(nameof(ShrinkToFitOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan);
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Rechunk the DataFrame, consolidating into a single contiguous memory chunk per column.
        /// Polars API: rechunk()
        /// </summary>
        public LazyFrame Rechunk()
        {
            var method = typeof(LazyFrame).GetMethod(nameof(RechunkOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan);
            return new LazyFrame(methodCall);
        }

        /// <summary>
        /// Apply a user-defined function to each batch/chunk of the DataFrame.
        /// Polars API: map()
        /// </summary>
        public LazyFrame Map(Func<DataFrame, DataFrame> func)
        {
            var method = typeof(LazyFrame).GetMethod(nameof(MapOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var methodCall = Expression.Call(null, method, Plan, Expression.Constant(func));
            return new LazyFrame(methodCall);
        }
    }
}