using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Glacier.Polaris
{
    /// <summary>
    /// Represents a deferred execution plan for a DataFrame.
    /// Accumulates operations as a strongly-typed <see cref="LogicalPlanNode"/> tree.
    /// </summary>
    public sealed class LazyFrame
    {
        private Expression? _cachedLegacyPlan;

        internal LogicalPlanNode PlanNode { get; }

        internal LogicalPlanNode LogicalPlan => PlanNode;

        public Expression Plan => _cachedLegacyPlan ??= PlanNode.ToLegacyExpression();

        public LazyFrame(Expression plan)
        {
            PlanNode = LogicalPlanNode.FromLegacyExpression(plan);
            _cachedLegacyPlan = plan;
        }

        internal LazyFrame(LogicalPlanNode planNode)
        {
            PlanNode = planNode ?? throw new ArgumentNullException(nameof(planNode));
        }

        public LazyFrame Filter(Expression<Func<Expr, Expr>> predicate)
        {
            var expr = ExprLambdaEvaluator.Evaluate(predicate);
            return new LazyFrame(new FilterPlanNode(PlanNode, expr, predicate));
        }

        public LazyFrame Filter(Expr predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);
            return new LazyFrame(new FilterPlanNode(PlanNode, predicate));
        }

        public LazyFrame Select(params Expr[] selections)
        {
            return new LazyFrame(new SelectPlanNode(PlanNode, selections));
        }

        public LazyFrame Select(params Expression<Func<Expr, Expr>>[] selections)
        {
            var exprs = selections.Select(s => ExprLambdaEvaluator.Evaluate(s)).ToArray();
            return Select(exprs);
        }

        public LazyFrame GroupBy(params string[] columns)
        {
            return new LazyFrame(new GroupByPlanNode(PlanNode, columns));
        }

        public LazyFrame Agg(params Expr[] aggregations)
        {
            return new LazyFrame(new AggPlanNode(PlanNode, null, aggregations));
        }

        public static LazyFrame ScanCsv(string filePath)
        {
            return new LazyFrame(new ScanCsvPlanNode(filePath, null, null));
        }

        public static LazyFrame ScanParquet(string filePath)
        {
            return new LazyFrame(new ScanParquetPlanNode(filePath, null));
        }

        public static LazyFrame ScanJson(string filePath, string[]? columns = null)
        {
            return new LazyFrame(new ScanJsonPlanNode(filePath, columns));
        }

        public static LazyFrame ScanSql(System.Data.IDbConnection connection, string sql)
        {
            return new LazyFrame(new ScanSqlPlanNode(connection, sql));
        }

        /// <summary>
        /// Collect and optionally stream results.
        /// When streaming=false (default): materialize into a single DataFrame (existing behavior).
        /// When streaming=true: return each batch as a separate yield, useful for large datasets.
        /// Polars API: collect(streaming=True)
        /// </summary>
        public async IAsyncEnumerable<DataFrame> Collect(bool streaming)
        {
            if (!streaming)
            {
                var df = await Collect();
                yield return df;
                yield break;
            }
            await foreach (var df in CollectAsync())
            {
                yield return df;
            }
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
            ArgumentNullException.ThrowIfNull(other);
            var keys = type == JoinType.Cross && string.IsNullOrEmpty(on) ? Array.Empty<string>() : new[] { on };
            return new LazyFrame(new JoinPlanNode(PlanNode, other.PlanNode, keys, keys, type));
        }

        public LazyFrame Join(LazyFrame other, string[] on, JoinType type = JoinType.Inner)
        {
            ArgumentNullException.ThrowIfNull(other);
            return Join(other, on, on, type);
        }

        public LazyFrame Join(LazyFrame other, string[] leftOn, string[] rightOn, JoinType type = JoinType.Inner)
        {
            ArgumentNullException.ThrowIfNull(other);
            if (type == JoinType.Cross)
            {
                return new LazyFrame(new JoinPlanNode(PlanNode, other.PlanNode, Array.Empty<string>(), Array.Empty<string>(), type));
            }
            ArgumentNullException.ThrowIfNull(leftOn);
            ArgumentNullException.ThrowIfNull(rightOn);
            if (leftOn.Length == 0)
                throw new ArgumentException("At least one join key column is required.", nameof(leftOn));
            if (leftOn.Length != rightOn.Length)
                throw new ArgumentException($"Join key arity mismatch: left has {leftOn.Length} keys, right has {rightOn.Length} keys.");
            return new LazyFrame(new JoinPlanNode(PlanNode, other.PlanNode, leftOn, rightOn, type));
        }

        public LazyFrame Limit(int n)
        {
            return new LazyFrame(new LimitPlanNode(PlanNode, n));
        }

        /// <summary>
        /// Returns the first n rows (default 5). Matches Polars lf.head(n).
        /// </summary>
        public LazyFrame Head(int n = 5) => Limit(n);

        public LazyFrame WithColumns(params Expr[] columns)
        {
            return new LazyFrame(new WithColumnsPlanNode(PlanNode, columns));
        }

        // Marker methods retained for legacy Expression AST compatibility (bound via delegate .Method, zero reflection).
        internal static LazyFrame ScanCsvOp(string filePath, string[]? columns = null, int? nRows = null) => null!;
        internal static LazyFrame ScanParquetOp(string filePath, string[]? columns = null) => null!;
        internal static LazyFrame ScanJsonOp(string filePath, string[]? columns = null) => null!;
        internal static LazyFrame ScanSqlOp(System.Data.IDbConnection connection, string sql) => null!;
        internal static LazyFrame FilterOp(LazyFrame source, Expression<Func<Expr, Expr>> predicate) => null!;
        internal static LazyFrame SelectOp(LazyFrame source, Expr[] selections) => null!;
        internal static LazyFrame GroupByOp(LazyFrame source, string[] columns) => null!;
        internal static LazyFrame AggOp(LazyFrame source, Expr[] aggregations) => null!;
        internal static LazyFrame GroupByAggOp(LazyFrame source, string[] columns, Expr[] aggregations) => null!;
        internal static LazyFrame JoinOp(LazyFrame left, LazyFrame right, string on, JoinType type) => null!;
        internal static LazyFrame JoinMultiOp(LazyFrame left, LazyFrame right, string[] leftOn, string[] rightOn, JoinType type) => null!;
        internal static LazyFrame LimitOp(LazyFrame source, int n) => null!;
        internal static LazyFrame WithColumnsOp(LazyFrame source, Expr[] columns) => null!;

        public async Task<DataFrame> Collect()
        {
            var results = new List<DataFrame>();
            await foreach (var df in CollectAsync())
            {
                if (df.RowCount > 0 || df.Columns.Count > 0)
                {
                    results.Add(df);
                }
            }

            if (results.Count == 0) return new DataFrame();
            if (results.Count == 1) return results[0];
            if (results.All(df => df.RowCount == 0)) return results[0];

            return DataFrame.Concat(results);
        }

        public IAsyncEnumerable<DataFrame> CollectAsync()
        {
            var optimizer = new QueryOptimizer();
            var optimizedPlan = optimizer.Optimize(PlanNode);

            var executor = new ExecutionEngine(optimizer.NestedFieldPaths);
            return executor.ExecuteAsync(optimizedPlan);
        }

        public LazyFrame Delay(int ms)
        {
            return new LazyFrame(new DelayPlanNode(PlanNode, ms));
        }

        public LazyFrame Unnest(params string[] columns)
        {
            return new LazyFrame(new UnnestPlanNode(PlanNode, columns));
        }

        public LazyFrame Unpivot(string[] on, string[] index)
        {
            return new LazyFrame(new UnpivotPlanNode(PlanNode, on, index));
        }

        public LazyFrame Pivot(string[] index, string pivot, string values, string aggregate = "sum")
        {
            return new LazyFrame(new PivotPlanNode(PlanNode, index, new[] { pivot }, new[] { values }, aggregate));
        }

        public LazyFrame Transpose(bool include_header = false, string header_name = "column", string[]? column_names = null)
        {
            return new LazyFrame(new TransposePlanNode(PlanNode, include_header, header_name, column_names));
        }

        public LazyFrame Explode(string column)
        {
            return new LazyFrame(new ExplodePlanNode(PlanNode, column));
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
            return new LazyFrame(new SortPlanNode(PlanNode, columnNames, descending));
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
            return new LazyFrame(new DataFramePlanNode(df));
        }

        /// <summary>Remove duplicate rows (distinct). Polars API: distinct()</summary>
        public LazyFrame Distinct(string[]? subset = null, bool maintainOrder = true)
        {
            return Unique(subset, maintainOrder);
        }

        /// <summary>Remove duplicate rows (distinct).</summary>
        public LazyFrame Unique(string[]? subset = null, bool maintainOrder = true)
        {
            return new LazyFrame(new UniquePlanNode(PlanNode, subset, maintainOrder));
        }
        internal static LazyFrame UniqueOp(LazyFrame source, string[]? subset = null, bool maintainOrder = true) => null!;

        /// <summary>Return a slice of rows (offset, length). Supports negative offset.</summary>
        public LazyFrame Slice(int offset, int length)
        {
            return new LazyFrame(new SlicePlanNode(PlanNode, offset, length));
        }

        /// <summary>Return the last n rows.</summary>
        public LazyFrame Tail(int n)
        {
            return new LazyFrame(new TailPlanNode(PlanNode, n));
        }

        /// <summary>Remove rows containing nulls. If anyNull is true (default), drops rows where any column has null; if false, drops rows where all columns have null.</summary>
        public LazyFrame DropNulls(string[]? subset = null, bool anyNull = true)
        {
            return new LazyFrame(new DropNullsPlanNode(PlanNode, subset, anyNull));
        }

        /// <summary>Replace NaN values in float columns with the specified value.</summary>
        public LazyFrame FillNan(double value)
        {
            return new LazyFrame(new FillNanPlanNode(PlanNode, value));
        }

        /// <summary>Prepend a 0-based row index column with the given name.</summary>
        public LazyFrame WithRowIndex(string name = "index")
        {
            return new LazyFrame(new WithRowIndexPlanNode(PlanNode, name));
        }

        /// <summary>Rename columns using a dictionary of old name to new name mappings.</summary>
        public LazyFrame Rename(Dictionary<string, string> mapping)
        {
            return new LazyFrame(new RenamePlanNode(PlanNode, mapping));
        }

        /// <summary>Return a 2-column DataFrame with per-column null counts (column name + null_count).</summary>
        public LazyFrame NullCount()
        {
            return new LazyFrame(new NullCountPlanNode(PlanNode));
        }

        /// <summary>
        /// Fetch the first n rows (early-terminating Limit). Like Limit(n) but may be optimized differently.
        /// </summary>
        public LazyFrame Fetch(int n)
        {
            return new LazyFrame(new FetchPlanNode(PlanNode, n));
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
        /// Stream the plan chunk-by-chunk directly to a CSV file without materializing all rows.
        /// </summary>
        public async Task SinkCsv(string filePath)
        {
            await IO.CsvWriter.WriteStreamAsync(CollectAsync(), filePath).ConfigureAwait(false);
        }

        /// <summary>
        /// Stream the plan chunk-by-chunk directly to a Parquet file without materializing all rows.
        /// </summary>
        public async Task SinkParquet(string filePath)
        {
            await IO.ParquetWriter.WriteStreamAsync(CollectAsync(), filePath).ConfigureAwait(false);
        }

        /// <summary>
        /// Shift all columns by n rows. Positive n shifts downward (forward-fill with nulls).
        /// Negative n shifts upward. Delegates to ArrayKernels.Shift per column.
        /// </summary>
        public LazyFrame ShiftColumns(int n)
        {
            return new LazyFrame(new ShiftColumnsPlanNode(PlanNode, n));
        }

        /// <summary>Shift columns by n positions (alias for ShiftColumns). Polars API: shift()</summary>
        public LazyFrame Shift(int n)
        {
            return ShiftColumns(n);
        }

        /// <summary>
        /// Stream the plan chunk-by-chunk directly to a Glacier Compact Columnar IPC v1 file without materializing all rows.
        /// </summary>
        public async Task SinkIpc(string filePath)
        {
            await IO.IpcWriter.WriteStreamAsync(CollectAsync(), filePath).ConfigureAwait(false);
        }
        internal static LazyFrame SinkIpcOp(LazyFrame source, string filePath) => null!;
        internal static LazyFrame AggGroupsOp(LazyFrame source, string[] columns) => null!;

        /// <summary>
        /// Group-by dynamic (time-window-based grouping). Ported from Glacier.Polaris_OLD.
        /// </summary>
        public LazyFrame GroupByDynamic(string indexColumn, string every, string? period = null, string? offset = null, string closed = "left", string startBy = "window")
        {
            return new LazyFrame(new GroupByDynamicPlanNode(PlanNode, indexColumn, every, period, offset, closed, startBy));
        }

        /// <summary>
        /// Group-by rolling (sliding-window-based grouping). Ported from Glacier.Polaris_OLD.
        /// </summary>
        public LazyFrame GroupByRolling(string indexColumn, string period, string? offset = null, string closed = "right")
        {
            return new LazyFrame(new GroupByRollingPlanNode(PlanNode, indexColumn, period, offset, closed));
        }

        /// <summary>
        /// Remove all rows from the DataFrame while preserving the schema. Polars API: clear()
        /// </summary>
        public LazyFrame Clear()
        {
            return new LazyFrame(new ClearPlanNode(PlanNode));
        }

        /// <summary>
        /// Shrink memory usage by reallocating columns to their actual sizes. Polars API: shrink_to_fit()
        /// </summary>
        public LazyFrame ShrinkToFit()
        {
            return new LazyFrame(new ShrinkToFitPlanNode(PlanNode));
        }

        /// <summary>
        /// Rechunk the DataFrame, consolidating into a single contiguous memory chunk per column.
        /// Polars API: rechunk()
        /// </summary>
        public LazyFrame Rechunk()
        {
            return new LazyFrame(new RechunkPlanNode(PlanNode));
        }

        /// <summary>
        /// Apply a user-defined function to each batch/chunk of the DataFrame.
        /// Polars API: map()
        /// </summary>
        public LazyFrame Map(Func<DataFrame, DataFrame> func)
        {
            return new LazyFrame(new MapPlanNode(PlanNode, func));
        }
    }
}
