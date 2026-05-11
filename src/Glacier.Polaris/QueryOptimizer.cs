using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris
{
    public class QueryOptimizer : ExpressionVisitor
    {
        public Expression Optimize(Expression plan)
        {
            // 1. Analyze: Find all columns used in the entire query
            var tracker = new ColumnTrackerVisitor();
            tracker.Visit(plan);

            // 2. Transform: Predicate Pushdown (re-order filters)
            var transformed = Visit(plan);

            // 3. Join Reordering: Apply cost-based join reordering
            transformed = new JoinReorderingVisitor().Visit(transformed);

            // 4. Inject: Projection Pushdown (tell scanners what columns to load)
            // Only push down if we have a Select node that restricts columns.
            // Otherwise, we must load all columns.
            if (tracker.HasGlobalSelect && tracker.Columns.Count > 0)
            {
                transformed = new ProjectionPushdownVisitor(tracker.Columns.ToArray()).Visit(transformed);
            }

            return transformed;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // Recursive optimization: Visit arguments first
            var visitedNode = (MethodCallExpression)base.VisitMethodCall(node);

            if (visitedNode.Method.Name == nameof(LazyFrame.FilterOp))
            {
                var source = visitedNode.Arguments[0];
                var predicate = visitedNode.Arguments[1];

                if (source is MethodCallExpression sourceCall && sourceCall.Method.Name == nameof(LazyFrame.SelectOp))
                {
                    // Push Filter through Select
                    // Filter(Select(src, sel), pred) -> Select(Filter(src, pred), sel)
                    var innerSource = sourceCall.Arguments[0];
                    var selections = sourceCall.Arguments[1];

                    var newFilter = Expression.Call(null, visitedNode.Method, innerSource, predicate);
                    return Expression.Call(null, sourceCall.Method, Visit(newFilter), selections);
                }
                else if (source is MethodCallExpression sourceJoin && sourceJoin.Method.Name == nameof(LazyFrame.JoinOp))
                {
                    // Push Filter through Join
                    var left = sourceJoin.Arguments[0];
                    var right = sourceJoin.Arguments[1];
                    var on = sourceJoin.Arguments[2];
                    var type = (JoinType)((ConstantExpression)sourceJoin.Arguments[3]).Value!;

                    var predTracker = new ColumnTrackerVisitor();
                    predTracker.Visit(predicate);

                    var leftTracker = new ColumnTrackerVisitor();
                    leftTracker.Visit(left);

                    var rightTracker = new ColumnTrackerVisitor();
                    rightTracker.Visit(right);

                    bool onlyLeft = predTracker.Columns.All(c => leftTracker.Columns.Contains(c));
                    bool onlyRight = predTracker.Columns.All(c => rightTracker.Columns.Contains(c));

                    if (onlyLeft && (type == JoinType.Inner || type == JoinType.Left))
                    {
                        var newLeft = Expression.Call(null, visitedNode.Method, left, predicate);
                        return Expression.Call(null, sourceJoin.Method, Visit(newLeft), right, on, sourceJoin.Arguments[3]);
                    }
                    else if (onlyRight && type == JoinType.Inner)
                    {
                        var newRight = Expression.Call(null, visitedNode.Method, right, predicate);
                        return Expression.Call(null, sourceJoin.Method, left, Visit(newRight), on, sourceJoin.Arguments[3]);
                    }
                }
                else if (source is MethodCallExpression sourceGroupBy && sourceGroupBy.Method.Name == nameof(LazyFrame.GroupByOp))
                {
                    // Push Filter through GroupBy (only if filter columns are in GroupBy keys)
                    var innerSource = sourceGroupBy.Arguments[0];
                    var groupCols = (string[])((ConstantExpression)sourceGroupBy.Arguments[1]).Value!;

                    var predTracker = new ColumnTrackerVisitor();
                    predTracker.Visit(predicate);

                    if (predTracker.Columns.All(c => groupCols.Contains(c)))
                    {
                        var newFilter = Expression.Call(null, visitedNode.Method, innerSource, predicate);
                        return Expression.Call(null, sourceGroupBy.Method, Visit(newFilter), sourceGroupBy.Arguments[1]);
                    }
                }
            }
            else if (visitedNode.Method.Name == nameof(LazyFrame.LimitOp))
            {
                var source = visitedNode.Arguments[0];
                var n = (int)((ConstantExpression)visitedNode.Arguments[1]).Value!;

                if (source is MethodCallExpression sourceCall)
                {
                    if (sourceCall.Method.Name == nameof(LazyFrame.ScanCsvOp))
                    {
                        // Limit -> ScanCsv
                        return Expression.Call(null, sourceCall.Method,
                            sourceCall.Arguments[0],
                            sourceCall.Arguments[1],
                            Expression.Constant((int?)n, typeof(int?)));
                    }
                    else if (sourceCall.Method.Name == nameof(LazyFrame.SortOp))
                    {
                        var method = typeof(LazyFrame).GetMethod(nameof(LazyFrame.TopKOp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
                        return Expression.Call(null, method, sourceCall.Arguments[0], sourceCall.Arguments[1], sourceCall.Arguments[2], visitedNode.Arguments[1]);
                    }
                    else if (sourceCall.Method.Name == nameof(LazyFrame.SelectOp))
                    {
                        // Limit -> Select -> Limit
                        var innerSource = sourceCall.Arguments[0];
                        var newLimit = Expression.Call(null, visitedNode.Method, innerSource, visitedNode.Arguments[1]);
                        return Expression.Call(null, sourceCall.Method, Visit(newLimit), sourceCall.Arguments[1]);
                    }
                }
            }
            else if (visitedNode.Method.Name == nameof(LazyFrame.SelectOp))
            {
                var source = visitedNode.Arguments[0];
                var selections = visitedNode.Arguments[1];

                if (source is MethodCallExpression sourceCall && sourceCall.Method.Name == nameof(LazyFrame.SelectOp))
                {
                    // Merge consecutive Selects: Select(Select(src, inner), outer) -> Select(src, outer)
                    // Note: This is safe if 'outer' doesn't depend on aliases created in 'inner' 
                    // that are not in 'src'. For this MVP, we assume simple column references.
                    return Expression.Call(null, visitedNode.Method, sourceCall.Arguments[0], selections);
                }
            }

            return visitedNode;
        }
    }

    internal class ColumnTrackerVisitor : ExpressionVisitor
    {
        public HashSet<string> Columns { get; } = new HashSet<string>();
        public bool HasGlobalSelect { get; private set; } = false;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Col")
            {
                Columns.Add((string)((ConstantExpression)node.Arguments[0]).Value!);
            }
            else if (node.Method.Name == nameof(LazyFrame.JoinOp))
            {
                var on = (string)((ConstantExpression)node.Arguments[2]).Value!;
                Columns.Add(on);
            }
            else if (node.Method.Name == nameof(LazyFrame.SelectOp))
            {
                HasGlobalSelect = true;
            }
            else if (node.Method.Name == nameof(LazyFrame.WithColumnsOp))
            {
                // Visit the selections to find used columns
                Visit(node.Arguments[1]);
            }
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is Expr expr)
            {
                Visit(expr.Expression);
            }
            return base.VisitConstant(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (node is Expression<Func<Expr, Expr>> exprLambda)
            {
                try
                {
                    var func = exprLambda.Compile();
                    var result = func(null!);
                    Visit(result.Expression);
                }
                catch { /* Ignore errors during analysis */ }
            }
            return base.VisitLambda(node);
        }
    }

    internal class ProjectionPushdownVisitor : ExpressionVisitor
    {
        private readonly string[] _allUsedColumns;
        public ProjectionPushdownVisitor(string[] columns) => _allUsedColumns = columns;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(LazyFrame.ScanCsvOp))
            {
                var filePath = (string)((ConstantExpression)node.Arguments[0]).Value!;
                try
                {
                    var fileHeaders = IO.CsvReader.PeekHeaders(filePath);
                    // Only push down columns that exist in this file
                    var validColumns = _allUsedColumns.Intersect(fileHeaders).ToArray();
                    return Expression.Call(null, node.Method, node.Arguments[0], Expression.Constant(validColumns, typeof(string[])), node.Arguments[2]);
                }
                catch
                {
                    // Fallback if file not found during optimization
                    return node;
                }
            }
            else if (node.Method.Name == nameof(LazyFrame.ScanParquetOp))
            {
                var filePath = (string)((ConstantExpression)node.Arguments[0]).Value!;
                try
                {
                    var fileHeaders = IO.ParquetReader.PeekHeaders(filePath);
                    var validColumns = _allUsedColumns.Intersect(fileHeaders).ToArray();
                    return Expression.Call(null, node.Method, node.Arguments[0], Expression.Constant(validColumns, typeof(string[])));
                }
                catch
                {
                    return node;
                }
            }
            else if (node.Method.Name == nameof(LazyFrame.ScanJsonOp))
            {
                // JsonReader doesn't support column projection yet (reads chunks)
                return node;
            }
            return base.VisitMethodCall(node);
        }
    }

    public class ExecutionEngine
    {
        public IAsyncEnumerable<DataFrame> ExecuteAsync(Expression plan)
        {
            // The execution engine evaluates the optimized expression tree.
            // A real engine would compile this into a highly-optimized execution DAG.
            // For this implementation, we recursively evaluate the MethodCallExpressions.

            return Evaluate(plan);
        }
        private IAsyncEnumerable<DataFrame> Evaluate(Expression node)
        {
            if (node is MethodCallExpression methodCall)
            {
                if (methodCall.Method.Name == nameof(LazyFrame.ScanCsvOp))
                {
                    var filePath = (string)((ConstantExpression)methodCall.Arguments[0]).Value!;
                    var columns = (string[]?)((ConstantExpression)methodCall.Arguments[1]).Value;
                    var nRows = (int?)((ConstantExpression)methodCall.Arguments[2]).Value;
                    var reader = new IO.CsvReader(filePath, columns, nRows);
                    return reader.ReadAsync();
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ScanParquetOp))
                {
                    var filePath = (string)((ConstantExpression)methodCall.Arguments[0]).Value!;
                    var columns = (string[]?)((ConstantExpression)methodCall.Arguments[1]).Value;
                    var reader = new IO.ParquetReader(filePath, columns);
                    return reader.ReadAsync();
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ScanJsonOp))
                {
                    var filePath = (string)((ConstantExpression)methodCall.Arguments[0]).Value!;
                    var reader = new IO.JsonReader(filePath, 10000);
                    return reader.ReadNdJsonAsync();
                }
                else if (methodCall.Method.Name == "DataFrameOp")
                {
                    var df = (DataFrame)((ConstantExpression)methodCall.Arguments[0]).Value!;
                    return ToAsyncEnumerable(df);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.LimitOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var n = (int)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyLimit(source, n);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.FilterOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var predicate = methodCall.Arguments[1];
                    return ApplyFilter(source, predicate);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.SelectOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var selections = methodCall.Arguments[1];
                    return ApplySelect(source, selections);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.JoinOp))
                {
                    var leftSource = Evaluate(methodCall.Arguments[0]);
                    var rightSource = Evaluate(methodCall.Arguments[1]);
                    var on = (string)((ConstantExpression)methodCall.Arguments[2]).Value!;
                    var type = (JoinType)((ConstantExpression)methodCall.Arguments[3]).Value!;
                    return ApplyJoin(leftSource, rightSource, on, type);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.WithColumnsOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var selections = methodCall.Arguments[1];
                    return ApplyWithColumns(source, selections);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ExplodeOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var column = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyExplode(source, column);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.UnnestOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var columns = ExtractStringArrayFromExpr(methodCall.Arguments[1]);
                    return ApplyUnnest(source, columns);
                }

                else if (methodCall.Method.Name == nameof(LazyFrame.TopKOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var columnNames = (string[])((ConstantExpression)methodCall.Arguments[1]).Value!;
                    var descending = (bool[])((ConstantExpression)methodCall.Arguments[2]).Value!;
                    var k = (int)((ConstantExpression)methodCall.Arguments[3]).Value!;
                    return ApplyTopK(source, columnNames, descending, k);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.SortOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var columnNames = (string[])((ConstantExpression)methodCall.Arguments[1]).Value!;
                    var descending = (bool[])((ConstantExpression)methodCall.Arguments[2]).Value!;
                    return ApplySort(source, columnNames, descending);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ExplodeOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var column = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyExplode(source, column);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.DelayOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var ms = (int)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyDelay(source, ms);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.UnpivotOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var idVars = (string[])((ConstantExpression)methodCall.Arguments[1]).Value!;
                    var valueVars = (string[])((ConstantExpression)methodCall.Arguments[2]).Value!;
                    // Note: the static UnpivotOp only takes (source, on, index), no varName/valName
                    return ApplyUnpivot(source, idVars, valueVars, "variable", "value");
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.PivotOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var index = (string[])((ConstantExpression)methodCall.Arguments[1]).Value!;
                    var pivot = ((string[])((ConstantExpression)methodCall.Arguments[2]).Value!)[0];
                    var values = ((string[])((ConstantExpression)methodCall.Arguments[3]).Value!)[0];
                    var agg = (string)((ConstantExpression)methodCall.Arguments[4]).Value!;
                    return ApplyPivot(source, index, pivot, values, agg);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.TransposeOp))
                {
                    var source = Evaluate(methodCall.Arguments[0]);
                    var include_header = (bool)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    var header_name = (string)((ConstantExpression)methodCall.Arguments[2]).Value!;
                    var column_names = (string[]?)((ConstantExpression)methodCall.Arguments[3]).Value;
                    return ApplyTranspose(source, include_header, header_name, column_names);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.UniqueOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    return ApplyUnique(source2);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.SliceOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var offset = (int)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    var length = (int)((ConstantExpression)methodCall.Arguments[2]).Value!;
                    return ApplySlice(source2, offset, length);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.TailOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var n = (int)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyTail(source2, n);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.DropNullsOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var subset = (string[]?)((ConstantExpression)methodCall.Arguments[1]).Value;
                    var anyNull = (bool)((ConstantExpression)methodCall.Arguments[2]).Value!;
                    return ApplyDropNulls(source2, subset, anyNull);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.FillNanOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var value = (double)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyFillNan(source2, value);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.WithRowIndexOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var name = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyWithRowIndex(source2, name);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.RenameOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var mapping = (Dictionary<string, string>)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyRename(source2, mapping);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.NullCountOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    return ApplyNullCount(source2);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.FetchOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var n = (int)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyLimit(source2, n);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.SinkCsvOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var filePath = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplySinkCsv(source2, filePath);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.SinkParquetOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var filePath = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplySinkParquet(source2, filePath);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.SinkIpcOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var filePath = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplySinkIpc(source2, filePath);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.AggGroupsOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var columns = (string[])((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyAggGroups(source2, columns);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ClearOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    return ApplyClear(source2);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ShrinkToFitOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    return ApplyShrinkToFit(source2);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.RechunkOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    return ApplyRechunk(source2);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.MapOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var func = (Func<DataFrame, DataFrame>)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyMap(source2, func);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ShiftColumnsOp))
                {
                    var source2 = Evaluate(methodCall.Arguments[0]);
                    var n = (int)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyShiftColumns(source2, n);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.AggOp))
                {
                    // AggOp(GroupByOp(source, columns), aggregations)
                    var groupByExpr = methodCall.Arguments[0];
                    if (groupByExpr is MethodCallExpression groupByCall && groupByCall.Method.Name == nameof(LazyFrame.GroupByOp))
                    {
                        var groupColumns = ExtractStringArrayFromExpr(groupByCall.Arguments[1]);
                        var source = Evaluate(groupByCall.Arguments[0]);
                        var aggregations = methodCall.Arguments[1];
                        return ApplyAgg(source, groupColumns, aggregations);
                    }
                    throw new NotSupportedException("AggOp must follow GroupByOp.");
                }
                else if (methodCall.Method.Name == "GroupByAggOp")
                {
                    // GroupByAggOp(source, columns[], aggregations[])
                    var source = Evaluate(methodCall.Arguments[0]);
                    var groupColumns = ExtractStringArrayFromExpr(methodCall.Arguments[1]);
                    var aggregations = methodCall.Arguments[2];
                    return ApplyAgg(source, groupColumns, aggregations);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.GroupByOp))
                {
                    // Just pass-through: evaluate the source
                    return Evaluate(methodCall.Arguments[0]);
                }
                else if (methodCall.Method.Name == nameof(LazyFrame.ScanSqlOp))
                {
                    var connection = (System.Data.IDbConnection)((ConstantExpression)methodCall.Arguments[0]).Value!;
                    var sql = (string)((ConstantExpression)methodCall.Arguments[1]).Value!;
                    return ApplyScanSql(connection, sql);
                }
            }

            throw new NotSupportedException($"Execution engine cannot evaluate node: {node}");
        }

        private async IAsyncEnumerable<DataFrame> ApplyFilter(IAsyncEnumerable<DataFrame> source, Expression predicateExpr)
        {
            // The predicate is a lambda: (c) => Expr
            var quoteUnary = (UnaryExpression)predicateExpr;
            var lambda = (LambdaExpression)quoteUnary.Operand;
            var compiledLambda = (Func<Expr, Expr>)lambda.Compile(); // Compile: _ => predicateExpr

            await foreach (var df in source)
            {
                var disposables = new List<IDisposable>();
                try
                {
                    // Invoke the lambda to get the Expr object, then evaluate its expression
                    var actualExpr = compiledLambda(null!);
                    var result = EvaluateExpression(actualExpr.Expression, df, disposables);


                    if (result is Data.BooleanSeries mask)
                    {
                        var maskSpan = mask.Memory.Span;
                        int matchCount = 0;
                        for (int i = 0; i < maskSpan.Length; i++) if (maskSpan[i]) matchCount++;

                        if (matchCount == 0) continue; // Skip empty chunk

                        int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(matchCount);
                        int idx = 0;
                        for (int i = 0; i < maskSpan.Length; i++) if (maskSpan[i]) indices[idx++] = i;

                        var spanIndices = new ReadOnlySpan<int>(indices, 0, matchCount);
                        var newSeries = new List<ISeries>(df.Columns.Count);

                        foreach (var col in df.Columns)
                        {
                            ISeries newCol;
                            if (col is Data.Utf8StringSeries u8)
                            {
                                int totalBytes = 0;
                                for (int i = 0; i < spanIndices.Length; i++) totalBytes += u8.GetStringSpan(spanIndices[i]).Length;
                                newCol = new Data.Utf8StringSeries(u8.Name, matchCount, totalBytes);
                            }
                            else
                            {
                                newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, matchCount)!;
                            }
                            col.Take(newCol, spanIndices);
                            newSeries.Add(newCol);
                        }

                        System.Buffers.ArrayPool<int>.Shared.Return(indices);
                        yield return new DataFrame(newSeries);
                    }
                    else
                    {
                        throw new InvalidOperationException("Filter expression must evaluate to a Boolean series.");
                    }
                }
                finally
                {
                    foreach (var d in disposables) d.Dispose();
                }
            }
        }
        private async IAsyncEnumerable<DataFrame> ApplySelect(IAsyncEnumerable<DataFrame> source, Expression selections)
        {
            await foreach (var df in source)
            {
                if (selections is NewArrayExpression arrayExpr)
                {
                    var newColumns = new List<ISeries>();
                    var disposables = new List<IDisposable>(); // Track intermediate allocations

                    try
                    {
                        foreach (var exprNode in arrayExpr.Expressions)
                        {
                            var selection = (Expr)((ConstantExpression)exprNode).Value!;
                            var body = selection.Expression;
                            string alias = null;
                            if (body is MethodCallExpression mce && mce.Method.Name == "AliasOp")
                            {
                                alias = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                                body = mce.Arguments[0];
                            }

                            var resultSeries = EvaluateExpression(body, df, disposables);
                            if (alias != null)
                            {
                                resultSeries.Rename(alias);
                            }

                            // If the final result is in the disposable list (meaning it's an intermediate we created),
                            // we must REMOVE it from the disposable list so it survives into the final DataFrame!
                            if (resultSeries is IDisposable d && disposables.Contains(d))
                            {
                                disposables.Remove(d);
                            }

                            newColumns.Add(resultSeries);
                        }
                        yield return new DataFrame(newColumns);
                    }
                    finally
                    {
                        foreach (var d in disposables)
                        {
                            d.Dispose();
                        }
                    }
                }
                else
                {
                    yield return df;
                }
            }
        }
        private async IAsyncEnumerable<DataFrame> ApplyWithColumns(IAsyncEnumerable<DataFrame> source, Expression selections)
        {
            await foreach (var df in source)
            {
                if (selections is NewArrayExpression arrayExpr)
                {
                    var newColumns = df.Columns.ToList();
                    var disposables = new List<IDisposable>();

                    try
                    {
                        foreach (var exprNode in arrayExpr.Expressions)
                        {
                            var selection = (Expr)((ConstantExpression)exprNode).Value!;
                            var body = selection.Expression;
                            string alias = null;
                            if (body is MethodCallExpression mce && mce.Method.Name == "AliasOp")
                            {
                                alias = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                                body = mce.Arguments[0];
                            }
                            else if (body is MethodCallExpression mceCol && mceCol.Method.Name == "Col")
                            {
                                alias = (string)((ConstantExpression)mceCol.Arguments[0]).Value!;
                            }

                            if (alias == null) throw new InvalidOperationException("WithColumns requires an alias or a column reference.");

                            var resultSeries = EvaluateExpression(body, df, disposables);
                            resultSeries.Rename(alias);

                            if (resultSeries is IDisposable d && disposables.Contains(d))
                            {
                                disposables.Remove(d);
                            }

                            // Overwrite or append
                            int existingIndex = newColumns.FindIndex(c => c.Name == alias);
                            if (existingIndex >= 0) newColumns[existingIndex] = resultSeries;
                            else newColumns.Add(resultSeries);
                        }
                        yield return new DataFrame(newColumns);
                    }
                    finally
                    {
                        foreach (var d in disposables) d.Dispose();
                    }
                }
                else
                {
                    yield return df;
                }
            }
        }

        private ISeries EvaluateExpression(Expression body, DataFrame df, List<IDisposable> disposables)
        {
            if (body is MethodCallExpression mce)
            {
                if (mce.Method.Name == "Col")
                {
                    string colName = (string)((ConstantExpression)mce.Arguments[0]).Value!;
                    return df.GetColumn(colName);
                }
                else if (mce.Method.Name == "LitOp")
                {
                    var arg = mce.Arguments[0];
                    if (arg is UnaryExpression ue && ue.NodeType == ExpressionType.Convert) arg = ue.Operand;
                    var val = ((ConstantExpression)arg).Value;
                    return CreateLiteralSeries(val, df.RowCount);
                }
                // ── Binary ops (Bin namespace) ────────────────────────────────
                else if (mce.Method.Name == "Bin_LengthsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.Lengths(bin);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.Lengths requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_ContainsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    byte[] pattern = (byte[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.Contains(bin, pattern);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.Contains requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_StartsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    byte[] prefix = (byte[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.StartsWith(bin, prefix);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.StartsWith requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_EndsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    byte[] suffix = (byte[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.BinarySeries bin)
                    {
                        var result = Compute.BinaryKernels.EndsWith(bin, suffix);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Bin.EndsWith requires BinarySeries.");
                }
                else if (mce.Method.Name == "Bin_EncodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string encoding = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    // Encode: text→binary (utf-8) or binary→string (hex/base64)
                    bool isUtf8Encoding = encoding.Equals("utf-8", StringComparison.OrdinalIgnoreCase) || encoding.Equals("utf8", StringComparison.OrdinalIgnoreCase);
                    if (isUtf8Encoding)
                    {
                        // Utf8String → Binary (EncodeUtf8)
                        if (series is Data.Utf8StringSeries utf8)
                        {
                            var result = Compute.BinaryKernels.EncodeUtf8(utf8);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Encode('utf-8') requires Utf8StringSeries.");
                    }
                    else
                    {
                        // Binary → string (hex/base64)
                        if (series is Data.BinarySeries bin)
                        {
                            var result = Compute.BinaryKernels.Encode(bin, encoding);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Encode requires BinarySeries for non-utf8 encodings.");
                    }
                }
                else if (mce.Method.Name == "Bin_DecodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string encoding = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    // Decode: binary→text (utf-8 or hex/base64) or string→binary (hex/base64 round-trip)
                    bool isUtf8Encoding = encoding.Equals("utf-8", StringComparison.OrdinalIgnoreCase) || encoding.Equals("utf8", StringComparison.OrdinalIgnoreCase);
                    if (isUtf8Encoding)
                    {
                        // Binary → Utf8String (DecodeUtf8)
                        if (series is Data.BinarySeries bin)
                        {
                            var result = Compute.BinaryKernels.DecodeUtf8(bin);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Decode('utf-8') requires BinarySeries.");
                    }
                    else
                    {
                        // hex/base64: if input is BinarySeries, encode to string; if input is Utf8StringSeries, decode to binary
                        if (series is Data.BinarySeries bin)
                        {
                            // Binary → hex/base64 string (interpret as "encode binary to text representation")
                            var result = Compute.BinaryKernels.Encode(bin, encoding);
                            disposables.Add(result);
                            return result;
                        }
                        if (series is Data.Utf8StringSeries utf8)
                        {
                            // hex/base64 string → binary (round-trip decode)
                            var result = Compute.BinaryKernels.Decode(utf8, encoding);
                            disposables.Add(result);
                            return result;
                        }
                        throw new NotSupportedException("Bin.Decode requires BinarySeries or Utf8StringSeries for non-utf8 encodings.");
                    }
                }




                // ── Temporal ops: Hour, Minute, Second, Nanosecond on TimeSeries ──
                else if (mce.Method.Name == "HourOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.TimeSeries ts)
                    {
                        var result = new Data.Int32Series(series.Name + "_hour", series.Length);
                        for (int i = 0; i < ts.Length; i++)
                        {
                            if (ts.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                            else result.Memory.Span[i] = ts.GetHour(i);
                        }
                        disposables.Add(result);
                        return result;
                    }
                    // Fallback to TemporalKernels for DatetimeSeries
                    var res = Compute.TemporalKernels.ExtractHour(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "MinuteOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.TimeSeries ts)
                    {
                        var result = new Data.Int32Series(series.Name + "_minute", series.Length);
                        for (int i = 0; i < ts.Length; i++)
                        {
                            if (ts.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                            else result.Memory.Span[i] = ts.GetMinute(i);
                        }
                        disposables.Add(result);
                        return result;
                    }
                    var res2 = Compute.TemporalKernels.ExtractMinute(series);
                    disposables.Add(res2);
                    return res2;
                }
                else if (mce.Method.Name == "SecondOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.TimeSeries ts)
                    {
                        var result = new Data.Int32Series(series.Name + "_second", series.Length);
                        for (int i = 0; i < ts.Length; i++)
                        {
                            if (ts.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                            else result.Memory.Span[i] = ts.GetSecond(i);
                        }
                        disposables.Add(result);
                        return result;
                    }
                    var res3 = Compute.TemporalKernels.ExtractSecond(series);
                    disposables.Add(res3);
                    return res3;
                }
                else if (mce.Method.Name == "NanosecondOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractNanosecond(series);
                    disposables.Add(res);
                    return res;
                }

                else if (mce.Method.Name == "RollingMeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingMean(series, window);
                    result.Rename(series.Name + "_rolling_mean");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingSumOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingSum(series, window);
                    result.Rename(series.Name + "_rolling_sum");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingStdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingStd(series, window);
                    result.Rename(series.Name + "_rolling_std");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingMinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingMin(series, window);
                    result.Rename(series.Name + "_rolling_min");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RollingMaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int window = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.RollingMax(series, window);
                    result.Rename(series.Name + "_rolling_max");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingSumOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingSum(series);
                    result.Rename(series.Name + "_expanding_sum");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingMeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingMean(series);
                    result.Rename(series.Name + "_expanding_mean");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingMinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingMin(series);
                    result.Rename(series.Name + "_expanding_min");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingMaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingMax(series);
                    result.Rename(series.Name + "_expanding_max");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpandingStdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.WindowKernels.ExpandingStd(series);
                    result.Rename(series.Name + "_expanding_std");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "EWMMeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double alpha = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.EWMMean(series, alpha);
                    result.Rename(series.Name + "_ewm_mean");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SumOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Sum();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "MeanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Mean();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "MinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Min();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "MaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Max();
                    disposables.Add(result);
                    return result;
                }

                else if (mce.Method.Name == "StdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Std();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "VarOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Var();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "FillNullLiteralOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    // Argument may be a UnaryExpression(Convert) wrapping a ConstantExpression for value types
                    var arg1 = mce.Arguments[1];
                    if (arg1 is UnaryExpression ueArg && ueArg.NodeType == ExpressionType.Convert)
                        arg1 = ueArg.Operand;
                    object val = ((ConstantExpression)arg1).Value!;
                    var result = series.FillNull(val);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "FillNullOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    FillStrategy strategy = (FillStrategy)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.FillNull(strategy);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "LengthOp")
                {

                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var lengthSeries = new Int32Series("length", 1) { [0] = series.Length };
                    disposables.Add(lengthSeries);
                    return lengthSeries;
                }
                else if (mce.Method.Name == "StartsWithOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.StartsWith(pattern);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "EndsWithOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.EndsWith(pattern);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "OverOp")
                {
                    var innerExpr = mce.Arguments[0];
                    var groupCols = (string[])((ConstantExpression)mce.Arguments[1]).Value!;

                    if (innerExpr is MethodCallExpression innerMce)
                    {
                        string aggType = innerMce.Method.Name.Replace("Op", "").ToLower(); // sum, mean, count
                        var targetColExpr = innerMce.Arguments[0];
                        var targetSeries = EvaluateExpression(targetColExpr, df, disposables);

                        var gCols = groupCols.Select(c => df.GetColumn(c)).ToArray();
                        var groups = Compute.GroupByKernels.GroupBy(gCols);

                        var aggregated = Compute.GroupByKernels.Aggregate(targetSeries, groups, aggType);
                        var broadcasted = Compute.WindowKernels.BroadcastOver(aggregated, groups, df.RowCount);

                        disposables.Add(broadcasted);
                        return broadcasted;
                    }
                    throw new NotSupportedException("Over requires an aggregation expression.");
                }
                else if (mce.Method.Name == "Str_ContainsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_contains", utf8.Length);
                        Compute.StringKernels.Contains(utf8.DataBytes.Span, utf8.Offsets.Span, pattern, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_contains", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.Contains requires Utf8StringSeries.");
                }


                else if (mce.Method.Name == "Str_LengthsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var result = new Data.Int32Series(utf8.Name + "_len", utf8.Length);
                        Compute.StringKernels.Lengths(utf8.Offsets.Span, result.Memory.Span);
                        result.ValidityMask.CopyFrom(utf8.ValidityMask);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.Lengths requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_StartsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string prefix = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_startswith", utf8.Length);
                        intResult.Memory.Span.Clear(); // NativeMemory.Alloc doesn't zero; ensure non-matches are 0
                        Compute.StringKernels.StartsWith(utf8.DataBytes.Span, utf8.Offsets.Span, prefix, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_startswith", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.StartsWith requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_EndsWithOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string suffix = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_endswith", utf8.Length);
                        intResult.Memory.Span.Clear(); // NativeMemory.Alloc doesn't zero; ensure non-matches are 0
                        Compute.StringKernels.EndsWith(utf8.DataBytes.Span, utf8.Offsets.Span, suffix, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_endswith", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.EndsWith requires Utf8StringSeries.");
                }

                else if (mce.Method.Name == "Str_ToUpperOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var result = Compute.StringKernels.ToUppercase(utf8);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.ToUppercase requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToLowerOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var result = Compute.StringKernels.ToLowercase(utf8);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Str.ToLowercase requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "RegexMatchOp")
                {
                    string colName = (string)((ConstantExpression)mce.Arguments[0]).Value!;
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var series = df.GetColumn(colName);
                    if (series is Data.Utf8StringSeries utf8)
                    {
                        var intResult = new Data.Int32Series(utf8.Name + "_match", utf8.Length);
                        intResult.Memory.Span.Clear();
                        Compute.StringKernels.RegexMatch(utf8.DataBytes.Span, utf8.Offsets.Span, pattern, intResult.Memory.Span);
                        intResult.ValidityMask.CopyFrom(utf8.ValidityMask);
                        // Convert Int32Series (0/1) to BooleanSeries
                        var result = new Data.BooleanSeries(utf8.Name + "_match", utf8.Length);
                        for (int i = 0; i < utf8.Length; i++)
                        {
                            result.Memory.Span[i] = intResult.Memory.Span[i] != 0;
                            if (intResult.ValidityMask.IsNull(i)) result.ValidityMask.SetNull(i);
                        }
                        disposables.Add(intResult);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("RegexMatch requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "WhenThenOtherwiseOp")
                {
                    var conditionExpr = mce.Arguments[0];
                    var thenExpr = mce.Arguments[1];
                    var otherwiseExpr = mce.Arguments[2];

                    var conditionSeries = EvaluateExpression(conditionExpr, df, disposables);
                    var thenSeries = EvaluateExpression(thenExpr, df, disposables);
                    var otherwiseSeries = EvaluateExpression(otherwiseExpr, df, disposables);

                    var result = Compute.ConditionalKernels.Select(conditionSeries, thenSeries, otherwiseSeries);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsNullOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = new Data.BooleanSeries(series.Name + "_isnull", series.Length);
                    var resSpan = result.Memory.Span;
                    for (int i = 0; i < series.Length; i++) resSpan[i] = series.ValidityMask.IsNull(i);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsNotNullOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = new Data.BooleanSeries(series.Name + "_isnotnull", series.Length);
                    var resSpan = result.Memory.Span;
                    for (int i = 0; i < series.Length; i++) resSpan[i] = series.ValidityMask.IsValid(i);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CastOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var targetType = (Type)((ConstantExpression)mce.Arguments[1]).Value!;

                    if (targetType == typeof(CategoricalSeries))
                    {
                        if (series is Data.Utf8StringSeries utf8)
                        {
                            var strings = new string[utf8.Length];
                            for (int i = 0; i < utf8.Length; i++)
                            {
                                if (utf8.ValidityMask.IsValid(i))
                                    strings[i] = System.Text.Encoding.UTF8.GetString(utf8.GetStringSpan(i));
                                else
                                    strings[i] = null!;
                            }
                            var result = CategoricalSeries.FromStrings(utf8.Name, strings);
                            disposables.Add(result);
                            return result;
                        }
                    }
                    throw new NotSupportedException($"Cast to {targetType.Name} not yet implemented.");
                }
                else if (mce.Method.Name == "List_SumOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Sum(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Sum requires ListSeries.");
                }

                else if (mce.Method.Name == "List_MeanOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Mean(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Mean requires ListSeries.");
                }
                else if (mce.Method.Name == "List_MinOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Min(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Min requires ListSeries.");
                }
                else if (mce.Method.Name == "List_MaxOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Max(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Max requires ListSeries.");
                }

                else if (mce.Method.Name == "List_LengthsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Lengths(list.Offsets);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Lengths requires ListSeries.");
                }
                else if (mce.Method.Name == "List_GetOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int index = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.GetItem(list.Offsets, list.Values, index);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Get requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ContainsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var elementExpr = mce.Arguments[1];
                    object? element = null;

                    var actualExpr = elementExpr;
                    if (actualExpr is UnaryExpression ue && ue.NodeType == ExpressionType.Convert)
                        actualExpr = ue.Operand;

                    if (actualExpr is ConstantExpression ce)
                    {
                        element = ce.Value;
                        // If the element is an Expr (e.g., Expr.Lit(1)), evaluate it to get the actual value
                        if (element is Expr exprElement)
                        {
                            var elementSeries = EvaluateExpression(exprElement.Expression, df, disposables);
                            element = elementSeries.Get(0);
                        }
                    }
                    else
                    {
                        var elementSeries = EvaluateExpression(elementExpr, df, disposables);
                        element = elementSeries.Get(0);
                    }

                    if (series is Data.ListSeries list && element != null)
                    {
                        var result = Compute.ListKernels.Contains(list.Offsets, list.Values, element);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Contains requires ListSeries and a valid element.");
                }

                else if (mce.Method.Name == "List_JoinOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string sep = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Join(list.Offsets, list.Values, sep);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Join requires ListSeries.");
                }
                else if (mce.Method.Name == "List_UniqueOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Unique(list.Offsets, list.Values);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Unique requires ListSeries.");
                }
                else if (mce.Method.Name == "List_SortOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool descending = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Sort(list.Offsets, list.Values, descending);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Sort requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ReverseOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Reverse(list.Offsets, list.Values);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Reverse requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ArgMinOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.ArgMin(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.ArgMin requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ArgMaxOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.ArgMax(list.Offsets, list.Values);
                        PropagateListNulls(list, result);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.ArgMax requires ListSeries.");
                }
                else if (mce.Method.Name == "List_DiffOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Diff(list.Offsets, list.Values, n);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Diff requires ListSeries.");
                }
                else if (mce.Method.Name == "List_ShiftOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Shift(list.Offsets, list.Values, n);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Shift requires ListSeries.");
                }
                else if (mce.Method.Name == "List_SliceOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int offset = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int? lengthArg = (int?)((ConstantExpression)mce.Arguments[2]).Value;
                    if (series is Data.ListSeries list)
                    {
                        var result = Compute.ListKernels.Slice(list.Offsets, list.Values, offset, lengthArg);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Slice requires ListSeries.");
                }
                else if (mce.Method.Name == "List_EvalOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.ListSeries list)
                    {
                        var elemExprBody = mce.Arguments[1];
                        var result = Compute.ListKernels.Eval(list, (subDf) => EvaluateExpression(elemExprBody, subDf, disposables));
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("List.Eval requires ListSeries.");
                }
                else if (mce.Method.Name == "ElementOp")
                {
                    return df.GetColumn("element");
                }
                else if (mce.Method.Name == "Struct_FieldOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string fieldName = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.StructSeries structSeries)
                    {
                        var field = structSeries.Fields.FirstOrDefault(f => f.Name == fieldName);
                        if (field == null) throw new ArgumentException($"Field '{fieldName}' not found in struct.");
                        return field;
                    }
                    throw new NotSupportedException("Struct.Field requires StructSeries.");
                }
                else if (mce.Method.Name == "Struct_RenameFieldsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string[] newNames = (string[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.StructSeries structSeries)
                    {
                        var result = Compute.StructKernels.RenameFields(structSeries, newNames);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Struct.RenameFields requires StructSeries.");
                }
                else if (mce.Method.Name == "Struct_JsonEncodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.StructSeries structSeries)
                    {
                        var result = Compute.StructKernels.JsonEncode(structSeries);
                        disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Struct.JsonEncode requires StructSeries.");
                }
                else if (mce.Method.Name == "Struct_WithFieldsOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var fieldExprArr = (Expr[])((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.StructSeries structSeries)
                    {
                        var newFields = new List<ISeries>();
                        foreach (var fe in fieldExprArr)
                        {
                            var fieldBody = fe.Expression;
                            string alias = null;
                            if (fieldBody is MethodCallExpression fmce && fmce.Method.Name == "AliasOp")
                            {
                                alias = (string)((ConstantExpression)fmce.Arguments[1]).Value!;
                                fieldBody = fmce.Arguments[0];
                            }
                            else if (fieldBody is MethodCallExpression colMce && colMce.Method.Name == "Col")
                            {
                                alias = (string)((ConstantExpression)colMce.Arguments[0]).Value!;
                            }
                            if (alias == null) throw new InvalidOperationException("Struct.WithFields requires each field expression to have an alias.");
                            var fieldSeries = EvaluateExpression(fieldBody, df, disposables);
                            fieldSeries.Rename(alias);
                            if (fieldSeries is IDisposable d && disposables.Contains(d))
                                disposables.Remove(d);
                            newFields.Add(fieldSeries);
                        }
                        var result = Compute.StructKernels.WithFields(structSeries, newFields.ToArray());
                        if (result is IDisposable rd && !disposables.Contains(rd))
                            disposables.Add(result);
                        return result;
                    }
                    throw new NotSupportedException("Struct.WithFields requires StructSeries.");
                }
                else if (mce.Method.Name == "Dt_YearOp" || mce.Method.Name == "YearOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractYear(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MonthOp" || mce.Method.Name == "MonthOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractMonth(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_DayOp" || mce.Method.Name == "DayOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractDay(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_HourOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractHour(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MinuteOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractMinute(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_SecondOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractSecond(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "MedianOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Median();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CountOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Count();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "NUniqueOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.NUnique();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "UniqueOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = series.Unique();
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "QuantileOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double quantile = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = series.Quantile(quantile);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DurationOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    // If already a DurationSeries, just return it (no-op conversion)
                    if (series is Data.DurationSeries ds) return ds;
                    throw new NotSupportedException("DurationOp requires a DurationSeries input.");
                }
                else if (mce.Method.Name == "Dt_AddDurationOp" || mce.Method.Name == "Temporal_AddDurationOp")
                {

                    var temporal = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var duration = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var res = Compute.TemporalKernels.AddDuration(temporal, duration);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_SubtractDurationOp" || mce.Method.Name == "Temporal_SubtractDurationOp")
                {
                    var temporal = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var duration = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var res = Compute.TemporalKernels.SubtractDuration(temporal, duration);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_SubtractOp" || mce.Method.Name == "Temporal_SubtractOp")
                {
                    var left = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var right = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var res = Compute.TemporalKernels.Subtract(left, right);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "TotalDaysOp" || mce.Method.Name == "Duration_TotalDaysOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractTotalDays(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "TotalHoursOp" || mce.Method.Name == "Duration_TotalHoursOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractTotalHours(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "TotalSecondsOp" || mce.Method.Name == "Duration_TotalSecondsOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractTotalSeconds(series);
                    disposables.Add(res);
                    return res;
                }

                // --- Weekday / Quarter ---
                else if (mce.Method.Name == "WeekdayOp" || mce.Method.Name == "Dt_WeekdayOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractWeekday(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "QuarterOp" || mce.Method.Name == "Dt_QuarterOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractQuarter(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_OffsetByOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string duration = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.OffsetBy(series, duration);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_RoundOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string every = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.Round(series, every);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_EpochOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.ExtractEpoch(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_TruncateOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string every = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.Truncate(series, every);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_OrdinalDayOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.ExtractOrdinalDay(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_TimestampOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.ExtractTimestamp(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_WithTimeUnitOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.WithTimeUnit(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_CastTimeUnitOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string unit = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    var res = Compute.TemporalKernels.CastTimeUnit(series, unit);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MonthStartOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.MonthStart(series);
                    disposables.Add(res);
                    return res;
                }
                else if (mce.Method.Name == "Dt_MonthEndOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var res = Compute.TemporalKernels.MonthEnd(series);
                    disposables.Add(res);
                    return res;
                }
                // --- First / Last ---
                else if (mce.Method.Name == "FirstOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.First(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "LastOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.Last(series);
                    disposables.Add(result);
                    return result;
                }
                // --- IsDuplicated / IsUnique ---
                else if (mce.Method.Name == "IsDuplicatedOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.UniqueKernels.IsDuplicated(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "IsUniqueOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.UniqueKernels.IsUnique(series);
                    disposables.Add(result);
                    return result;
                }
                // --- EWMStd ---
                else if (mce.Method.Name == "EWMStdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double alpha = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.EWMStd(series, alpha);
                    result.Rename(series.Name + "_ewm_std");
                    disposables.Add(result);
                    return result;
                }
                // --- Array ops: Shift / Diff / Abs / Clip / DropNulls ---
                else if (mce.Method.Name == "ShiftOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.Shift(series, n);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DiffOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.Diff(series, n);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "AbsOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.ArrayKernels.Abs(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ClipOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    double min = (double)((ConstantExpression)mce.Arguments[1]).Value!;
                    double max = (double)((ConstantExpression)mce.Arguments[2]).Value!;
                    var result = Compute.ArrayKernels.Clip(series, min, max);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "DropNullsOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.ArrayKernels.DropNulls(series);
                    disposables.Add(result);
                    return result;
                }

                // --- Math ops: Sqrt / Log / Log10 / Exp / Sin / Cos / Tan ---
                else if (mce.Method.Name == "SqrtOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Sqrt(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "LogOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Log(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "Log10Op")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Log10(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "ExpOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Exp(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Sin(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CosOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Cos(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "TanOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Tan(series);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RankOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool descending = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.MathKernels.Rank(series, descending);
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "PctChangeOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.MathKernels.PctChange(series, n);
                    disposables.Add(result);
                    return result;
                }

                // --- String ops ---
                else if (mce.Method.Name == "Str_ReplaceOp")

                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string oldValue = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    string newValue = (string)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Replace(utf8, oldValue, newValue); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Replace requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ReplaceAllOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string oldValue = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    string newValue = (string)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ReplaceAll(utf8, oldValue, newValue); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ReplaceAll requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_StripOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Strip(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Strip requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_LStripOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.LStrip(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.LStrip requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_RStripOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.RStrip(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.RStrip requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_SplitOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string separator = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Split(utf8, separator); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Split requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_SliceOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int start = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int? length = (int?)((ConstantExpression)mce.Arguments[2]).Value;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Slice(utf8, start, length); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Slice requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToDateOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string? format = (string?)((ConstantExpression)mce.Arguments[1]).Value;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ParseDate(utf8, format); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ToDate requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToDatetimeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string? format = (string?)((ConstantExpression)mce.Arguments[1]).Value;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ParseDatetime(utf8, format); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ToDatetime requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_HeadOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Head(utf8, n); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Head requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_TailOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Tail(utf8, n); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Tail requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_PadStartOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int width = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    char fillChar = (char)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.PadStart(utf8, width, fillChar); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.PadStart requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_PadEndOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int width = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    char fillChar = (char)((ConstantExpression)mce.Arguments[2]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.PadEnd(utf8, width, fillChar); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.PadEnd requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ToTitlecaseOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ToTitlecase(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ToTitlecase requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ExtractOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Extract(utf8, pattern); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Extract requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ReverseOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.Reverse(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.Reverse requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_ExtractAllOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    string pattern = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.ExtractAll(utf8, pattern); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.ExtractAll requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_JsonEncodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.JsonEncode(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.JsonEncode requires Utf8StringSeries.");
                }
                else if (mce.Method.Name == "Str_JsonDecodeOp")
                {
                    var series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    if (series is Data.Utf8StringSeries utf8) { var result = Compute.StringKernels.JsonDecode(utf8); disposables.Add(result); return result; }
                    throw new NotSupportedException("Str.JsonDecode requires Utf8StringSeries.");
                }
                // --- Floor / Ceil / Round ---
                else if (mce.Method.Name == "FloorOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Floor(series);
                    result.Rename(series.Name + "_floor");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CeilOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.MathKernels.Ceil(series);
                    result.Rename(series.Name + "_ceil");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "RoundOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int decimals = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.MathKernels.Round(series, decimals);
                    result.Rename(series.Name + "_round");
                    disposables.Add(result);
                    return result;
                }
                // --- Cumulative ops ---
                else if (mce.Method.Name == "CumCountOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool reverse = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.ExpandingCount(series, reverse);
                    result.Rename(series.Name + "_cum_count");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "CumProdOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    bool reverse = (bool)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.WindowKernels.ExpandingProd(series, reverse);
                    result.Rename(series.Name + "_cum_prod");
                    disposables.Add(result);
                    return result;
                }
                // --- NullCount (aggregation) ---
                else if (mce.Method.Name == "NullCountOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.NullCount(series);
                    disposables.Add(result);
                    return result;
                }
                // --- ArgMin (aggregation) ---
                else if (mce.Method.Name == "ArgMinOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.ArgMin(series);
                    disposables.Add(result);
                    return result;
                }
                // --- ArgMax (aggregation) ---
                else if (mce.Method.Name == "ArgMaxOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    var result = Compute.AggregationKernels.ArgMax(series);
                    disposables.Add(result);
                    return result;
                }
                // --- Select ops: GatherEvery, SearchSorted, Slice, TopK, BottomK ---
                else if (mce.Method.Name == "GatherEveryOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int n = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int offset = (int)((ConstantExpression)mce.Arguments[2]).Value!;
                    var result = Compute.ArrayKernels.GatherEvery(series, n, offset);
                    result.Rename(series.Name + "_gather_every");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SearchSortedOp")
                {
                    ISeries source = EvaluateExpression(mce.Arguments[0], df, disposables);
                    ISeries element = EvaluateExpression(mce.Arguments[1], df, disposables);
                    var result = Compute.ArrayKernels.SearchSorted(source, element);
                    result.Rename("search_sorted");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "SliceOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int offset = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    int? length = (int?)((ConstantExpression)mce.Arguments[2]).Value;
                    var result = Compute.ArrayKernels.SliceSeries(series, offset, length);
                    result.Rename(series.Name + "_slice");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "TopKOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int k = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.TopKSeries(series, k);
                    result.Rename(series.Name + "_topk");
                    disposables.Add(result);
                    return result;
                }
                else if (mce.Method.Name == "BottomKOp")
                {
                    ISeries series = EvaluateExpression(mce.Arguments[0], df, disposables);
                    int k = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                    var result = Compute.ArrayKernels.BottomKSeries(series, k);
                    result.Rename(series.Name + "_bottomk");
                    disposables.Add(result);
                    return result;
                }
            }

            if (body is BinaryExpression binary)
            {
                var left = EvaluateExpression(binary.Left, df, disposables);
                var right = EvaluateExpression(binary.Right, df, disposables);

                bool isCmp = binary.NodeType == ExpressionType.Equal || binary.NodeType == ExpressionType.NotEqual ||
                             binary.NodeType == ExpressionType.GreaterThan || binary.NodeType == ExpressionType.GreaterThanOrEqual ||
                             binary.NodeType == ExpressionType.LessThan || binary.NodeType == ExpressionType.LessThanOrEqual;

                Compute.FilterOperation cmpOp = isCmp ? binary.NodeType switch
                {
                    ExpressionType.Equal => Compute.FilterOperation.Equal,
                    ExpressionType.NotEqual => Compute.FilterOperation.NotEqual,
                    ExpressionType.GreaterThan => Compute.FilterOperation.GreaterThan,
                    ExpressionType.GreaterThanOrEqual => Compute.FilterOperation.GreaterThanOrEqual,
                    ExpressionType.LessThan => Compute.FilterOperation.LessThan,
                    ExpressionType.LessThanOrEqual => Compute.FilterOperation.LessThanOrEqual,
                    _ => default
                } : default;

                if (isCmp)
                {
                    var result = DispatchCompare(left, right, cmpOp);
                    CombineMasks(left, right, result);
                    disposables.Add(result);
                    return result;
                }

                // Arithmetic
                var arithResult = DispatchArithmetic(left, right, binary.NodeType);
                CombineMasks(left, right, arithResult);
                disposables.Add(arithResult);
                return arithResult;
            }

            if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Negate)
            {
                var operand = EvaluateExpression(unary.Operand, df, disposables);
                if (operand is Data.Int32Series i32)
                {
                    var result = new Data.Int32Series("neg", i32.Length);
                    for (int i = 0; i < i32.Length; i++)
                        if (i32.ValidityMask.IsValid(i))
                            result.Memory.Span[i] = -i32.Memory.Span[i];
                        else
                            result.ValidityMask.SetNull(i);
                    disposables.Add(result);
                    return result;
                }
                if (operand is Data.Int64Series i64)
                {
                    var result = new Data.Int64Series("neg", i64.Length);
                    for (int i = 0; i < i64.Length; i++)
                        if (i64.ValidityMask.IsValid(i))
                            result.Memory.Span[i] = -i64.Memory.Span[i];
                        else
                            result.ValidityMask.SetNull(i);
                    disposables.Add(result);
                    return result;
                }
                if (operand is Data.Float64Series f64)
                {
                    var result = new Data.Float64Series("neg", f64.Length);
                    for (int i = 0; i < f64.Length; i++)
                        if (f64.ValidityMask.IsValid(i))
                            result.Memory.Span[i] = -f64.Memory.Span[i];
                        else
                            result.ValidityMask.SetNull(i);
                    disposables.Add(result);
                    return result;
                }
                throw new NotSupportedException($"Negation not supported for {operand.DataType.Name}");
            }

            if (body is ConstantExpression constExpr)
            {
                return CreateLiteralSeries(constExpr.Value, df.RowCount);
            }

            throw new NotSupportedException($"Unsupported expression: {body}");

        }

        private void CombineMasks(ISeries left, ISeries right, ISeries result)
        {
            // Handle broadcasting: if one is length 1, just use the other's mask
            if (left.Length == 1 && right.Length > 1)
            {
                result.ValidityMask.CopyFrom(right.ValidityMask);
            }
            else if (right.Length == 1 && left.Length > 1)
            {
                result.ValidityMask.CopyFrom(left.ValidityMask);
            }
            else
            {
                result.ValidityMask.CopyFrom(left.ValidityMask);
                result.ValidityMask.And(right.ValidityMask);
            }
        }


        private ISeries CreateLiteralSeries(object? val, int rowCount)
        {
            if (val is int i) return new Data.Int32Series("lit", Enumerable.Repeat(i, rowCount).ToArray());
            if (val is double d) return new Data.Float64Series("lit", Enumerable.Repeat(d, rowCount).ToArray());
            if (val is string s) return new Data.Utf8StringSeries("lit", Enumerable.Repeat(s, rowCount).ToArray());
            if (val is bool b) return new Data.BooleanSeries("lit", Enumerable.Repeat(b, rowCount).ToArray());
            if (val is long l) return new Data.Int64Series("lit", Enumerable.Repeat(l, rowCount).ToArray());
            if (val is decimal m) return new Data.DecimalSeries("lit", Enumerable.Repeat<decimal?>(m, rowCount).ToArray());
            throw new NotSupportedException($"Literal type {val?.GetType().Name} not supported.");
        }

        private ISeries DispatchCompare(ISeries left, ISeries right, Compute.FilterOperation op)
        {
            // Simple double-dispatch or switch on type
            if (left is Data.Int32Series l32 && right is Data.Int32Series r32) return Compute.ComparisonKernels.Compare(l32, r32, op);
            if (left is Data.Float64Series lF64 && right is Data.Float64Series rF64) return Compute.ComparisonKernels.Compare(lF64, rF64, op);
            if (left is Data.Int64Series l64 && right is Data.Int64Series r64) return Compute.ComparisonKernels.Compare(l64, r64, op);
            if (left is Data.Utf8StringSeries lStr && right is Data.Utf8StringSeries rStr) return CompareStringSeries(lStr, rStr, op);
            // Promote minor integer types to Int32 / Int64 / Float64 for comparison
            if (left is Data.Int8Series || left is Data.Int16Series || left is Data.UInt8Series || left is Data.UInt16Series)
                return DispatchCompare(PromoteToInt32Series(left), right, op);
            if (right is Data.Int8Series || right is Data.Int16Series || right is Data.UInt8Series || right is Data.UInt16Series)
                return DispatchCompare(left, PromoteToInt32Series(right), op);
            if (left is Data.UInt32Series) return DispatchCompare(PromoteToInt64Series(left), right, op);
            if (right is Data.UInt32Series) return DispatchCompare(left, PromoteToInt64Series(right), op);
            if (left is Data.Float32Series) return DispatchCompare(PromoteToFloat64Series(left), right, op);
            if (right is Data.Float32Series) return DispatchCompare(left, PromoteToFloat64Series(right), op);

            // Handle promotion if needed, but for now strict types
            throw new NotSupportedException($"Comparison between {left.DataType.Name} and {right.DataType.Name} not supported.");
        }

        private static Data.BooleanSeries CompareStringSeries(Data.Utf8StringSeries left, Data.Utf8StringSeries right, Compute.FilterOperation op)
        {
            int len = Math.Max(left.Length, right.Length);
            // Scalar broadcast: if one side is length 1, treat it as a scalar
            bool leftScalar = left.Length == 1;
            bool rightScalar = right.Length == 1;
            var result = new Data.BooleanSeries("cmp", len);
            for (int i = 0; i < len; i++)
            {
                int li = leftScalar ? 0 : i;
                int ri = rightScalar ? 0 : i;
                if (left.ValidityMask.IsNull(li) || right.ValidityMask.IsNull(ri))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }
                string ls = System.Text.Encoding.UTF8.GetString(left.GetStringSpan(li));
                string rs = System.Text.Encoding.UTF8.GetString(right.GetStringSpan(ri));
                int cmp = string.Compare(ls, rs, StringComparison.Ordinal);
                result.Memory.Span[i] = op switch
                {
                    Compute.FilterOperation.Equal => cmp == 0,
                    Compute.FilterOperation.NotEqual => cmp != 0,
                    Compute.FilterOperation.GreaterThan => cmp > 0,
                    Compute.FilterOperation.GreaterThanOrEqual => cmp >= 0,
                    Compute.FilterOperation.LessThan => cmp < 0,
                    Compute.FilterOperation.LessThanOrEqual => cmp <= 0,
                    _ => false
                };
            }
            return result;
        }

        // Thin wrappers so DispatchCompare can delegate to existing PromoteSeries helpers
        private static Data.Int32Series PromoteToInt32Series(ISeries s) => PromoteToInt32(s);
        private static Data.Int64Series PromoteToInt64Series(ISeries s) => PromoteToInt64(s);
        private static Data.Float64Series PromoteToFloat64Series(ISeries s) => PromoteToFloat64(s);

        private ISeries Broadcast(ISeries series, int length)
        {
            if (series.Length == length) return series;
            if (series is Data.Int32Series i32)
            {
                var val = i32.Memory.Span[0];
                var result = new Data.Int32Series(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            if (series is Data.Float64Series f64)
            {
                var val = f64.Memory.Span[0];
                var result = new Data.Float64Series(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            if (series is Data.Int64Series i64)
            {
                var val = i64.Memory.Span[0];
                var result = new Data.Int64Series(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            if (series is Data.BooleanSeries bs)
            {
                var val = bs.Memory.Span[0];
                var result = new Data.BooleanSeries(series.Name, length);
                result.Memory.Span.Fill(val);
                result.ValidityMask.CopyFrom(series.ValidityMask);
                return result;
            }
            return series;
        }

        private ISeries BroadcastScalar(ISeries series, int length)
        {
            if (series.Length == length) return series;
            if (length <= 0) return series;
            if (series is Data.Int32Series i32)
            {
                var val = i32.Memory.Span[0];
                var result = new Data.Int32Series(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            if (series is Data.Float64Series f64)
            {
                var val = f64.Memory.Span[0];
                var result = new Data.Float64Series(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            if (series is Data.Int64Series i64)
            {
                var val = i64.Memory.Span[0];
                var result = new Data.Int64Series(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            if (series is Data.BooleanSeries bs)
            {
                var val = bs.Memory.Span[0];
                var result = new Data.BooleanSeries(series.Name, length);
                for (int i = 0; i < length; i++) result.Memory.Span[i] = val;
                return result;
            }
            return series;
        }

        private ISeries DispatchArithmetic(ISeries left, ISeries right, ExpressionType type)
        {
            // Broadcast scalars to match vector length
            if (left.Length == 1 && right.Length > 1)
                left = BroadcastScalar(left, right.Length);
            else if (right.Length == 1 && left.Length > 1)
                right = BroadcastScalar(right, left.Length);

            // Boolean (And/Or) - BooleanSeries & BooleanSeries = BooleanSeries
            if (left is Data.BooleanSeries lBool && right is Data.BooleanSeries rBool)
            {
                var result = new Data.BooleanSeries("res", lBool.Length);
                if (type == ExpressionType.And)
                    for (int i = 0; i < lBool.Length; i++)
                        result.Memory.Span[i] = lBool.Memory.Span[i] && rBool.Memory.Span[i];
                else if (type == ExpressionType.Or)
                    for (int i = 0; i < lBool.Length; i++)
                        result.Memory.Span[i] = lBool.Memory.Span[i] || rBool.Memory.Span[i];
                else
                    throw new NotSupportedException($"BooleanSeries does not support operation {type}");
                result.ValidityMask.CopyFrom(lBool.ValidityMask);
                result.ValidityMask.And(rBool.ValidityMask);
                return result;
            }

            if (left is Data.DatetimeSeries || left is Data.DateSeries)
            {

                if (right is Data.DurationSeries)
                {
                    if (type == ExpressionType.Add) return Compute.TemporalKernels.AddDuration(left, right);
                    if (type == ExpressionType.Subtract) return Compute.TemporalKernels.SubtractDuration(left, right);
                }
                if (right is Data.DatetimeSeries && type == ExpressionType.Subtract)
                {
                    return Compute.TemporalKernels.Subtract(left, right);
                }
            }

            // Exact type matches (fast path)
            if (left is Data.Int32Series l32 && right is Data.Int32Series r32)
            {
                // Division: Int32/Int32 should promote to Float64 (like Python Polars)
                if (type == ExpressionType.Divide)
                {
                    var divRes = new Data.Float64Series("res", l32.Length);
                    var lSpan = l32.Memory.Span;
                    var rSpan = r32.Memory.Span;
                    var resSpan = divRes.Memory.Span;
                    for (int i = 0; i < l32.Length; i++)
                        resSpan[i] = (double)lSpan[i] / rSpan[i];
                    return divRes;
                }
                var res = new Data.Int32Series("res", l32.Length);
                if (type == ExpressionType.Add) Compute.ArithmeticKernels.Add<int>(l32.Memory.Span, r32.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Subtract) Compute.ArithmeticKernels.Subtract<int>(l32.Memory.Span, r32.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Multiply) Compute.ArithmeticKernels.Multiply<int>(l32.Memory.Span, r32.Memory.Span, res.Memory.Span);
                return res;
            }
            if (left is Data.Float64Series lF64 && right is Data.Float64Series rF64)
            {
                var res = new Data.Float64Series("res", lF64.Length);
                if (type == ExpressionType.Add) Compute.ArithmeticKernels.Add<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Subtract) Compute.ArithmeticKernels.Subtract<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Multiply) Compute.ArithmeticKernels.Multiply<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Divide) Compute.ArithmeticKernels.Divide<double>(lF64.Memory.Span, rF64.Memory.Span, res.Memory.Span);
                return res;
            }

            if (left is Data.Int64Series l64 && right is Data.Int64Series r64)
            {
                var res = new Data.Int64Series("res", l64.Length);
                if (type == ExpressionType.Add) Compute.ArithmeticKernels.Add<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Subtract) Compute.ArithmeticKernels.Subtract<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Multiply) Compute.ArithmeticKernels.Multiply<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                else if (type == ExpressionType.Divide) Compute.ArithmeticKernels.Divide<long>(l64.Memory.Span, r64.Memory.Span, res.Memory.Span);
                return res;
            }

            // Same-type matches for the remaining integer and float types
            if (left is Data.Int8Series lI8 && right is Data.Int8Series rI8)
            {
                var res = new Data.Int32Series("res", lI8.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] + rI8.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] - rI8.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] * rI8.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lI8.Length; i++) res.Memory.Span[i] = lI8.Memory.Span[i] / rI8.Memory.Span[i];
                return res;
            }
            if (left is Data.Int16Series lI16 && right is Data.Int16Series rI16)
            {
                var res = new Data.Int32Series("res", lI16.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] + rI16.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] - rI16.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] * rI16.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lI16.Length; i++) res.Memory.Span[i] = lI16.Memory.Span[i] / rI16.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt8Series lU8 && right is Data.UInt8Series rU8)
            {
                var res = new Data.Int32Series("res", lU8.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] + rU8.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] - rU8.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] * rU8.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU8.Length; i++) res.Memory.Span[i] = lU8.Memory.Span[i] / rU8.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt16Series lU16 && right is Data.UInt16Series rU16)
            {
                var res = new Data.Int32Series("res", lU16.Length); // Widen to Int32
                if (type == ExpressionType.Add) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] + rU16.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] - rU16.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] * rU16.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU16.Length; i++) res.Memory.Span[i] = lU16.Memory.Span[i] / rU16.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt32Series lU32 && right is Data.UInt32Series rU32)
            {
                var res = new Data.Int64Series("res", lU32.Length); // Widen to Int64
                if (type == ExpressionType.Add) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] + (long)rU32.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] - (long)rU32.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] * (long)rU32.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU32.Length; i++) res.Memory.Span[i] = (long)lU32.Memory.Span[i] / (long)rU32.Memory.Span[i];
                return res;
            }
            if (left is Data.UInt64Series lU64 && right is Data.UInt64Series rU64)
            {
                var res = new Data.Float64Series("res", lU64.Length); // Widen to Float64 for safety
                if (type == ExpressionType.Add) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] + (double)rU64.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] - (double)rU64.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] * (double)rU64.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lU64.Length; i++) res.Memory.Span[i] = (double)lU64.Memory.Span[i] / (double)rU64.Memory.Span[i];
                return res;
            }
            if (left is Data.Float32Series lF32 && right is Data.Float32Series rF32)
            {
                var res = new Data.Float64Series("res", lF32.Length); // Widen to Float64
                if (type == ExpressionType.Add) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] + (double)rF32.Memory.Span[i];
                else if (type == ExpressionType.Subtract) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] - (double)rF32.Memory.Span[i];
                else if (type == ExpressionType.Multiply) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] * (double)rF32.Memory.Span[i];
                else if (type == ExpressionType.Divide) for (int i = 0; i < lF32.Length; i++) res.Memory.Span[i] = (double)lF32.Memory.Span[i] / (double)rF32.Memory.Span[i];
                return res;
            }


            // Mixed-type promotion: promote narrower types to wider types
            // Determine the target type based on the widest type present
            var typeOrder = GetNumericTypeOrder(left, right);
            if (typeOrder != null)
            {
                var (promotedLeft, promotedRight, resultType) = typeOrder.Value;
                if (promotedLeft != null) return DispatchArithmetic(promotedLeft, right, type);
                if (promotedRight != null) return DispatchArithmetic(left, promotedRight, type);
                // Both promoted? shouldn't happen since we assign both
            }

            // Handle NullSeries: NullSeries op X = NullSeries
            if (left is Data.NullSeries) return new Data.NullSeries("null", left.Length);
            if (right is Data.NullSeries) return new Data.NullSeries("null", right.Length);

            // Handle Decimal arithmetic (decimal doesn't support INumber<T>, use manual loop)
            if (left is Data.DecimalSeries lDec && right is Data.DecimalSeries rDec)
            {
                var res = new Data.DecimalSeries("res", lDec.Length);
                var lSpan = lDec.Memory.Span;
                var rSpan = rDec.Memory.Span;
                var rSpan2 = res.Memory.Span;
                if (type == ExpressionType.Add)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] + rSpan[i];
                else if (type == ExpressionType.Subtract)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] - rSpan[i];
                else if (type == ExpressionType.Multiply)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] * rSpan[i];
                else if (type == ExpressionType.Divide)
                    for (int i = 0; i < lSpan.Length; i++) rSpan2[i] = lSpan[i] / rSpan[i];
                return res;
            }

            throw new NotSupportedException($"Arithmetic between {left.DataType.Name} and {right.DataType.Name} not supported.");
        }

        /// <summary>
        /// Determines if type promotion is needed and returns (promotedLeft, promotedRight, resultTypeName).
        /// If a promotion is needed, sets the appropriate side to the promoted series.
        /// </summary>
        private (ISeries? promotedLeft, ISeries? promotedRight, string resultType)? GetNumericTypeOrder(ISeries left, ISeries right)
        {
            // Assign each type a numeric rank (higher = wider)
            int Rank(ISeries s)
            {
                if (s is Data.Float64Series) return 80;
                if (s is Data.Float32Series) return 70;
                if (s is Data.Int64Series) return 60;
                if (s is Data.UInt64Series) return 59; // UInt64 can't safely fit in Int64
                if (s is Data.UInt32Series) return 55;
                if (s is Data.Int32Series) return 50;
                if (s is Data.UInt16Series) return 45;
                if (s is Data.Int16Series) return 40;
                if (s is Data.UInt8Series) return 35;
                if (s is Data.Int8Series) return 30;
                return 0; // Non-numeric
            }

            int leftRank = Rank(left);
            int rightRank = Rank(right);
            if (leftRank == 0 || rightRank == 0) return null; // Non-numeric, skip

            if (leftRank == rightRank) return null; // Same type, already handled

            // Determine if either side needs promotion
            ISeries? promotedLeft = null;
            ISeries? promotedRight = null;
            string resultType = "";

            if (leftRank < rightRank)
            {
                // Promote left to match right
                promotedLeft = PromoteSeries(left, right);
                resultType = right.DataType.Name;
            }
            else
            {
                // Promote right to match left
                promotedRight = PromoteSeries(right, left);
                resultType = left.DataType.Name;
            }

            return (promotedLeft, promotedRight, resultType);
        }

        /// <summary>
        /// Create a new series by promoting 'source' to match 'target's type.
        /// </summary>
        private static ISeries PromoteSeries(ISeries source, ISeries target)
        {
            if (target is Data.Float64Series)
            {
                return PromoteToFloat64(source);
            }
            if (target is Data.Float32Series)
            {
                return PromoteToFloat32(source);
            }
            if (target is Data.Int64Series)
            {
                return PromoteToInt64(source);
            }
            if (target is Data.UInt64Series)
            {
                return PromoteToUInt64(source);
            }
            if (target is Data.UInt32Series)
            {
                return PromoteToUInt32(source);
            }
            if (target is Data.Int32Series)
            {
                return PromoteToInt32(source);
            }
            if (target is Data.UInt16Series)
            {
                return PromoteToUInt16(source);
            }
            if (target is Data.Int16Series)
            {
                return PromoteToInt16(source);
            }
            if (target is Data.UInt8Series)
            {
                return PromoteToUInt8(source);
            }
            if (target is Data.Int8Series)
            {
                return PromoteToInt8(source);
            }
            throw new NotSupportedException($"Cannot promote to {target.DataType.Name}");
        }

        private static Data.Float64Series PromoteToFloat64(ISeries source)
        {
            var f64 = new Data.Float64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { f64.ValidityMask.SetNull(i); continue; }
                f64.Memory.Span[i] = source.Get(i) switch
                {
                    sbyte v => v,
                    byte v => v,
                    short v => v,
                    ushort v => v,
                    int v => v,
                    uint v => v,
                    long v => v,
                    ulong v => v,
                    float v => v,
                    double v => v,
                    _ => 0
                };
            }
            f64.ValidityMask.CopyFrom(source.ValidityMask);
            return f64;
        }

        private static Data.Float32Series PromoteToFloat32(ISeries source)
        {
            var f32 = new Data.Float32Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { f32.ValidityMask.SetNull(i); continue; }
                f32.Memory.Span[i] = Convert.ToSingle(source.Get(i));
            }
            f32.ValidityMask.CopyFrom(source.ValidityMask);
            return f32;
        }

        private static Data.Int64Series PromoteToInt64(ISeries source)
        {
            var i64 = new Data.Int64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i64.ValidityMask.SetNull(i); continue; }
                i64.Memory.Span[i] = Convert.ToInt64(source.Get(i));
            }
            i64.ValidityMask.CopyFrom(source.ValidityMask);
            return i64;
        }

        private static Data.UInt64Series PromoteToUInt64(ISeries source)
        {
            var u64 = new Data.UInt64Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u64.ValidityMask.SetNull(i); continue; }
                u64.Memory.Span[i] = Convert.ToUInt64(source.Get(i));
            }
            u64.ValidityMask.CopyFrom(source.ValidityMask);
            return u64;
        }

        private static Data.UInt32Series PromoteToUInt32(ISeries source)
        {
            var u32 = new Data.UInt32Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u32.ValidityMask.SetNull(i); continue; }
                u32.Memory.Span[i] = Convert.ToUInt32(source.Get(i));
            }
            u32.ValidityMask.CopyFrom(source.ValidityMask);
            return u32;
        }

        private static Data.Int32Series PromoteToInt32(ISeries source)
        {
            var i32 = new Data.Int32Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i32.ValidityMask.SetNull(i); continue; }
                i32.Memory.Span[i] = Convert.ToInt32(source.Get(i));
            }
            i32.ValidityMask.CopyFrom(source.ValidityMask);
            return i32;
        }

        private static Data.UInt16Series PromoteToUInt16(ISeries source)
        {
            var u16 = new Data.UInt16Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u16.ValidityMask.SetNull(i); continue; }
                u16.Memory.Span[i] = Convert.ToUInt16(source.Get(i));
            }
            u16.ValidityMask.CopyFrom(source.ValidityMask);
            return u16;
        }

        private static Data.Int16Series PromoteToInt16(ISeries source)
        {
            var i16 = new Data.Int16Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i16.ValidityMask.SetNull(i); continue; }
                i16.Memory.Span[i] = Convert.ToInt16(source.Get(i));
            }
            i16.ValidityMask.CopyFrom(source.ValidityMask);
            return i16;
        }

        private static Data.UInt8Series PromoteToUInt8(ISeries source)
        {
            var u8 = new Data.UInt8Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { u8.ValidityMask.SetNull(i); continue; }
                u8.Memory.Span[i] = Convert.ToByte(source.Get(i));
            }
            u8.ValidityMask.CopyFrom(source.ValidityMask);
            return u8;
        }

        private static Data.Int8Series PromoteToInt8(ISeries source)
        {
            var i8 = new Data.Int8Series(source.Name, source.Length);
            for (int i = 0; i < source.Length; i++)
            {
                if (source.ValidityMask.IsNull(i)) { i8.ValidityMask.SetNull(i); continue; }
                i8.Memory.Span[i] = Convert.ToSByte(source.Get(i));
            }
            i8.ValidityMask.CopyFrom(source.ValidityMask);
            return i8;
        }

        private async IAsyncEnumerable<DataFrame> ApplyPivot(IAsyncEnumerable<DataFrame> source, string[] index, string pivot, string values, string agg)
        {
            // Pivot is a non-streaming operation. We must materialize the entire source.
            var results = new List<DataFrame>();
            await foreach (var df in source)
            {
                if (df.RowCount > 0) results.Add(df);
            }

            if (results.Count == 0) yield break;

            DataFrame fullDf = results.Count == 1 ? results[0] : DataFrame.Concat(results);
            yield return fullDf.Pivot(index, pivot, values, agg);
        }

        private async IAsyncEnumerable<DataFrame> ApplyUnpivot(IAsyncEnumerable<DataFrame> source, string[] idVars, string[] valueVars, string varName, string valName)
        {
            await foreach (var df in source)
            {
                yield return df.Melt(idVars, valueVars, varName, valName);
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyAgg(IAsyncEnumerable<DataFrame> source, string[] groupColumns, Expression aggregations)
        {
            // Materialize all chunks
            var chunks = new List<DataFrame>();
            await foreach (var df in source) chunks.Add(df);
            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);

            // Extract group columns from the source
            var groupCols = groupColumns.Select(c => fullDf.GetColumn(c)).ToArray();
            var groups = Compute.GroupByKernels.GroupBy(groupCols);

            // Parse aggregations expression to get Expr[]
            List<Expr> aggExprs = new();
            if (aggregations is NewArrayExpression arrExpr)
            {
                foreach (var exprNode in arrExpr.Expressions)
                {
                    if (exprNode is UnaryExpression ue && ue.NodeType == ExpressionType.Convert)
                    {
                        if (ue.Operand is ConstantExpression ce && ce.Value is Expr e)
                            aggExprs.Add(e);
                        else if (ue.Operand is MethodCallExpression mce && mce.Method.Name == "LitOp")
                            aggExprs.Add(Expr.Lit(((ConstantExpression)mce.Arguments[0]).Value!));
                    }
                    else if (exprNode is ConstantExpression ce && ce.Value is Expr e)
                    {
                        aggExprs.Add(e);
                    }
                }
            }

            // Build group key columns (first element of each group)
            var groupKeyCols = new List<ISeries>();
            foreach (var gc in groupColumns)
            {
                var grpSeries = fullDf.GetColumn(gc);
                ISeries keyCol;
                if (grpSeries is Data.Int32Series i32)
                    keyCol = new Data.Int32Series(gc, groups.Count);
                else if (grpSeries is Data.Float64Series f64)
                    keyCol = new Data.Float64Series(gc, groups.Count);
                else if (grpSeries is Data.Utf8StringSeries u8)
                    keyCol = new Data.Utf8StringSeries(gc, groups.Count);
                else if (grpSeries is Data.Int64Series i64)
                    keyCol = new Data.Int64Series(gc, groups.Count);
                else if (grpSeries is Data.BooleanSeries bs)
                    keyCol = new Data.BooleanSeries(gc, groups.Count);
                else
                    keyCol = (ISeries)Activator.CreateInstance(grpSeries.GetType(), gc, groups.Count)!;
                for (int gi = 0; gi < groups.Count; gi++)
                {
                    int firstRow = groups[gi][0];
                    grpSeries.Take(keyCol, firstRow, gi);
                }
                groupKeyCols.Add(keyCol);
            }

            // Evaluate each aggregation expression per group
            var disposables = new List<IDisposable>();
            try
            {
                var resultColumns = new List<ISeries>();
                foreach (var aggExpr in aggExprs)
                {
                    var body = aggExpr.Expression;
                    string alias = null;
                    // Unwrap AliasOp
                    Expression innerBody = body;
                    if (body is MethodCallExpression mce && mce.Method.Name == "AliasOp")
                    {
                        alias = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                        innerBody = mce.Arguments[0];
                    }

                    // Determine if expression is a "simple" single aggregation op that GroupByKernels can handle
                    bool isSimple = innerBody is MethodCallExpression innerMce
                        && innerMce.Arguments.Count == 1
                        && innerMce.Arguments[0] is MethodCallExpression colMce
                        && colMce.Method.Name == "Col";

                    if (isSimple)
                    {
                        // Simple case: SumOp(Col("x")), MinOp(Col("x")), etc.
                        var aggMce = (MethodCallExpression)innerBody;
                        string aggType = aggMce.Method.Name.Replace("Op", "").ToLower();

                        // Handle agg type aliases: "std" for "StdOp", "var" for "VarOp", etc.
                        if (aggType == "std") aggType = "std";
                        if (aggType == "var") aggType = "var";
                        if (aggType == "mean") aggType = "mean";
                        if (aggType == "median") aggType = "median";

                        string colName = (string)((ConstantExpression)((MethodCallExpression)aggMce.Arguments[0]).Arguments[0]).Value!;

                        var series = fullDf.GetColumn(colName);
                        var result = Compute.GroupByKernels.Aggregate(series, groups, aggType);
                        var name = alias ?? $"{colName}_{aggType}";
                        result.Rename(name);
                        resultColumns.Add(result);
                    }
                    else
                    {
                        // Complex case: evaluate expression tree per-group
                        // First, find all Column references in the expression
                        var colTracker = new ColumnTrackerVisitor();
                        colTracker.Visit(innerBody);
                        var usedCols = colTracker.Columns.ToArray();

                        // For each group, create a sub-DataFrame and evaluate
                        ISeries? firstResult = null;
                        int resultIdx = 0;
                        foreach (var group in groups)
                        {
                            // Build sub-DataFrame for this group
                            var subCols = new List<ISeries>();
                            foreach (var colName in usedCols)
                            {
                                var sourceCol = fullDf.GetColumn(colName);
                                int n = group.Count;
                                // Efficient Take: create new series and copy values
                                ISeries subCol;
                                if (sourceCol is Data.Int32Series i32)
                                {
                                    subCol = new Data.Int32Series(colName, n);
                                    for (int i = 0; i < n; i++) ((Data.Int32Series)subCol).Memory.Span[i] = i32.Memory.Span[group[i]];
                                }
                                else if (sourceCol is Data.Float64Series f64)
                                {
                                    subCol = new Data.Float64Series(colName, n);
                                    for (int i = 0; i < n; i++) ((Data.Float64Series)subCol).Memory.Span[i] = f64.Memory.Span[group[i]];
                                }
                                else if (sourceCol is Data.Utf8StringSeries u8)
                                {
                                    int totalBytes = 0;
                                    for (int i = 0; i < n; i++) totalBytes += u8.GetStringSpan(group[i]).Length;
                                    subCol = new Data.Utf8StringSeries(colName, n, totalBytes);
                                    int offset = 0;
                                    for (int i = 0; i < n; i++)
                                    {
                                        var span = u8.GetStringSpan(group[i]);
                                        span.CopyTo(((Data.Utf8StringSeries)subCol).DataBytes.Span.Slice(offset));
                                        offset += span.Length;
                                        ((Data.Utf8StringSeries)subCol).Offsets.Span[i + 1] = offset;
                                    }
                                }
                                else if (sourceCol is Data.Int64Series i64)
                                {
                                    subCol = new Data.Int64Series(colName, n);
                                    for (int i = 0; i < n; i++) ((Data.Int64Series)subCol).Memory.Span[i] = i64.Memory.Span[group[i]];
                                }
                                else if (sourceCol is Data.BooleanSeries bs)
                                {
                                    subCol = new Data.BooleanSeries(colName, n);
                                    for (int i = 0; i < n; i++) ((Data.BooleanSeries)subCol).Memory.Span[i] = bs.Memory.Span[group[i]];
                                }
                                else
                                {
                                    subCol = (ISeries)Activator.CreateInstance(sourceCol.GetType(), colName, n)!;
                                    for (int i = 0; i < n; i++) sourceCol.Take(subCol, group[i], i);
                                }
                                for (int i = 0; i < n; i++)

                                {
                                    if (sourceCol.ValidityMask.IsNull(group[i]))
                                        subCol.ValidityMask.SetNull(i);
                                }
                                subCols.Add(subCol);
                            }
                            var subDf = new DataFrame(subCols);

                            // Evaluate expression against sub-DataFrame
                            var groupResult = EvaluateExpression(innerBody, subDf, disposables);

                            // Result should be 1-row (aggregation reduces)
                            if (firstResult == null)
                            {
                                firstResult = groupResult;
                                // Create the result series sized for all groups
                                if (groupResult is Data.Int32Series ri32)
                                {
                                    var res = new Data.Int32Series(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = ri32.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                                else if (groupResult is Data.Float64Series rf64)
                                {
                                    var res = new Data.Float64Series(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = rf64.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                                else if (groupResult is Data.Int64Series ri64)
                                {
                                    var res = new Data.Int64Series(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = ri64.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                                else if (groupResult is Data.BooleanSeries rb)
                                {
                                    var res = new Data.BooleanSeries(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = rb.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                            }
                            else
                            {
                                // Copy result to the right group index in the last result column
                                var lastResult = resultColumns[resultColumns.Count - 1];
                                if (lastResult is Data.Int32Series ri32 && groupResult is Data.Int32Series gi32)
                                    ri32.Memory.Span[resultIdx] = gi32.Memory.Span[0];
                                else if (lastResult is Data.Float64Series rf64 && groupResult is Data.Float64Series gf64)
                                    rf64.Memory.Span[resultIdx] = gf64.Memory.Span[0];
                                else if (lastResult is Data.Int64Series ri64 && groupResult is Data.Int64Series gi64)
                                    ri64.Memory.Span[resultIdx] = gi64.Memory.Span[0];
                                else if (lastResult is Data.BooleanSeries rb && groupResult is Data.BooleanSeries gb)
                                    rb.Memory.Span[resultIdx] = gb.Memory.Span[0];
                            }
                            resultIdx++;

                        }
                    }
                }

                groupKeyCols.AddRange(resultColumns);
                yield return new DataFrame(groupKeyCols);
            }
            finally
            {
                foreach (var d in disposables) d.Dispose();
            }
        }


        private async IAsyncEnumerable<DataFrame> ApplyTopK(IAsyncEnumerable<DataFrame> source, string[] columnNames, bool[] descending, int k)
        {
            // TopK is a blocking operation - we must collect all data
            var chunks = new List<DataFrame>();
            await foreach (var df in source)
            {
                chunks.Add(df);
            }

            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);

            // Perform sort
            var indices = Compute.SortKernels.TopK(fullDf, columnNames, descending, k);

            // Apply indices to all columns
            var sortedCols = new List<ISeries>();
            foreach (var col in fullDf.Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < indices.Length; i++) totalBytes += u8.GetStringSpan(indices[i]).Length;
                    newCol = new Data.Utf8StringSeries(u8.Name, indices.Length, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, indices.Length)!;
                }
                col.Take(newCol, indices);
                sortedCols.Add(newCol);
            }

            yield return new DataFrame(sortedCols);
        }
        private async IAsyncEnumerable<DataFrame> ApplySort(IAsyncEnumerable<DataFrame> source, string[] columnNames, bool[] descending)
        {
            // Sorting is a blocking operation - we must collect all data
            var chunks = new List<DataFrame>();
            await foreach (var df in source)
            {
                chunks.Add(df);
            }

            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);

            // Perform sort
            var indices = Compute.SortKernels.MultiColumnSort(fullDf, columnNames, descending);

            // Apply indices to all columns
            var sortedCols = new List<ISeries>();
            foreach (var col in fullDf.Columns)
            {
                ISeries newCol;
                if (col is Data.Utf8StringSeries u8)
                {
                    int totalBytes = 0;
                    for (int i = 0; i < indices.Length; i++) totalBytes += u8.GetStringSpan(indices[i]).Length;
                    newCol = new Data.Utf8StringSeries(u8.Name, indices.Length, totalBytes);
                }
                else
                {
                    newCol = (ISeries)Activator.CreateInstance(col.GetType(), col.Name, indices.Length)!;
                }
                col.Take(newCol, indices);
                sortedCols.Add(newCol);
            }

            yield return new DataFrame(sortedCols);
        }
        private async IAsyncEnumerable<DataFrame> ApplyJoin(IAsyncEnumerable<DataFrame> leftSource, IAsyncEnumerable<DataFrame> rightSource, string on, JoinType type)
        {
            var rightTask = Task.Run(async () =>
            {
                var rightResults = new List<DataFrame>();
                await foreach (var df in rightSource) rightResults.Add(df);
                return DataFrame.Concat(rightResults);
            });

            // Use a Channel to allow the left branch to execute in parallel while we build the right-side hash table
            var channel = System.Threading.Channels.Channel.CreateUnbounded<DataFrame>();
            var leftTask = Task.Run(async () =>
            {
                try
                {
                    await foreach (var df in leftSource) await channel.Writer.WriteAsync(df);
                }
                finally
                {
                    channel.Writer.Complete();
                }
            });

            var rightDf = await rightTask;

            await foreach (var leftDf in channel.Reader.ReadAllAsync())
            {
                yield return leftDf.Join(rightDf, on, type);
            }
        }
        private async IAsyncEnumerable<DataFrame> ApplyDelay(IAsyncEnumerable<DataFrame> source, int ms)
        {
            await Task.Delay(ms);
            await foreach (var df in source)
            {
                yield return df;
            }
        }
        private async IAsyncEnumerable<DataFrame> ApplyLimit(IAsyncEnumerable<DataFrame> source, int n)
        {
            if (n == 0)
            {
                // Yield empty DataFrame with schema preserved from the first chunk
                await foreach (var df in source)
                {
                    var emptyCols = df.Columns.Select(col => col.CloneEmpty(0)).ToList();
                    yield return new DataFrame(emptyCols);
                    yield break;
                }
                yield break;
            }

            int count = 0;
            await foreach (var df in source)
            {
                if (count >= n) break;
                if (count + df.RowCount > n)
                {
                    // Slice the last chunk
                    int take = n - count;
                    var slicedColumns = new List<ISeries>();
                    foreach (var col in df.Columns)
                    {
                        if (col is Data.Int32Series i32)
                        {
                            var newCol = new Data.Int32Series(col.Name, take);
                            i32.Memory.Span.Slice(0, take).CopyTo(newCol.Memory.Span);
                            slicedColumns.Add(newCol);
                        }
                        else if (col is Data.Float64Series f64)
                        {
                            var newCol = new Data.Float64Series(col.Name, take);
                            f64.Memory.Span.Slice(0, take).CopyTo(newCol.Memory.Span);
                            slicedColumns.Add(newCol);
                        }
                        else if (col is Data.Utf8StringSeries utf8)
                        {
                            int byteLen = utf8.Offsets.Span[take];
                            var newCol = new Data.Utf8StringSeries(col.Name, take, byteLen);
                            utf8.DataBytes.Span.Slice(0, byteLen).CopyTo(newCol.DataBytes.Span);
                            utf8.Offsets.Span.Slice(0, take + 1).CopyTo(newCol.Offsets.Span);
                            slicedColumns.Add(newCol);
                        }
                        else
                        {
                            // Fallback: use CloneEmpty + Take for other types
                            var newCol = col.CloneEmpty(take);
                            var indices = Enumerable.Range(0, take).ToArray();
                            col.Take(newCol, indices);
                            slicedColumns.Add(newCol);
                        }
                    }
                    yield return new DataFrame(slicedColumns);
                    count = n;
                    break;
                }
                else
                {
                    yield return df;
                    count += df.RowCount;
                }
            }
        }
        private async IAsyncEnumerable<DataFrame> ToAsyncEnumerable(DataFrame df)
        {
            yield return df;
        }
        private async IAsyncEnumerable<DataFrame> ApplyExplode(IAsyncEnumerable<DataFrame> source, string columnName)
        {
            await foreach (var df in source)
            {
                var col = df.GetColumn(columnName);
                if (col is Data.ListSeries listCol)
                {
                    var offsets = listCol.Offsets.Memory.Span;
                    int totalNewRows = listCol.Values.Length;

                    if (totalNewRows == 0) continue;

                    // Build the mapping from new row index to old row index
                    int[] expansionIndices = new int[totalNewRows];
                    int currentIdx = 0;
                    for (int i = 0; i < listCol.Length; i++)
                    {
                        int count = offsets[i + 1] - offsets[i];
                        for (int j = 0; j < count; j++)
                        {
                            expansionIndices[currentIdx++] = i;
                        }
                    }

                    var newColumns = new List<ISeries>();
                    foreach (var c in df.Columns)
                    {
                        if (c.Name == columnName)
                        {
                            // Preserve the name of the exploded column
                            var explodedValues = listCol.Values;
                            // We might need to clone or rename here, but since it's a new DataFrame, 
                            // we can just rename it if it's not shared. 
                            // Polars behavior is to keep the original column name.
                            explodedValues.Rename(columnName);
                            newColumns.Add(explodedValues);
                        }
                        else
                        {
                            newColumns.Add(df.MaterializeColumn(c, expansionIndices, hasNulls: true));
                        }
                    }
                    yield return new DataFrame(newColumns);
                }
                else
                {
                    yield return df;
                }
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyUnnest(IAsyncEnumerable<DataFrame> source, string[] columnNames)
        {
            await foreach (var df in source)
            {
                var columnNamesSet = new HashSet<string>(columnNames);
                var newCols = new List<ISeries>();
                foreach (var c in df.Columns)
                {
                    if (columnNamesSet.Contains(c.Name) && c is Data.StructSeries structCol)
                    {
                        newCols.AddRange(structCol.Fields);
                    }
                    else
                    {
                        newCols.Add(c);
                    }
                }
                yield return new DataFrame(newCols);
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyScanSql(System.Data.IDbConnection connection, string sql)
        {
            if (connection.State != System.Data.ConnectionState.Open) connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = command.ExecuteReader();

            // Simplistic chunking for now: read all at once as a single chunk
            var df = DataFrame.FromSqlReader(reader);
            if (df.RowCount > 0) yield return df;
        }

        private async IAsyncEnumerable<DataFrame> ApplyTranspose(IAsyncEnumerable<DataFrame> source, bool include_header, string header_name, string[]? column_names)
        {
            var df = await CollectAll(source);
            yield return df.Transpose(include_header, header_name, column_names);
        }

        private async IAsyncEnumerable<DataFrame> ApplyUnique(IAsyncEnumerable<DataFrame> source)
        {
            var df = await CollectAll(source);
            yield return df.Unique();
        }

        private async IAsyncEnumerable<DataFrame> ApplySlice(IAsyncEnumerable<DataFrame> source, int offset, int length)
        {
            var df = await CollectAll(source);
            yield return df.Slice(offset, length);
        }

        private async IAsyncEnumerable<DataFrame> ApplyTail(IAsyncEnumerable<DataFrame> source, int n)
        {
            var df = await CollectAll(source);
            yield return df.Tail(n);
        }

        private async IAsyncEnumerable<DataFrame> ApplyDropNulls(IAsyncEnumerable<DataFrame> source, string[]? subset, bool anyNull)
        {
            var df = await CollectAll(source);
            yield return df.DropNulls(subset, anyNull);
        }

        private async IAsyncEnumerable<DataFrame> ApplyFillNan(IAsyncEnumerable<DataFrame> source, double value)
        {
            var df = await CollectAll(source);
            yield return df.FillNan(value);
        }

        private async IAsyncEnumerable<DataFrame> ApplyWithRowIndex(IAsyncEnumerable<DataFrame> source, string name)
        {
            var df = await CollectAll(source);
            yield return df.WithRowIndex(name);
        }

        private async IAsyncEnumerable<DataFrame> ApplyRename(IAsyncEnumerable<DataFrame> source, Dictionary<string, string> mapping)
        {
            var df = await CollectAll(source);
            yield return df.Rename(mapping);
        }

        private async IAsyncEnumerable<DataFrame> ApplyNullCount(IAsyncEnumerable<DataFrame> source)
        {
            var df = await CollectAll(source);
            yield return df.NullCount();
        }

        private async IAsyncEnumerable<DataFrame> ApplySinkCsv(IAsyncEnumerable<DataFrame> source, string filePath)
        {
            var df = await CollectAll(source);
            df.WriteCsv(filePath);
            yield break;
        }

        private async IAsyncEnumerable<DataFrame> ApplySinkParquet(IAsyncEnumerable<DataFrame> source, string filePath)
        {
            var df = await CollectAll(source);
            df.WriteParquet(filePath);
            yield break;
        }

        private async IAsyncEnumerable<DataFrame> ApplyShiftColumns(IAsyncEnumerable<DataFrame> source, int n)
        {
            await foreach (var df in source)
            {
                var newCols = new List<ISeries>();
                foreach (var col in df.Columns)
                {
                    var shifted = Compute.ArrayKernels.Shift(col, n);
                    shifted.Rename(col.Name);
                    newCols.Add(shifted);
                }
                yield return new DataFrame(newCols);
            }
        }

        private async Task<DataFrame> CollectAll(IAsyncEnumerable<DataFrame> source)
        {
            var results = new List<DataFrame>();
            await foreach (var df in source) results.Add(df);
            return DataFrame.Concat(results.ToArray());
        }

        private Memory<bool> CreateIsNullMask(ISeries series)
        {
            var mask = new bool[series.Length];
            for (int i = 0; i < series.Length; i++) mask[i] = series.ValidityMask.IsNull(i);
            return mask;
        }

        private static string[] ExtractStringArrayFromExpr(Expression expr)
        {
            if (expr is NewArrayExpression arrayExpr)
            {
                var result = new string[arrayExpr.Expressions.Count];
                for (int i = 0; i < arrayExpr.Expressions.Count; i++)
                {
                    var element = arrayExpr.Expressions[i];
                    // May be wrapped in a convert (boxed)
                    if (element is UnaryExpression ue && ue.NodeType == ExpressionType.Convert)
                        element = ue.Operand;
                    result[i] = (string)((ConstantExpression)element).Value!;
                }
                return result;
            }
            if (expr is ConstantExpression ce && ce.Value is string[] arr)
                return arr;
            throw new NotSupportedException($"Cannot extract string[] from expression: {expr}");
        }

        /// <summary>Copy nulls from the source ListSeries validity mask to the result.</summary>
        private static void PropagateListNulls(Data.ListSeries list, ISeries result)
        {
            for (int i = 0; i < list.Length; i++)
            {
                if (list.ValidityMask.IsNull(i))
                    result.ValidityMask.SetNull(i);
            }
        }
        private async IAsyncEnumerable<DataFrame> ApplySinkIpc(IAsyncEnumerable<DataFrame> source, string filePath)
        {
            var df = await CollectAll(source);
            df.WriteIpc(filePath);
            yield break;
        }
        private async IAsyncEnumerable<DataFrame> ApplyAggGroups(IAsyncEnumerable<DataFrame> source, string[] columns)
        {
            var df = await CollectAll(source);
            var gb = new GroupByBuilder(df, columns);
            yield return gb.AggGroups();
        }
        private async IAsyncEnumerable<DataFrame> ApplyClear(IAsyncEnumerable<DataFrame> source)
        {
            await foreach (var df in source)
            {
                yield return df.Clear();
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyShrinkToFit(IAsyncEnumerable<DataFrame> source)
        {
            await foreach (var df in source)
            {
                df.ShrinkToFit();
                yield return df;
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyRechunk(IAsyncEnumerable<DataFrame> source)
        {
            // Rechunk is a no-op for now since columns are single-chunk
            await foreach (var df in source)
            {
                yield return df;
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyMap(IAsyncEnumerable<DataFrame> source, Func<DataFrame, DataFrame> func)
        {
            await foreach (var df in source)
            {
                yield return func(df);
            }
        }    }

    /// <summary>
    /// Applies cost-based join reordering to minimize intermediate result sizes.
    /// Currently reorders left-associative join chains so that joins on the same key are
    /// grouped together, and smaller datasets are processed first.
    /// </summary>
    internal class JoinReorderingVisitor : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(LazyFrame.JoinOp))
            {
                // Visit children first
                var left = Visit(node.Arguments[0]);
                var right = Visit(node.Arguments[1]);
                var on = node.Arguments[2];
                var type = node.Arguments[3];

                var leftMethodCall = left as MethodCallExpression;

                // If left is itself a JoinOp, check if we can reorder for cheaper execution.
                // Simple heuristic: prefer to join smaller tables first (estimated by scan existence).
                if (leftMethodCall != null && leftMethodCall.Method.Name == nameof(LazyFrame.JoinOp)
                    && leftMethodCall.Arguments[2].ToString() == on.ToString())
                {
                    // Same join key: Join(Join(A, B, k), C, k) -> Join(A, Join(B, C, k), k)
                    // This can help the hash table be built from (B?C) instead of recomputing A?B.
                    var a = leftMethodCall.Arguments[0];
                    var b = leftMethodCall.Arguments[1];
                    var innerOn = leftMethodCall.Arguments[2];
                    var innerType = leftMethodCall.Arguments[3];

                    var innerJoin = Expression.Call(null, node.Method, b, right, innerOn, innerType);
                    var innerVisited = Visit(innerJoin);
                    return Expression.Call(null, node.Method, a, innerVisited, on, type);
                }

                // For Inner joins, if the left side is a computed expression and the right side is
                // a simple scan, swap the order so the scan is the build side (right).
                if ((JoinType)((ConstantExpression)type).Value! == JoinType.Inner)
                {
                    bool leftIsScan = left is MethodCallExpression lm && (
                        lm.Method.Name == nameof(LazyFrame.ScanCsvOp) ||
                        lm.Method.Name == nameof(LazyFrame.ScanParquetOp) ||
                        lm.Method.Name == nameof(LazyFrame.ScanJsonOp));
                    bool rightIsScan = right is MethodCallExpression rm && (
                        rm.Method.Name == nameof(LazyFrame.ScanCsvOp) ||
                        rm.Method.Name == nameof(LazyFrame.ScanParquetOp) ||
                        rm.Method.Name == nameof(LazyFrame.ScanJsonOp));

                    // If left is complex and right is a scan, swap to make the scan the build side
                    if (!leftIsScan && rightIsScan)
                    {
                        return Expression.Call(null, node.Method, right, left, on, type);
                    }
                }

                return Expression.Call(null, node.Method, left, right, on, type);
            }

            return base.VisitMethodCall(node);
        }
    }
}


