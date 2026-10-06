using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Glacier.Polaris.Data;

namespace Glacier.Polaris
{
    public partial class ExecutionEngine
    {
        private readonly Dictionary<string, HashSet<string>>? _nestedFieldPaths;

        public ExecutionEngine(Dictionary<string, HashSet<string>>? nestedFieldPaths = null)
        {
            _nestedFieldPaths = nestedFieldPaths;
        }

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

    }
}
