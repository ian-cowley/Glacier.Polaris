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
            await foreach (var df in Compute.ExternalMergeSort.SortAsync(source, columnNames, descending))
            {
                yield return df;
            }
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
        }
    }
}
