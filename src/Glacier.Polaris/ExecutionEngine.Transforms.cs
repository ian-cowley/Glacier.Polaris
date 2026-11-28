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

        private async IAsyncEnumerable<DataFrame> ApplyAgg(IAsyncEnumerable<DataFrame> source, string[] groupColumns, Expr[] aggExprs)
        {
            var chunks = new List<DataFrame>();
            await foreach (var df in source) chunks.Add(df);
            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);
            var groupCols = groupColumns.Select(fullDf.GetColumn).ToArray();
            var groups = Compute.GroupByKernels.GroupBy(groupCols);
            var keyCols = BuildGroupKeyColumns(fullDf, groupColumns, groups);

            yield return EvaluateGroupAggregations(fullDf, keyCols, groups, aggExprs);
        }

        private async IAsyncEnumerable<DataFrame> ApplyDynamicAgg(IAsyncEnumerable<DataFrame> source, GroupByDynamicPlanNode dyn, Expr[] aggExprs)
        {
            var chunks = new List<DataFrame>();
            await foreach (var df in source) chunks.Add(df);
            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);
            yield return EvaluateDynamicAggSync(fullDf, dyn, aggExprs);
        }

        private async IAsyncEnumerable<DataFrame> ApplyRollingAgg(IAsyncEnumerable<DataFrame> source, GroupByRollingPlanNode roll, Expr[] aggExprs)
        {
            var chunks = new List<DataFrame>();
            await foreach (var df in source) chunks.Add(df);
            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);
            yield return EvaluateRollingAggSync(fullDf, roll, aggExprs);
        }

        private DataFrame EvaluateDynamicAggSync(DataFrame fullDf, GroupByDynamicPlanNode dyn, Expr[] aggExprs)
        {
            var idxCol = fullDf.GetColumn(dyn.IndexColumn);
            double parsedEvery = Duration.Parse(dyn.Every, idxCol.DataType);
            double parsedPeriod = string.IsNullOrEmpty(dyn.Period) ? parsedEvery : Duration.Parse(dyn.Period, idxCol.DataType);
            double parsedOffset = string.IsNullOrEmpty(dyn.Offset) ? 0 : Duration.Parse(dyn.Offset, idxCol.DataType);

            var (groups, windowKeys) = Compute.GroupByKernels.GenerateDynamicGroups(
                idxCol, parsedEvery, parsedPeriod, parsedOffset, dyn.Closed, dyn.StartBy);
            return EvaluateGroupAggregations(fullDf, new[] { windowKeys }, groups, aggExprs);
        }

        private DataFrame EvaluateRollingAggSync(DataFrame fullDf, GroupByRollingPlanNode roll, Expr[] aggExprs)
        {
            var idxCol = fullDf.GetColumn(roll.IndexColumn);
            double parsedPeriod = Duration.Parse(roll.Period, idxCol.DataType);
            double parsedOffset = string.IsNullOrEmpty(roll.Offset) ? -parsedPeriod : Duration.Parse(roll.Offset, idxCol.DataType);

            var groups = Compute.GroupByKernels.GenerateRollingGroups(idxCol, parsedPeriod, parsedOffset, roll.Closed);
            return EvaluateGroupAggregations(fullDf, new[] { idxCol }, groups, aggExprs);
        }

        internal static DataFrame EvaluateGroupAggregations(
            DataFrame fullDf,
            string[] groupColumns,
            List<List<int>> groups,
            Expr[] aggExprs)
        {
            var keyCols = BuildGroupKeyColumns(fullDf, groupColumns, groups);
            return new ExecutionEngine().EvaluateGroupAggregations(fullDf, keyCols, groups, aggExprs);
        }

        internal static DataFrame EvaluateGroupAggregationsStatic(
            DataFrame fullDf,
            ISeries[] groupKeyCols,
            List<List<int>> groups,
            Expr[] aggExprs)
        {
            return new ExecutionEngine().EvaluateGroupAggregations(fullDf, groupKeyCols, groups, aggExprs);
        }

        private static ISeries[] BuildGroupKeyColumns(DataFrame fullDf, string[] groupColumns, List<List<int>> groups)
        {
            var groupKeyCols = new ISeries[groupColumns.Length];
            int[] firstIndices = new int[groups.Count];
            for (int gi = 0; gi < groups.Count; gi++) firstIndices[gi] = groups[gi][0];

            for (int c = 0; c < groupColumns.Length; c++)
            {
                string gc = groupColumns[c];
                var grpSeries = fullDf.GetColumn(gc);
                var keyCol = Compute.UniqueKernels.CreateSeriesForTake(grpSeries, firstIndices, gc);
                grpSeries.Take(keyCol, firstIndices);
                groupKeyCols[c] = keyCol;
            }
            return groupKeyCols;
        }

        private DataFrame EvaluateGroupAggregations(
            DataFrame fullDf,
            ISeries[] groupKeyCols,
            List<List<int>> groups,
            Expr[] aggExprs)
        {
            var outputCols = new List<ISeries>(groupKeyCols.Length + aggExprs.Length);
            outputCols.AddRange(groupKeyCols);

            var disposables = new List<IDisposable>();
            try
            {
                var resultColumns = new List<ISeries>();
                foreach (var aggExpr in aggExprs)
                {
                    var body = aggExpr.Expression;
                    string? alias = null;
                    Expression innerBody = body;
                    if (body is MethodCallExpression mce && mce.Method.Name == "AliasOp")
                    {
                        alias = (string)((ConstantExpression)mce.Arguments[1]).Value!;
                        innerBody = mce.Arguments[0];
                    }

                    bool isSimple = innerBody is MethodCallExpression innerMce
                        && innerMce.Arguments.Count == 1
                        && innerMce.Arguments[0] is MethodCallExpression colMce
                        && colMce.Method.Name == "Col";

                    if (isSimple)
                    {
                        var aggMce = (MethodCallExpression)innerBody;
                        string aggType = aggMce.Method.Name.Replace("Op", "").ToLower();
                        string colName = (string)((ConstantExpression)((MethodCallExpression)aggMce.Arguments[0]).Arguments[0]).Value!;

                        var series = fullDf.GetColumn(colName);
                        var result = Compute.GroupByKernels.Aggregate(series, groups, aggType);
                        var name = alias ?? $"{colName}_{aggType}";
                        result.Rename(name);
                        resultColumns.Add(result);
                    }
                    else
                    {
                        var colTracker = new ColumnTrackerVisitor();
                        colTracker.Visit(innerBody);
                        var usedCols = colTracker.Columns.ToArray();

                        ISeries? firstResult = null;
                        int resultIdx = 0;
                        foreach (var group in groups)
                        {
                            var subCols = new List<ISeries>();
                            foreach (var colName in usedCols)
                            {
                                var sourceCol = fullDf.GetColumn(colName);
                                int n = group.Count;
                                ISeries subCol;
                                if (sourceCol is Int32Series i32)
                                {
                                    subCol = new Int32Series(colName, n);
                                    for (int i = 0; i < n; i++) ((Int32Series)subCol).Memory.Span[i] = i32.Memory.Span[group[i]];
                                }
                                else if (sourceCol is Float64Series f64)
                                {
                                    subCol = new Float64Series(colName, n);
                                    for (int i = 0; i < n; i++) ((Float64Series)subCol).Memory.Span[i] = f64.Memory.Span[group[i]];
                                }
                                else if (sourceCol is Utf8StringSeries u8)
                                {
                                    int totalBytes = 0;
                                    for (int i = 0; i < n; i++) totalBytes += u8.GetStringSpan(group[i]).Length;
                                    subCol = new Utf8StringSeries(colName, n, totalBytes);
                                    int offset = 0;
                                    for (int i = 0; i < n; i++)
                                    {
                                        var span = u8.GetStringSpan(group[i]);
                                        span.CopyTo(((Utf8StringSeries)subCol).DataBytes.Span.Slice(offset));
                                        offset += span.Length;
                                        ((Utf8StringSeries)subCol).Offsets.Span[i + 1] = offset;
                                    }
                                }
                                else if (sourceCol is Int64Series i64)
                                {
                                    subCol = new Int64Series(colName, n);
                                    for (int i = 0; i < n; i++) ((Int64Series)subCol).Memory.Span[i] = i64.Memory.Span[group[i]];
                                }
                                else if (sourceCol is BooleanSeries bs)
                                {
                                    subCol = new BooleanSeries(colName, n);
                                    for (int i = 0; i < n; i++) ((BooleanSeries)subCol).Memory.Span[i] = bs.Memory.Span[group[i]];
                                }
                                else
                                {
                                    var groupSpan = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(group);
                                    subCol = Compute.UniqueKernels.CreateSeriesForTake(sourceCol, groupSpan, colName);
                                    sourceCol.Take(subCol, groupSpan);
                                }
                                for (int i = 0; i < n; i++)
                                {
                                    if (sourceCol.ValidityMask.IsNull(group[i]))
                                        subCol.ValidityMask.SetNull(i);
                                }
                                subCols.Add(subCol);
                            }
                            var subDf = new DataFrame(subCols);
                            var groupResult = EvaluateExpression(innerBody, subDf, disposables);

                            if (firstResult == null)
                            {
                                firstResult = groupResult;
                                if (groupResult is Int32Series ri32)
                                {
                                    var res = new Int32Series(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = ri32.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                                else if (groupResult is Float64Series rf64)
                                {
                                    var res = new Float64Series(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = rf64.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                                else if (groupResult is Int64Series ri64)
                                {
                                    var res = new Int64Series(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = ri64.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                                else if (groupResult is BooleanSeries rb)
                                {
                                    var res = new BooleanSeries(alias ?? "result", groups.Count);
                                    res.Memory.Span[0] = rb.Memory.Span[0];
                                    resultColumns.Add(res);
                                }
                            }
                            else
                            {
                                var lastResult = resultColumns[resultColumns.Count - 1];
                                if (lastResult is Int32Series ri32 && groupResult is Int32Series gi32)
                                    ri32.Memory.Span[resultIdx] = gi32.Memory.Span[0];
                                else if (lastResult is Float64Series rf64 && groupResult is Float64Series gf64)
                                    rf64.Memory.Span[resultIdx] = gf64.Memory.Span[0];
                                else if (lastResult is Int64Series ri64 && groupResult is Int64Series gi64)
                                    ri64.Memory.Span[resultIdx] = gi64.Memory.Span[0];
                                else if (lastResult is BooleanSeries rb && groupResult is BooleanSeries gb)
                                    rb.Memory.Span[resultIdx] = gb.Memory.Span[0];
                            }
                            resultIdx++;
                        }
                    }
                }

                outputCols.AddRange(resultColumns);
                return new DataFrame(outputCols);
            }
            finally
            {
                foreach (var d in disposables) d.Dispose();
            }
        }

        private static DataFrame SortDataFrame(DataFrame fullDf, string[] columnNames, bool[] descending)
        {
            if (fullDf.Columns.Count == 0 || fullDf.RowCount == 0)
                return fullDf.Clone();

            var indices = Compute.SortKernels.MultiColumnSort(fullDf, columnNames, descending);
            var sortedCols = new List<ISeries>(fullDf.Columns.Count);
            foreach (var col in fullDf.Columns)
            {
                var newCol = Compute.UniqueKernels.CreateSeriesForTake(col, indices);
                col.Take(newCol, indices);
                sortedCols.Add(newCol);
            }
            return new DataFrame(sortedCols);
        }

        private static DataFrame TopKDataFrame(DataFrame fullDf, string[] columnNames, bool[] descending, int k)
        {
            if (fullDf.Columns.Count == 0 || fullDf.RowCount == 0)
                return fullDf.Clone();

            var indices = Compute.SortKernels.TopK(fullDf, columnNames, descending, k);
            var sortedCols = new List<ISeries>(fullDf.Columns.Count);
            foreach (var col in fullDf.Columns)
            {
                var newCol = Compute.UniqueKernels.CreateSeriesForTake(col, indices);
                col.Take(newCol, indices);
                sortedCols.Add(newCol);
            }
            return new DataFrame(sortedCols);
        }

        private async IAsyncEnumerable<DataFrame> ApplyTopK(IAsyncEnumerable<DataFrame> source, string[] columnNames, bool[] descending, int k)
        {
            var chunks = new List<DataFrame>();
            await foreach (var df in source)
            {
                chunks.Add(df);
            }

            if (chunks.Count == 0) yield break;

            var fullDf = chunks.Count == 1 ? chunks[0] : DataFrame.Concat(chunks);
            yield return TopKDataFrame(fullDf, columnNames, descending, k);
        }

        private async IAsyncEnumerable<DataFrame> ApplySort(IAsyncEnumerable<DataFrame> source, string[] columnNames, bool[] descending)
        {
            await foreach (var df in Compute.ExternalMergeSort.SortAsync(source, columnNames, descending))
            {
                yield return df;
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyJoin(
            IAsyncEnumerable<DataFrame> leftSource,
            IAsyncEnumerable<DataFrame> rightSource,
            string[] leftOn,
            string[] rightOn,
            JoinType type)
        {
            var rightTask = Task.Run(async () =>
            {
                var rightResults = new List<DataFrame>();
                await foreach (var df in rightSource) rightResults.Add(df);
                return DataFrame.Concat(rightResults);
            });

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
                yield return leftDf.Join(rightDf, leftOn, rightOn, type);
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

        private static DataFrame LimitBatch(DataFrame df, int n)
        {
            if (n <= 0)
            {
                return new DataFrame(df.Columns.Select(col => col.CloneEmpty(0)).ToList());
            }
            if (df.RowCount <= n)
            {
                return df;
            }

            var slicedColumns = new List<ISeries>(df.Columns.Count);
            foreach (var col in df.Columns)
            {
                if (col is Int32Series i32)
                {
                    var newCol = new Int32Series(col.Name, n);
                    i32.Memory.Span.Slice(0, n).CopyTo(newCol.Memory.Span);
                    if (i32.ValidityMask.HasNulls)
                    {
                        for (int idx = 0; idx < n; idx++)
                            if (i32.ValidityMask.IsNull(idx)) newCol.ValidityMask.SetNull(idx);
                    }
                    slicedColumns.Add(newCol);
                }
                else if (col is Float64Series f64)
                {
                    var newCol = new Float64Series(col.Name, n);
                    f64.Memory.Span.Slice(0, n).CopyTo(newCol.Memory.Span);
                    if (f64.ValidityMask.HasNulls)
                    {
                        for (int idx = 0; idx < n; idx++)
                            if (f64.ValidityMask.IsNull(idx)) newCol.ValidityMask.SetNull(idx);
                    }
                    slicedColumns.Add(newCol);
                }
                else if (col is Utf8StringSeries utf8)
                {
                    int byteLen = utf8.Offsets.Span[n];
                    var newCol = new Utf8StringSeries(col.Name, n, byteLen);
                    utf8.DataBytes.Span.Slice(0, byteLen).CopyTo(newCol.DataBytes.Span);
                    utf8.Offsets.Span.Slice(0, n + 1).CopyTo(newCol.Offsets.Span);
                    if (utf8.ValidityMask.HasNulls)
                    {
                        for (int idx = 0; idx < n; idx++)
                            if (utf8.ValidityMask.IsNull(idx)) newCol.ValidityMask.SetNull(idx);
                    }
                    slicedColumns.Add(newCol);
                }
                else
                {
                    var indices = Enumerable.Range(0, n).ToArray();
                    var newCol = Compute.UniqueKernels.CreateSeriesForTake(col, indices);
                    col.Take(newCol, indices);
                    slicedColumns.Add(newCol);
                }
            }
            return new DataFrame(slicedColumns);
        }

        private async IAsyncEnumerable<DataFrame> ApplyLimit(IAsyncEnumerable<DataFrame> source, int n)
        {
            if (n <= 0)
            {
                await foreach (var df in source)
                {
                    yield return LimitBatch(df, 0);
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
                    int take = n - count;
                    yield return LimitBatch(df, take);
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

        private static DataFrame ExplodeBatch(DataFrame df, string columnName)
        {
            var col = df.GetColumn(columnName);
            if (col is not ListSeries listCol) return df;

            var offsets = listCol.Offsets.Memory.Span;
            int totalNewRows = listCol.Values.Length;
            if (totalNewRows == 0) return df.Clear();

            int[] expansionIndices = new int[totalNewRows];
            int currentIdx = 0;
            for (int i = 0; i < listCol.Length; i++)
            {
                int count = offsets[i + 1] - offsets[i];
                for (int j = 0; j < count; j++)
                    expansionIndices[currentIdx++] = i;
            }

            var newColumns = new List<ISeries>(df.Columns.Count);
            foreach (var c in df.Columns)
            {
                if (c.Name == columnName)
                {
                    var explodedValues = listCol.Values.CloneWithName(columnName);
                    newColumns.Add(explodedValues);
                }
                else
                {
                    newColumns.Add(df.MaterializeColumn(c, expansionIndices, hasNulls: true));
                }
            }
            return new DataFrame(newColumns);
        }

        private async IAsyncEnumerable<DataFrame> ApplyExplode(IAsyncEnumerable<DataFrame> source, string columnName)
        {
            await foreach (var df in source)
            {
                var col = df.GetColumn(columnName);
                if (col is ListSeries listCol && listCol.Values.Length == 0)
                {
                    continue;
                }
                yield return ExplodeBatch(df, columnName);
            }
        }

        private static DataFrame UnnestBatch(DataFrame df, string[] columnNames)
        {
            var columnNamesSet = new HashSet<string>(columnNames);
            var newCols = new List<ISeries>();
            foreach (var c in df.Columns)
            {
                if (columnNamesSet.Contains(c.Name) && c is StructSeries structCol)
                {
                    newCols.AddRange(structCol.Fields);
                }
                else
                {
                    newCols.Add(c);
                }
            }
            return new DataFrame(newCols);
        }

        private async IAsyncEnumerable<DataFrame> ApplyUnnest(IAsyncEnumerable<DataFrame> source, string[] columnNames)
        {
            await foreach (var df in source)
            {
                yield return UnnestBatch(df, columnNames);
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyScanSql(System.Data.IDbConnection connection, string sql)
        {
            if (connection.State != System.Data.ConnectionState.Open) connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = command.ExecuteReader();

            var df = DataFrame.FromSqlReader(reader);
            if (df.RowCount > 0) yield return df;
        }

        private async IAsyncEnumerable<DataFrame> ApplyTranspose(IAsyncEnumerable<DataFrame> source, bool include_header, string header_name, string[]? column_names)
        {
            var df = await CollectAll(source);
            yield return df.Transpose(include_header, header_name, column_names);
        }

        private async IAsyncEnumerable<DataFrame> ApplyUnique(IAsyncEnumerable<DataFrame> source, string[]? subset = null, bool maintainOrder = true)
        {
            var df = await CollectAll(source);
            yield return df.Unique(subset, maintainOrder);
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
            await IO.CsvWriter.WriteStreamAsync(source, filePath).ConfigureAwait(false);
            yield break;
        }

        private async IAsyncEnumerable<DataFrame> ApplySinkParquet(IAsyncEnumerable<DataFrame> source, string filePath)
        {
            await IO.ParquetWriter.WriteStreamAsync(source, filePath).ConfigureAwait(false);
            yield break;
        }

        private static DataFrame ShiftColumnsBatch(DataFrame df, int n)
        {
            var newCols = new List<ISeries>(df.Columns.Count);
            foreach (var col in df.Columns)
            {
                var shifted = Compute.ArrayKernels.Shift(col, n);
                shifted.Rename(col.Name);
                newCols.Add(shifted);
            }
            return new DataFrame(newCols);
        }

        private async IAsyncEnumerable<DataFrame> ApplyShiftColumns(IAsyncEnumerable<DataFrame> source, int n)
        {
            await foreach (var df in source)
            {
                yield return ShiftColumnsBatch(df, n);
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

        private static void PropagateListNulls(ListSeries list, ISeries result)
        {
            for (int i = 0; i < list.Length; i++)
            {
                if (list.ValidityMask.IsNull(i))
                    result.ValidityMask.SetNull(i);
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplySinkIpc(IAsyncEnumerable<DataFrame> source, string filePath)
        {
            await IO.IpcWriter.WriteStreamAsync(source, filePath).ConfigureAwait(false);
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
