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

        internal ExecutionEngine(Dictionary<string, HashSet<string>>? nestedFieldPaths = null)
        {
            _nestedFieldPaths = nestedFieldPaths;
        }

        public ExecutionEngine() : this(null)
        {
        }

        public IAsyncEnumerable<DataFrame> ExecuteAsync(Expression plan)
        {
            return ExecuteAsync(LogicalPlanNode.FromLegacyExpression(plan));
        }

        internal IAsyncEnumerable<DataFrame> ExecuteAsync(LogicalPlanNode plan)
        {
            return Evaluate(plan);
        }

        internal static DataFrame ExecuteSync(LogicalPlanNode plan)
        {
            var optimizer = new QueryOptimizer();
            var optimized = optimizer.Optimize(plan);
            var engine = new ExecutionEngine(optimizer.NestedFieldPaths);
            return engine.EvaluateSync(optimized);
        }

        private DataFrame EvaluateSync(LogicalPlanNode node)
        {
            switch (node)
            {
                case DataFramePlanNode dfNode:
                    return dfNode.DataFrame;

                case ScanSqlPlanNode sqlNode:
                {
                    var conn = sqlNode.Connection;
                    if (conn.State != System.Data.ConnectionState.Open) conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = sqlNode.Sql;
                    using var reader = cmd.ExecuteReader();
                    return DataFrame.FromSqlReader(reader);
                }

                case FilterPlanNode f:
                {
                    var df = EvaluateSync(f.Source);
                    return FilterBatch(df, f.Predicate) ?? new DataFrame(df.Columns.Select(c => c.CloneEmpty(0)).ToList());
                }

                case SelectPlanNode sel:
                    return SelectBatch(EvaluateSync(sel.Source), sel.Selections);

                case WithColumnsPlanNode wc:
                    return WithColumnsBatch(EvaluateSync(wc.Source), wc.Columns);

                case SortPlanNode sort:
                    return SortDataFrame(EvaluateSync(sort.Source), sort.ColumnNames, sort.Descending);

                case TopKPlanNode topK:
                    return TopKDataFrame(EvaluateSync(topK.Source), topK.ColumnNames, topK.Descending, topK.K);

                case LimitPlanNode lim:
                    return LimitBatch(EvaluateSync(lim.Source), lim.N);

                case FetchPlanNode fetch:
                    return LimitBatch(EvaluateSync(fetch.Source), fetch.N);

                case JoinPlanNode join:
                {
                    var leftDf = EvaluateSync(join.Left);
                    var rightDf = EvaluateSync(join.Right);
                    return leftDf.Join(rightDf, join.LeftOn, join.RightOn, join.JoinType);
                }

                case GroupByPlanNode gb:
                    return EvaluateSync(gb.Source);

                case AggPlanNode agg:
                {
                    if (agg.GroupColumns != null)
                    {
                        var df = EvaluateSync(agg.Source);
                        var groupCols = agg.GroupColumns.Select(df.GetColumn).ToArray();
                        var groups = Compute.GroupByKernels.GroupBy(groupCols);
                        var keyCols = BuildGroupKeyColumns(df, agg.GroupColumns, groups);
                        return EvaluateGroupAggregations(df, keyCols, groups, agg.Aggregations);
                    }
                    if (agg.Source is GroupByPlanNode gbSource)
                    {
                        var df = EvaluateSync(gbSource.Source);
                        var groupCols = gbSource.Columns.Select(df.GetColumn).ToArray();
                        var groups = Compute.GroupByKernels.GroupBy(groupCols);
                        var keyCols = BuildGroupKeyColumns(df, gbSource.Columns, groups);
                        return EvaluateGroupAggregations(df, keyCols, groups, agg.Aggregations);
                    }
                    if (agg.Source is GroupByDynamicPlanNode dyn)
                    {
                        var df = EvaluateSync(dyn.Source);
                        return EvaluateDynamicAggSync(df, dyn, agg.Aggregations);
                    }
                    if (agg.Source is GroupByRollingPlanNode roll)
                    {
                        var df = EvaluateSync(roll.Source);
                        return EvaluateRollingAggSync(df, roll, agg.Aggregations);
                    }
                    throw new NotSupportedException("AggOp must follow GroupByOp.");
                }

                case AggGroupsPlanNode agp:
                    return new GroupByBuilder(EvaluateSync(agp.Source), agp.Columns).AggGroups();

                case ExplodePlanNode ex:
                    return ExplodeBatch(EvaluateSync(ex.Source), ex.Column);

                case UnnestPlanNode un:
                    return UnnestBatch(EvaluateSync(un.Source), un.Columns);

                case UnpivotPlanNode up:
                    return EvaluateSync(up.Source).Melt(up.On, up.Index, "variable", "value");

                case PivotPlanNode pv:
                    return EvaluateSync(pv.Source).Pivot(pv.Index, pv.Pivot[0], pv.Values[0], pv.Aggregate);

                case TransposePlanNode tr:
                    return EvaluateSync(tr.Source).Transpose(tr.IncludeHeader, tr.HeaderName, tr.ColumnNames);

                case UniquePlanNode uq:
                    return EvaluateSync(uq.Source).Unique(uq.Subset, uq.MaintainOrder);

                case SlicePlanNode sl:
                    return EvaluateSync(sl.Source).Slice(sl.Offset, sl.Length);

                case TailPlanNode tl:
                    return EvaluateSync(tl.Source).Tail(tl.N);

                case DropNullsPlanNode dn:
                    return EvaluateSync(dn.Source).DropNulls(dn.Subset, dn.AnyNull);

                case FillNanPlanNode fn:
                    return EvaluateSync(fn.Source).FillNan(fn.Value);

                case WithRowIndexPlanNode wri:
                    return EvaluateSync(wri.Source).WithRowIndex(wri.Name);

                case RenamePlanNode rn:
                    return EvaluateSync(rn.Source).Rename(rn.Mapping);

                case NullCountPlanNode nc:
                    return EvaluateSync(nc.Source).NullCount();

                case ShiftColumnsPlanNode sc:
                    return ShiftColumnsBatch(EvaluateSync(sc.Source), sc.N);

                case ClearPlanNode cl:
                    return EvaluateSync(cl.Source).Clear();

                case ShrinkToFitPlanNode sf:
                    return EvaluateSync(sf.Source).ShrinkToFit();

                case RechunkPlanNode rc:
                    return EvaluateSync(rc.Source);

                case MapPlanNode mp:
                    return mp.Func(EvaluateSync(mp.Source));

                default:
                    return Task.Run(async () =>
                    {
                        var chunks = new List<DataFrame>();
                        await foreach (var c in Evaluate(node).ConfigureAwait(false))
                        {
                            if (c.RowCount > 0 || c.Columns.Count > 0) chunks.Add(c);
                        }
                        if (chunks.Count == 0) return new DataFrame();
                        if (chunks.Count == 1) return chunks[0];
                        return DataFrame.Concat(chunks);
                    }).GetAwaiter().GetResult();
            }
        }

        private IAsyncEnumerable<DataFrame> Evaluate(LogicalPlanNode node) => node switch
        {
            ScanCsvPlanNode csv => new IO.CsvReader(csv.FilePath, csv.Columns, csv.NRows).ReadAsync(),
            ScanParquetPlanNode pq => new IO.ParquetReader(pq.FilePath, pq.Columns).ReadAsync(),
            ScanJsonPlanNode json => new IO.JsonReader(json.FilePath, 10000, json.Columns).ReadNdJsonAsync(),
            ScanSqlPlanNode sql => ApplyScanSql(sql.Connection, sql.Sql),
            DataFramePlanNode df => ToAsyncEnumerable(df.DataFrame),
            LimitPlanNode lim => ApplyLimit(Evaluate(lim.Source), lim.N),
            FetchPlanNode fetch => ApplyLimit(Evaluate(fetch.Source), fetch.N),
            FilterPlanNode filter => ApplyFilter(Evaluate(filter.Source), filter.Predicate),
            SelectPlanNode sel => ApplySelect(Evaluate(sel.Source), sel.Selections),
            WithColumnsPlanNode wc => ApplyWithColumns(Evaluate(wc.Source), wc.Columns),
            JoinPlanNode join => ApplyJoin(Evaluate(join.Left), Evaluate(join.Right), join.LeftOn, join.RightOn, join.JoinType),
            ExplodePlanNode ex => ApplyExplode(Evaluate(ex.Source), ex.Column),
            UnnestPlanNode un => ApplyUnnest(Evaluate(un.Source), un.Columns),
            TopKPlanNode tk => ApplyTopK(Evaluate(tk.Source), tk.ColumnNames, tk.Descending, tk.K),
            SortPlanNode st => ApplySort(Evaluate(st.Source), st.ColumnNames, st.Descending),
            DelayPlanNode dl => ApplyDelay(Evaluate(dl.Source), dl.Ms),
            UnpivotPlanNode up => ApplyUnpivot(Evaluate(up.Source), up.On, up.Index, "variable", "value"),
            PivotPlanNode pv => ApplyPivot(Evaluate(pv.Source), pv.Index, pv.Pivot[0], pv.Values[0], pv.Aggregate),
            TransposePlanNode tr => ApplyTranspose(Evaluate(tr.Source), tr.IncludeHeader, tr.HeaderName, tr.ColumnNames),
            UniquePlanNode uq => ApplyUnique(Evaluate(uq.Source), uq.Subset, uq.MaintainOrder),
            SlicePlanNode sl => ApplySlice(Evaluate(sl.Source), sl.Offset, sl.Length),
            TailPlanNode tl => ApplyTail(Evaluate(tl.Source), tl.N),
            DropNullsPlanNode dn => ApplyDropNulls(Evaluate(dn.Source), dn.Subset, dn.AnyNull),
            FillNanPlanNode fn => ApplyFillNan(Evaluate(fn.Source), fn.Value),
            WithRowIndexPlanNode wri => ApplyWithRowIndex(Evaluate(wri.Source), wri.Name),
            RenamePlanNode rn => ApplyRename(Evaluate(rn.Source), rn.Mapping),
            NullCountPlanNode nc => ApplyNullCount(Evaluate(nc.Source)),
            SinkCsvPlanNode scsv => ApplySinkCsv(Evaluate(scsv.Source), scsv.FilePath),
            SinkParquetPlanNode spq => ApplySinkParquet(Evaluate(spq.Source), spq.FilePath),
            SinkIpcPlanNode sipc => ApplySinkIpc(Evaluate(sipc.Source), sipc.FilePath),
            AggGroupsPlanNode agp => ApplyAggGroups(Evaluate(agp.Source), agp.Columns),
            ClearPlanNode cl => ApplyClear(Evaluate(cl.Source)),
            ShrinkToFitPlanNode sf => ApplyShrinkToFit(Evaluate(sf.Source)),
            RechunkPlanNode rc => ApplyRechunk(Evaluate(rc.Source)),
            MapPlanNode mp => ApplyMap(Evaluate(mp.Source), mp.Func),
            ShiftColumnsPlanNode sc => ApplyShiftColumns(Evaluate(sc.Source), sc.N),
            GroupByPlanNode gb => Evaluate(gb.Source),
            GroupByDynamicPlanNode gbd => Evaluate(gbd.Source),
            GroupByRollingPlanNode gbr => Evaluate(gbr.Source),
            AggPlanNode agg => EvaluateAggNode(agg),
            _ => throw new NotSupportedException($"Execution engine cannot evaluate node: {node}")
        };

        private IAsyncEnumerable<DataFrame> EvaluateAggNode(AggPlanNode agg)
        {
            if (agg.GroupColumns != null)
            {
                return ApplyAgg(Evaluate(agg.Source), agg.GroupColumns, agg.Aggregations);
            }
            if (agg.Source is GroupByPlanNode gb)
            {
                return ApplyAgg(Evaluate(gb.Source), gb.Columns, agg.Aggregations);
            }
            if (agg.Source is GroupByDynamicPlanNode dyn)
            {
                return ApplyDynamicAgg(Evaluate(dyn.Source), dyn, agg.Aggregations);
            }
            if (agg.Source is GroupByRollingPlanNode roll)
            {
                return ApplyRollingAgg(Evaluate(roll.Source), roll, agg.Aggregations);
            }
            throw new NotSupportedException("AggOp must follow GroupByOp.");
        }

        private DataFrame? FilterBatch(DataFrame df, Expr predicateExpr)
        {
            var disposables = new List<IDisposable>();
            try
            {
                var result = EvaluateExpression(predicateExpr.Expression, df, disposables);
                if (result is not BooleanSeries mask)
                    throw new InvalidOperationException("Filter expression must evaluate to a Boolean series.");

                var maskSpan = mask.Memory.Span;
                var maskValidity = mask.ValidityMask;
                int matchCount = 0;
                for (int i = 0; i < mask.Length; i++)
                {
                    if (maskValidity.IsValid(i) && maskSpan[i]) matchCount++;
                }

                if (matchCount == 0)
                {
                    return null;
                }

                int[] indices = System.Buffers.ArrayPool<int>.Shared.Rent(matchCount);
                try
                {
                    int idx = 0;
                    for (int i = 0; i < mask.Length; i++)
                    {
                        if (maskValidity.IsValid(i) && maskSpan[i]) indices[idx++] = i;
                    }

                    var spanIndices = new ReadOnlySpan<int>(indices, 0, matchCount);
                    var newSeries = new List<ISeries>(df.Columns.Count);
                    foreach (var col in df.Columns)
                    {
                        var newCol = Compute.UniqueKernels.CreateSeriesForTake(col, spanIndices);
                        col.Take(newCol, spanIndices);
                        newSeries.Add(newCol);
                    }
                    return new DataFrame(newSeries);
                }
                finally
                {
                    System.Buffers.ArrayPool<int>.Shared.Return(indices);
                }
            }
            finally
            {
                foreach (var d in disposables) d.Dispose();
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyFilter(IAsyncEnumerable<DataFrame> source, Expr predicateExpr)
        {
            bool yieldedAny = false;
            List<ISeries>? emptySchema = null;

            await foreach (var df in source)
            {
                var filtered = FilterBatch(df, predicateExpr);
                if (filtered == null)
                {
                    emptySchema ??= df.Columns.Select(c => c.CloneEmpty(0)).ToList();
                    continue;
                }

                yieldedAny = true;
                yield return filtered;
            }

            if (!yieldedAny && emptySchema != null)
            {
                yield return new DataFrame(emptySchema);
            }
        }

        private DataFrame SelectBatch(DataFrame df, Expr[] selExprs)
        {
            var newColumns = new List<ISeries>(selExprs.Length);
            var disposables = new List<IDisposable>();

            try
            {
                foreach (var selection in selExprs)
                {
                    var body = selection.Expression;
                    string? alias = null;
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

                    if (resultSeries is IDisposable d && disposables.Contains(d))
                    {
                        disposables.Remove(d);
                    }

                    newColumns.Add(resultSeries);
                }
                return new DataFrame(newColumns);
            }
            finally
            {
                foreach (var d in disposables) d.Dispose();
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplySelect(IAsyncEnumerable<DataFrame> source, Expr[] selExprs)
        {
            await foreach (var df in source)
            {
                yield return SelectBatch(df, selExprs);
            }
        }

        private DataFrame WithColumnsBatch(DataFrame df, Expr[] withExprs)
        {
            var newColumns = df.Columns.ToList();
            var disposables = new List<IDisposable>();

            try
            {
                foreach (var selection in withExprs)
                {
                    var body = selection.Expression;
                    string? alias = null;
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

                    int existingIndex = newColumns.FindIndex(c => c.Name == alias);
                    if (existingIndex >= 0) newColumns[existingIndex] = resultSeries;
                    else newColumns.Add(resultSeries);
                }
                return new DataFrame(newColumns);
            }
            finally
            {
                foreach (var d in disposables) d.Dispose();
            }
        }

        private async IAsyncEnumerable<DataFrame> ApplyWithColumns(IAsyncEnumerable<DataFrame> source, Expr[] withExprs)
        {
            await foreach (var df in source)
            {
                yield return WithColumnsBatch(df, withExprs);
            }
        }
    }
}
