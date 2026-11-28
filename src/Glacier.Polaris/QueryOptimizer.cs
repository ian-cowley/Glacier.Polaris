using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Glacier.Polaris.Data;

namespace Glacier.Polaris
{
    public class QueryOptimizer : ExpressionVisitor
    {
        public Dictionary<string, HashSet<string>> NestedFieldPaths { get; private set; } = new(StringComparer.OrdinalIgnoreCase);

        public Expression Optimize(Expression plan)
        {
            return Optimize(LogicalPlanNode.FromLegacyExpression(plan)).ToLegacyExpression();
        }

        internal LogicalPlanNode Optimize(LogicalPlanNode plan)
        {
            // 1. Analyze: Find all columns used in the entire query
            var tracker = new LogicalPlanColumnTracker();
            tracker.Visit(plan);
            NestedFieldPaths = tracker.NestedFieldPaths;

            // 2. Transform: Sound Predicate & Limit Pushdown
            var transformed = OptimizeNode(plan);

            // 3. Join Reordering: Sound inner-join associative reordering
            transformed = ReorderJoins(transformed);

            // 4. Inject: Projection Pushdown (tell scanners what columns to load)
            if (tracker.HasGlobalSelect && !tracker.RequiresAllColumns && tracker.Columns.Count > 0)
            {
                transformed = PushdownProjection(transformed, tracker.Columns);
            }

            return transformed;
        }

        private LogicalPlanNode OptimizeNode(LogicalPlanNode node)
        {
            switch (node)
            {
                case DataFramePlanNode or ScanCsvPlanNode or ScanParquetPlanNode or ScanJsonPlanNode or ScanSqlPlanNode or ExternalExpressionPlanNode:
                    return node;

                case FilterPlanNode filter:
                {
                    var source = OptimizeNode(filter.Source);

                    if (source is SelectPlanNode selectNode)
                    {
                        if (TryPushFilterThroughSelect(filter, selectNode, out var rewrittenSelect))
                        {
                            return rewrittenSelect;
                        }
                    }
                    else if (source is JoinPlanNode joinNode)
                    {
                        if (TryPushFilterThroughJoin(filter, joinNode, out var rewrittenJoin))
                        {
                            return rewrittenJoin;
                        }
                    }
                    else if (source is GroupByPlanNode gbNode)
                    {
                        if (OptimizerAnalysis.IsPureElementwise(filter.Predicate.Expression))
                        {
                            var predCols = new HashSet<string>(StringComparer.Ordinal);
                            OptimizerAnalysis.CollectReferencedColumns(filter.Predicate.Expression, predCols);

                            if (predCols.Count > 0 && predCols.All(c => gbNode.Columns.Contains(c, StringComparer.Ordinal)))
                            {
                                var pushedFilter = filter with { Source = gbNode.Source };
                                return gbNode with { Source = OptimizeNode(pushedFilter) };
                            }
                        }
                    }

                    return ReferenceEquals(source, filter.Source) ? filter : filter with { Source = source };
                }

                case LimitPlanNode limit:
                {
                    var source = OptimizeNode(limit.Source);
                    int n = limit.N;

                    if (source is ScanCsvPlanNode scanCsv)
                    {
                        int effectiveLimit = scanCsv.NRows.HasValue ? Math.Min(scanCsv.NRows.Value, n) : n;
                        return scanCsv with { NRows = effectiveLimit };
                    }
                    if (source is SortPlanNode sortNode)
                    {
                        return new TopKPlanNode(sortNode.Source, sortNode.ColumnNames, sortNode.Descending, n);
                    }
                    if (source is SelectPlanNode selectNode)
                    {
                        if (selectNode.Selections.Length > 0 &&
                            selectNode.Selections.All(se => OptimizerAnalysis.IsPureElementwise(se.Expression)))
                        {
                            var newLimit = new LimitPlanNode(selectNode.Source, n);
                            return selectNode with { Source = OptimizeNode(newLimit) };
                        }
                    }

                    return ReferenceEquals(source, limit.Source) ? limit : limit with { Source = source };
                }

                case SelectPlanNode sel:
                {
                    var src = OptimizeNode(sel.Source);
                    return ReferenceEquals(src, sel.Source) ? sel : sel with { Source = src };
                }
                case WithColumnsPlanNode wc:
                {
                    var src = OptimizeNode(wc.Source);
                    return ReferenceEquals(src, wc.Source) ? wc : wc with { Source = src };
                }
                case GroupByPlanNode gb:
                {
                    var src = OptimizeNode(gb.Source);
                    return ReferenceEquals(src, gb.Source) ? gb : gb with { Source = src };
                }
                case AggPlanNode agg:
                {
                    var src = OptimizeNode(agg.Source);
                    return ReferenceEquals(src, agg.Source) ? agg : agg with { Source = src };
                }
                case AggGroupsPlanNode ag:
                {
                    var src = OptimizeNode(ag.Source);
                    return ReferenceEquals(src, ag.Source) ? ag : ag with { Source = src };
                }
                case GroupByDynamicPlanNode gbd:
                {
                    var src = OptimizeNode(gbd.Source);
                    return ReferenceEquals(src, gbd.Source) ? gbd : gbd with { Source = src };
                }
                case GroupByRollingPlanNode gbr:
                {
                    var src = OptimizeNode(gbr.Source);
                    return ReferenceEquals(src, gbr.Source) ? gbr : gbr with { Source = src };
                }
                case JoinPlanNode j:
                {
                    var left = OptimizeNode(j.Left);
                    var right = OptimizeNode(j.Right);
                    return ReferenceEquals(left, j.Left) && ReferenceEquals(right, j.Right)
                        ? j
                        : j with { Left = left, Right = right };
                }
                case SortPlanNode s:
                {
                    var src = OptimizeNode(s.Source);
                    return ReferenceEquals(src, s.Source) ? s : s with { Source = src };
                }
                case TopKPlanNode tk:
                {
                    var src = OptimizeNode(tk.Source);
                    return ReferenceEquals(src, tk.Source) ? tk : tk with { Source = src };
                }
                case FetchPlanNode f:
                {
                    var src = OptimizeNode(f.Source);
                    return ReferenceEquals(src, f.Source) ? f : f with { Source = src };
                }
                case DelayPlanNode d:
                {
                    var src = OptimizeNode(d.Source);
                    return ReferenceEquals(src, d.Source) ? d : d with { Source = src };
                }
                case ExplodePlanNode ex:
                {
                    var src = OptimizeNode(ex.Source);
                    return ReferenceEquals(src, ex.Source) ? ex : ex with { Source = src };
                }
                case UnnestPlanNode un:
                {
                    var src = OptimizeNode(un.Source);
                    return ReferenceEquals(src, un.Source) ? un : un with { Source = src };
                }
                case UnpivotPlanNode up:
                {
                    var src = OptimizeNode(up.Source);
                    return ReferenceEquals(src, up.Source) ? up : up with { Source = src };
                }
                case PivotPlanNode pv:
                {
                    var src = OptimizeNode(pv.Source);
                    return ReferenceEquals(src, pv.Source) ? pv : pv with { Source = src };
                }
                case TransposePlanNode tr:
                {
                    var src = OptimizeNode(tr.Source);
                    return ReferenceEquals(src, tr.Source) ? tr : tr with { Source = src };
                }
                case UniquePlanNode uq:
                {
                    var src = OptimizeNode(uq.Source);
                    return ReferenceEquals(src, uq.Source) ? uq : uq with { Source = src };
                }
                case SlicePlanNode sl:
                {
                    var src = OptimizeNode(sl.Source);
                    return ReferenceEquals(src, sl.Source) ? sl : sl with { Source = src };
                }
                case TailPlanNode tl:
                {
                    var src = OptimizeNode(tl.Source);
                    return ReferenceEquals(src, tl.Source) ? tl : tl with { Source = src };
                }
                case DropNullsPlanNode dn:
                {
                    var src = OptimizeNode(dn.Source);
                    return ReferenceEquals(src, dn.Source) ? dn : dn with { Source = src };
                }
                case FillNanPlanNode fn:
                {
                    var src = OptimizeNode(fn.Source);
                    return ReferenceEquals(src, fn.Source) ? fn : fn with { Source = src };
                }
                case WithRowIndexPlanNode wri:
                {
                    var src = OptimizeNode(wri.Source);
                    return ReferenceEquals(src, wri.Source) ? wri : wri with { Source = src };
                }
                case RenamePlanNode rn:
                {
                    var src = OptimizeNode(rn.Source);
                    return ReferenceEquals(src, rn.Source) ? rn : rn with { Source = src };
                }
                case NullCountPlanNode nc:
                {
                    var src = OptimizeNode(nc.Source);
                    return ReferenceEquals(src, nc.Source) ? nc : nc with { Source = src };
                }
                case ShiftColumnsPlanNode sc:
                {
                    var src = OptimizeNode(sc.Source);
                    return ReferenceEquals(src, sc.Source) ? sc : sc with { Source = src };
                }
                case ClearPlanNode cl:
                {
                    var src = OptimizeNode(cl.Source);
                    return ReferenceEquals(src, cl.Source) ? cl : cl with { Source = src };
                }
                case ShrinkToFitPlanNode sf:
                {
                    var src = OptimizeNode(sf.Source);
                    return ReferenceEquals(src, sf.Source) ? sf : sf with { Source = src };
                }
                case RechunkPlanNode rc:
                {
                    var src = OptimizeNode(rc.Source);
                    return ReferenceEquals(src, rc.Source) ? rc : rc with { Source = src };
                }
                case MapPlanNode mp:
                {
                    var src = OptimizeNode(mp.Source);
                    return ReferenceEquals(src, mp.Source) ? mp : mp with { Source = src };
                }
                case SinkCsvPlanNode scsv:
                {
                    var src = OptimizeNode(scsv.Source);
                    return ReferenceEquals(src, scsv.Source) ? scsv : scsv with { Source = src };
                }
                case SinkParquetPlanNode spq:
                {
                    var src = OptimizeNode(spq.Source);
                    return ReferenceEquals(src, spq.Source) ? spq : spq with { Source = src };
                }
                case SinkIpcPlanNode sipc:
                {
                    var src = OptimizeNode(sipc.Source);
                    return ReferenceEquals(src, sipc.Source) ? sipc : sipc with { Source = src };
                }
                default:
                    return node;
            }
        }

        private bool TryPushFilterThroughSelect(
            FilterPlanNode filterNode,
            SelectPlanNode selectNode,
            out LogicalPlanNode result)
        {
            result = null!;

            var selectExprs = selectNode.Selections;
            if (selectExprs.Length == 0)
                return false;

            for (int i = 0; i < selectExprs.Length; i++)
            {
                if (!OptimizerAnalysis.IsPureElementwise(selectExprs[i].Expression))
                    return false;
            }

            if (!OptimizerAnalysis.IsPureElementwise(filterNode.Predicate.Expression))
                return false;

            var passThroughMap = new Dictionary<string, string>(StringComparer.Ordinal);
            var seenOutputNames = new HashSet<string>(StringComparer.Ordinal);

            for (int i = 0; i < selectExprs.Length; i++)
            {
                var body = selectExprs[i].Expression;
                if (body is MethodCallExpression colCall && colCall.Method.Name == "Col"
                    && colCall.Arguments[0] is ConstantExpression colConst && colConst.Value is string colName)
                {
                    if (!seenOutputNames.Add(colName))
                        passThroughMap.Remove(colName);
                    else
                        passThroughMap[colName] = colName;
                }
                else if (body is MethodCallExpression aliasCall && aliasCall.Method.Name == "AliasOp"
                    && aliasCall.Arguments[1] is ConstantExpression aliasConst && aliasConst.Value is string aliasName)
                {
                    var inner = aliasCall.Arguments[0];
                    if (inner is MethodCallExpression innerCol && innerCol.Method.Name == "Col"
                        && innerCol.Arguments[0] is ConstantExpression innerColConst && innerColConst.Value is string srcColName)
                    {
                        if (!seenOutputNames.Add(aliasName))
                            passThroughMap.Remove(aliasName);
                        else
                            passThroughMap[aliasName] = srcColName;
                    }
                    else
                    {
                        seenOutputNames.Add(aliasName);
                        passThroughMap.Remove(aliasName);
                    }
                }
                else
                {
                    if (OptimizerAnalysis.TryInferExprOutputName(body, out string? inferredName) && inferredName != null)
                    {
                        seenOutputNames.Add(inferredName);
                        passThroughMap.Remove(inferredName);
                    }
                    else
                    {
                        return false;
                    }
                }
            }

            var predCols = new HashSet<string>(StringComparer.Ordinal);
            OptimizerAnalysis.CollectReferencedColumns(filterNode.Predicate.Expression, predCols);
            if (predCols.Count == 0)
                return false;

            foreach (var col in predCols)
            {
                if (!passThroughMap.ContainsKey(col))
                    return false;
            }

            Expr rewrittenPredicate = filterNode.Predicate;
            Expression<Func<Expr, Expr>>? rewrittenRawLambda = filterNode.RawLambda;
            if (predCols.Any(c => !string.Equals(c, passThroughMap[c], StringComparison.Ordinal)))
            {
                var rewrittenBody = OptimizerAnalysis.RewriteColumns(filterNode.Predicate.Expression, passThroughMap);
                rewrittenPredicate = new Expr(rewrittenBody);
                rewrittenRawLambda = null;
            }

            var newFilter = new FilterPlanNode(selectNode.Source, rewrittenPredicate, rewrittenRawLambda);
            result = selectNode with { Source = OptimizeNode(newFilter) };
            return true;
        }

        private bool TryPushFilterThroughJoin(
            FilterPlanNode filterNode,
            JoinPlanNode joinNode,
            out LogicalPlanNode result)
        {
            result = null!;

            if (!OptimizerAnalysis.IsPureElementwise(filterNode.Predicate.Expression))
                return false;

            var predCols = new HashSet<string>(StringComparer.Ordinal);
            OptimizerAnalysis.CollectReferencedColumns(filterNode.Predicate.Expression, predCols);
            if (predCols.Count == 0)
                return false;

            if (!PlanSchemaResolver.TryResolveColumns(joinNode.Left, out var leftCols) ||
                !PlanSchemaResolver.TryResolveColumns(joinNode.Right, out var rightCols))
            {
                return false;
            }

            var leftSet = new HashSet<string>(leftCols, StringComparer.Ordinal);
            var rightSet = new HashSet<string>(rightCols, StringComparer.Ordinal);
            var type = joinNode.JoinType;

            if ((type == JoinType.Inner || type == JoinType.Left || type == JoinType.Semi || type == JoinType.Anti)
                && predCols.All(c => leftSet.Contains(c)))
            {
                var newLeft = new FilterPlanNode(joinNode.Left, filterNode.Predicate, filterNode.RawLambda);
                result = joinNode with { Left = OptimizeNode(newLeft) };
                return true;
            }

            if (type == JoinType.Inner
                && predCols.All(c => (IsSharedSameNameKey(joinNode.LeftOn, joinNode.RightOn, c) && rightSet.Contains(c))
                                     || (rightSet.Contains(c) && !leftSet.Contains(c))))
            {
                var newRight = new FilterPlanNode(joinNode.Right, filterNode.Predicate, filterNode.RawLambda);
                result = joinNode with { Right = OptimizeNode(newRight) };
                return true;
            }

            return false;

            static bool IsSharedSameNameKey(string[] leftOn, string[] rightOn, string col)
            {
                for (int i = 0; i < Math.Min(leftOn.Length, rightOn.Length); i++)
                {
                    if (string.Equals(leftOn[i], col, StringComparison.Ordinal) &&
                        string.Equals(rightOn[i], col, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        private static LogicalPlanNode ReorderJoins(LogicalPlanNode node)
        {
            switch (node)
            {
                case JoinPlanNode joinNode:
                {
                    var left = ReorderJoins(joinNode.Left);
                    var right = ReorderJoins(joinNode.Right);

                    if (joinNode.JoinType == JoinType.Inner
                        && left is JoinPlanNode leftJoin
                        && leftJoin.JoinType == JoinType.Inner
                        && joinNode.LeftOn.Length == 1 && joinNode.RightOn.Length == 1
                        && leftJoin.LeftOn.Length == 1 && leftJoin.RightOn.Length == 1
                        && string.Equals(joinNode.LeftOn[0], joinNode.RightOn[0], StringComparison.Ordinal)
                        && string.Equals(leftJoin.LeftOn[0], leftJoin.RightOn[0], StringComparison.Ordinal)
                        && string.Equals(joinNode.LeftOn[0], leftJoin.LeftOn[0], StringComparison.Ordinal))
                    {
                        var candidateInner = new JoinPlanNode(leftJoin.Right, right, leftJoin.LeftOn, leftJoin.RightOn, JoinType.Inner);
                        var candidateInnerVisited = ReorderJoins(candidateInner);
                        var candidatePlan = new JoinPlanNode(leftJoin.Left, candidateInnerVisited, joinNode.LeftOn, joinNode.RightOn, JoinType.Inner);
                        var originalPlan = new JoinPlanNode(left, right, joinNode.LeftOn, joinNode.RightOn, JoinType.Inner);

                        if (SchemasMatchOrUnresolved(originalPlan, candidatePlan))
                        {
                            return candidatePlan;
                        }
                    }

                    return ReferenceEquals(left, joinNode.Left) && ReferenceEquals(right, joinNode.Right)
                        ? joinNode
                        : joinNode with { Left = left, Right = right };
                }

                case FilterPlanNode f: return f with { Source = ReorderJoins(f.Source) };
                case SelectPlanNode s: return s with { Source = ReorderJoins(s.Source) };
                case WithColumnsPlanNode wc: return wc with { Source = ReorderJoins(wc.Source) };
                case GroupByPlanNode gb: return gb with { Source = ReorderJoins(gb.Source) };
                case AggPlanNode ag: return ag with { Source = ReorderJoins(ag.Source) };
                case AggGroupsPlanNode agg: return agg with { Source = ReorderJoins(agg.Source) };
                case GroupByDynamicPlanNode gbd: return gbd with { Source = ReorderJoins(gbd.Source) };
                case GroupByRollingPlanNode gbr: return gbr with { Source = ReorderJoins(gbr.Source) };
                case SortPlanNode st: return st with { Source = ReorderJoins(st.Source) };
                case TopKPlanNode tk: return tk with { Source = ReorderJoins(tk.Source) };
                case LimitPlanNode lm: return lm with { Source = ReorderJoins(lm.Source) };
                case FetchPlanNode ft: return ft with { Source = ReorderJoins(ft.Source) };
                case DelayPlanNode dl: return dl with { Source = ReorderJoins(dl.Source) };
                case ExplodePlanNode ex: return ex with { Source = ReorderJoins(ex.Source) };
                case UnnestPlanNode un: return un with { Source = ReorderJoins(un.Source) };
                case UnpivotPlanNode up: return up with { Source = ReorderJoins(up.Source) };
                case PivotPlanNode pv: return pv with { Source = ReorderJoins(pv.Source) };
                case TransposePlanNode tr: return tr with { Source = ReorderJoins(tr.Source) };
                case UniquePlanNode uq: return uq with { Source = ReorderJoins(uq.Source) };
                case SlicePlanNode sl: return sl with { Source = ReorderJoins(sl.Source) };
                case TailPlanNode tl: return tl with { Source = ReorderJoins(tl.Source) };
                case DropNullsPlanNode dn: return dn with { Source = ReorderJoins(dn.Source) };
                case FillNanPlanNode fn: return fn with { Source = ReorderJoins(fn.Source) };
                case WithRowIndexPlanNode wri: return wri with { Source = ReorderJoins(wri.Source) };
                case RenamePlanNode rn: return rn with { Source = ReorderJoins(rn.Source) };
                case NullCountPlanNode nc: return nc with { Source = ReorderJoins(nc.Source) };
                case ShiftColumnsPlanNode sc: return sc with { Source = ReorderJoins(sc.Source) };
                case ClearPlanNode cl: return cl with { Source = ReorderJoins(cl.Source) };
                case ShrinkToFitPlanNode sf: return sf with { Source = ReorderJoins(sf.Source) };
                case RechunkPlanNode rc: return rc with { Source = ReorderJoins(rc.Source) };
                case MapPlanNode mp: return mp with { Source = ReorderJoins(mp.Source) };
                case SinkCsvPlanNode scsv: return scsv with { Source = ReorderJoins(scsv.Source) };
                case SinkParquetPlanNode spq: return spq with { Source = ReorderJoins(spq.Source) };
                case SinkIpcPlanNode sipc: return sipc with { Source = ReorderJoins(sipc.Source) };
                default: return node;
            }

            static bool SchemasMatchOrUnresolved(LogicalPlanNode originalPlan, LogicalPlanNode candidatePlan)
            {
                if (PlanSchemaResolver.TryResolveColumns(originalPlan, out var origCols)
                    && PlanSchemaResolver.TryResolveColumns(candidatePlan, out var candCols))
                {
                    return origCols.SequenceEqual(candCols, StringComparer.Ordinal);
                }
                return true;
            }
        }

        private static LogicalPlanNode PushdownProjection(LogicalPlanNode node, HashSet<string> usedColumns)
        {
            switch (node)
            {
                case ScanCsvPlanNode csv:
                    try
                    {
                        string[] sourceCols = csv.Columns is { Length: > 0 } ? csv.Columns : IO.CsvReader.PeekHeaders(csv.FilePath);
                        var validColumns = sourceCols.Where(usedColumns.Contains).ToArray();
                        return validColumns.Length > 0 ? csv with { Columns = validColumns } : csv;
                    }
                    catch { return csv; }

                case ScanParquetPlanNode pq:
                    try
                    {
                        string[] sourceCols = pq.Columns is { Length: > 0 } ? pq.Columns : IO.ParquetReader.PeekHeaders(pq.FilePath);
                        var validColumns = sourceCols.Where(usedColumns.Contains).ToArray();
                        return validColumns.Length > 0 ? pq with { Columns = validColumns } : pq;
                    }
                    catch { return pq; }

                case ScanJsonPlanNode json:
                    try
                    {
                        string[] sourceCols = json.Columns is { Length: > 0 } ? json.Columns : IO.JsonReader.PeekHeaders(json.FilePath);
                        var validColumns = sourceCols.Where(usedColumns.Contains).ToArray();
                        return validColumns.Length > 0 ? json with { Columns = validColumns } : json;
                    }
                    catch { return json; }

                case FilterPlanNode f: return f with { Source = PushdownProjection(f.Source, usedColumns) };
                case SelectPlanNode s: return s with { Source = PushdownProjection(s.Source, usedColumns) };
                case WithColumnsPlanNode wc: return wc with { Source = PushdownProjection(wc.Source, usedColumns) };
                case GroupByPlanNode gb: return gb with { Source = PushdownProjection(gb.Source, usedColumns) };
                case AggPlanNode ag: return ag with { Source = PushdownProjection(ag.Source, usedColumns) };
                case AggGroupsPlanNode agg: return agg with { Source = PushdownProjection(agg.Source, usedColumns) };
                case GroupByDynamicPlanNode gbd: return gbd with { Source = PushdownProjection(gbd.Source, usedColumns) };
                case GroupByRollingPlanNode gbr: return gbr with { Source = PushdownProjection(gbr.Source, usedColumns) };
                case JoinPlanNode j: return j with { Left = PushdownProjection(j.Left, usedColumns), Right = PushdownProjection(j.Right, usedColumns) };
                case SortPlanNode st: return st with { Source = PushdownProjection(st.Source, usedColumns) };
                case TopKPlanNode tk: return tk with { Source = PushdownProjection(tk.Source, usedColumns) };
                case LimitPlanNode lm: return lm with { Source = PushdownProjection(lm.Source, usedColumns) };
                case FetchPlanNode ft: return ft with { Source = PushdownProjection(ft.Source, usedColumns) };
                case DelayPlanNode dl: return dl with { Source = PushdownProjection(dl.Source, usedColumns) };
                case ExplodePlanNode ex: return ex with { Source = PushdownProjection(ex.Source, usedColumns) };
                case UnnestPlanNode un: return un with { Source = PushdownProjection(un.Source, usedColumns) };
                case UnpivotPlanNode up: return up with { Source = PushdownProjection(up.Source, usedColumns) };
                case PivotPlanNode pv: return pv with { Source = PushdownProjection(pv.Source, usedColumns) };
                case TransposePlanNode tr: return tr with { Source = PushdownProjection(tr.Source, usedColumns) };
                case UniquePlanNode uq: return uq with { Source = PushdownProjection(uq.Source, usedColumns) };
                case SlicePlanNode sl: return sl with { Source = PushdownProjection(sl.Source, usedColumns) };
                case TailPlanNode tl: return tl with { Source = PushdownProjection(tl.Source, usedColumns) };
                case DropNullsPlanNode dn: return dn with { Source = PushdownProjection(dn.Source, usedColumns) };
                case FillNanPlanNode fn: return fn with { Source = PushdownProjection(fn.Source, usedColumns) };
                case WithRowIndexPlanNode wri: return wri with { Source = PushdownProjection(wri.Source, usedColumns) };
                case RenamePlanNode rn: return rn with { Source = PushdownProjection(rn.Source, usedColumns) };
                case NullCountPlanNode nc: return nc with { Source = PushdownProjection(nc.Source, usedColumns) };
                case ShiftColumnsPlanNode sc: return sc with { Source = PushdownProjection(sc.Source, usedColumns) };
                case ClearPlanNode cl: return cl with { Source = PushdownProjection(cl.Source, usedColumns) };
                case ShrinkToFitPlanNode sf: return sf with { Source = PushdownProjection(sf.Source, usedColumns) };
                case RechunkPlanNode rc: return rc with { Source = PushdownProjection(rc.Source, usedColumns) };
                case MapPlanNode mp: return mp with { Source = PushdownProjection(mp.Source, usedColumns) };
                case SinkCsvPlanNode scsv: return scsv with { Source = PushdownProjection(scsv.Source, usedColumns) };
                case SinkParquetPlanNode spq: return spq with { Source = PushdownProjection(spq.Source, usedColumns) };
                case SinkIpcPlanNode sipc: return sipc with { Source = PushdownProjection(sipc.Source, usedColumns) };
                default: return node;
            }
        }
    }

    internal static class OptimizerAnalysis
    {
        private static readonly HashSet<string> NonElementwiseExprMethods = new(StringComparer.Ordinal)
        {
            "SumOp", "MeanOp", "MinOp", "MaxOp", "StdOp", "VarOp", "MedianOp",
            "CountOp", "NUniqueOp", "QuantileOp", "FirstOp", "LastOp",
            "AllOp", "AnyOp", "NullCountOp", "ArgMinOp", "ArgMaxOp",
            "DotOp", "EntropyOp", "ApproxNUniqueOp", "ListOp",

            "OverOp",
            "RollingMeanOp", "RollingSumOp", "RollingMinOp", "RollingMaxOp", "RollingStdOp",
            "EWMMeanOp", "EWMStdOp",
            "ExpandingMeanOp", "ExpandingSumOp", "ExpandingMinOp", "ExpandingMaxOp", "ExpandingStdOp",
            "CumCountOp", "CumProdOp",
            "ShiftOp", "DiffOp", "PctChangeOp", "RankOp",
            "IsDuplicatedOp", "IsUniqueOp", "IsFirstOp",
            "FillNullOp", "IsInExprOp", "SearchSortedOp",

            "UniqueOp", "DropNullsOp", "GatherEveryOp", "SliceOp",
            "TopKOp", "BottomKOp", "ValueCountsOp"
        };

        public static bool IsPureElementwise(Expression expr)
        {
            var checker = new PureElementwiseVisitor();
            checker.Visit(expr);
            return checker.IsPure;
        }

        public static void CollectReferencedColumns(Expression expr, HashSet<string> columns)
        {
            var collector = new ExprColumnCollector(columns);
            collector.Visit(expr);
        }

        public static Expression RewriteColumns(Expression expr, IReadOnlyDictionary<string, string> columnMap)
        {
            return new ExprColumnRewriter(columnMap).Visit(expr);
        }

        public static bool TryExtractPredicateExpr(Expression predicateArg, out Expr expr)
        {
            expr = null!;
            Expression current = predicateArg;
            while (current is UnaryExpression ue && ue.NodeType == ExpressionType.Quote)
            {
                current = ue.Operand;
            }

            if (current is Expression<Func<Expr, Expr>> exprLambda)
            {
                try
                {
                    expr = ExprLambdaEvaluator.Evaluate(exprLambda);
                    return !ReferenceEquals(expr, null);
                }
                catch
                {
                    return false;
                }
            }

            if (current is ConstantExpression ce && ce.Value is Expr constExpr)
            {
                expr = constExpr;
                return true;
            }

            return false;
        }

        public static bool TryExtractExprArray(Expression arg, out Expr[] exprs)
        {
            if (arg is ConstantExpression ce && ce.Value is Expr[] arr)
            {
                exprs = arr;
                return true;
            }

            if (arg is NewArrayExpression nae)
            {
                var list = new Expr[nae.Expressions.Count];
                for (int i = 0; i < nae.Expressions.Count; i++)
                {
                    var item = nae.Expressions[i];
                    while (item is UnaryExpression ue && ue.NodeType == ExpressionType.Convert)
                        item = ue.Operand;

                    if (item is ConstantExpression itemConst && itemConst.Value is Expr e)
                    {
                        list[i] = e;
                    }
                    else
                    {
                        exprs = Array.Empty<Expr>();
                        return false;
                    }
                }
                exprs = list;
                return true;
            }

            exprs = Array.Empty<Expr>();
            return false;
        }

        public static bool TryExtractStringArray(Expression arg, out string[] values)
        {
            if (arg is ConstantExpression ce)
            {
                if (ce.Value is string[] arr)
                {
                    values = arr;
                    return true;
                }
                if (ce.Value is null)
                {
                    values = Array.Empty<string>();
                    return false;
                }
            }

            if (arg is NewArrayExpression nae)
            {
                var result = new string[nae.Expressions.Count];
                for (int i = 0; i < nae.Expressions.Count; i++)
                {
                    if (nae.Expressions[i] is ConstantExpression itemConst && itemConst.Value is string s)
                    {
                        result[i] = s;
                    }
                    else
                    {
                        values = Array.Empty<string>();
                        return false;
                    }
                }
                values = result;
                return true;
            }

            values = Array.Empty<string>();
            return false;
        }

        public static bool TryInferExprOutputName(Expression body, out string? name)
        {
            if (body is MethodCallExpression mce)
            {
                if (mce.Method.Name == "AliasOp" && mce.Arguments[1] is ConstantExpression aliasConst && aliasConst.Value is string alias)
                {
                    name = alias;
                    return true;
                }
                if (mce.Method.Name == "Col" && mce.Arguments[0] is ConstantExpression colConst && colConst.Value is string col)
                {
                    name = col;
                    return true;
                }
                if (mce.Method.Name == "LitOp")
                {
                    name = "literal";
                    return true;
                }
                if (mce.Method.Name == "RegexMatchOp" && mce.Arguments[0] is ConstantExpression rxConst && rxConst.Value is string rxCol)
                {
                    name = rxCol;
                    return true;
                }
                if (mce.Arguments.Count > 0)
                {
                    return TryInferExprOutputName(mce.Arguments[0], out name);
                }
            }
            else if (body is BinaryExpression)
            {
                name = "res";
                return true;
            }
            else if (body is UnaryExpression ue)
            {
                return TryInferExprOutputName(ue.Operand, out name);
            }
            else if (body is ConstantExpression ce && ce.Value is Expr innerExpr)
            {
                return TryInferExprOutputName(innerExpr.Expression, out name);
            }

            name = null;
            return false;
        }

        private sealed class PureElementwiseVisitor : ExpressionVisitor
        {
            public bool IsPure { get; private set; } = true;

            public override Expression? Visit(Expression? node)
            {
                if (!IsPure || node == null) return node;
                return base.Visit(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (NonElementwiseExprMethods.Contains(node.Method.Name))
                {
                    IsPure = false;
                    return node;
                }
                return base.VisitMethodCall(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value is Expr expr)
                {
                    Visit(expr.Expression);
                }
                else if (node.Value is Expr[] exprArr)
                {
                    for (int i = 0; i < exprArr.Length; i++)
                        Visit(exprArr[i].Expression);
                }
                return base.VisitConstant(node);
            }
        }

        private sealed class ExprColumnCollector : ExpressionVisitor
        {
            private readonly HashSet<string> _columns;
            public ExprColumnCollector(HashSet<string> columns) => _columns = columns;

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if ((node.Method.Name == "Col" || node.Method.Name == "RegexMatchOp")
                    && node.Arguments.Count > 0
                    && node.Arguments[0] is ConstantExpression ce
                    && ce.Value is string colName)
                {
                    _columns.Add(colName);
                }
                return base.VisitMethodCall(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value is Expr expr)
                {
                    Visit(expr.Expression);
                }
                else if (node.Value is Expr[] exprArr)
                {
                    for (int i = 0; i < exprArr.Length; i++)
                        Visit(exprArr[i].Expression);
                }
                return base.VisitConstant(node);
            }
        }

        private sealed class ExprColumnRewriter : ExpressionVisitor
        {
            private readonly IReadOnlyDictionary<string, string> _map;
            public ExprColumnRewriter(IReadOnlyDictionary<string, string> map) => _map = map;

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.Name == "Col"
                    && node.Arguments.Count == 1
                    && node.Arguments[0] is ConstantExpression ce
                    && ce.Value is string colName
                    && _map.TryGetValue(colName, out string? mapped)
                    && !string.Equals(colName, mapped, StringComparison.Ordinal))
                {
                    return Expression.Call(null, node.Method, Expression.Constant(mapped));
                }

                if (node.Method.Name == "RegexMatchOp"
                    && node.Arguments.Count == 2
                    && node.Arguments[0] is ConstantExpression rxCe
                    && rxCe.Value is string rxCol
                    && _map.TryGetValue(rxCol, out string? rxMapped)
                    && !string.Equals(rxCol, rxMapped, StringComparison.Ordinal))
                {
                    return Expression.Call(null, node.Method, Expression.Constant(rxMapped), node.Arguments[1]);
                }

                return base.VisitMethodCall(node);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value is Expr expr)
                {
                    var rewrittenInner = Visit(expr.Expression);
                    if (!ReferenceEquals(rewrittenInner, expr.Expression))
                    {
                        return Expression.Constant(new Expr(rewrittenInner), typeof(Expr));
                    }
                }
                else if (node.Value is Expr[] exprArr)
                {
                    bool changed = false;
                    var updated = new Expr[exprArr.Length];
                    for (int i = 0; i < exprArr.Length; i++)
                    {
                        var r = Visit(exprArr[i].Expression);
                        if (!ReferenceEquals(r, exprArr[i].Expression))
                        {
                            changed = true;
                            updated[i] = new Expr(r);
                        }
                        else
                        {
                            updated[i] = exprArr[i];
                        }
                    }
                    if (changed)
                    {
                        return Expression.Constant(updated, typeof(Expr[]));
                    }
                }
                return base.VisitConstant(node);
            }
        }
    }

    internal static class PlanSchemaResolver
    {
        public static bool TryResolveColumns(Expression plan, out List<string> columns)
        {
            return TryResolveColumns(LogicalPlanNode.FromLegacyExpression(plan), out columns);
        }

        public static bool TryResolveColumns(LogicalPlanNode plan, out List<string> columns)
        {
            columns = null!;

            switch (plan)
            {
                case DataFramePlanNode dfNode:
                    columns = dfNode.DataFrame.Columns.Select(c => c.Name).ToList();
                    return true;

                case ScanCsvPlanNode csv:
                    if (csv.Columns is { Length: > 0 })
                    {
                        columns = csv.Columns.ToList();
                        return true;
                    }
                    if (System.IO.File.Exists(csv.FilePath))
                    {
                        try
                        {
                            columns = IO.CsvReader.PeekHeaders(csv.FilePath).ToList();
                            return columns.Count > 0;
                        }
                        catch { return false; }
                    }
                    return false;

                case ScanParquetPlanNode pq:
                    if (pq.Columns is { Length: > 0 })
                    {
                        columns = pq.Columns.ToList();
                        return true;
                    }
                    if (System.IO.File.Exists(pq.FilePath))
                    {
                        try
                        {
                            columns = IO.ParquetReader.PeekHeaders(pq.FilePath).ToList();
                            return columns.Count > 0;
                        }
                        catch { return false; }
                    }
                    return false;

                case ScanJsonPlanNode json:
                    if (json.Columns is { Length: > 0 })
                    {
                        columns = json.Columns.ToList();
                        return true;
                    }
                    if (System.IO.File.Exists(json.FilePath))
                    {
                        try
                        {
                            columns = IO.JsonReader.PeekHeaders(json.FilePath).ToList();
                            return columns.Count > 0;
                        }
                        catch { return false; }
                    }
                    return false;

                case FilterPlanNode f: return TryResolveColumns(f.Source, out columns);
                case LimitPlanNode l: return TryResolveColumns(l.Source, out columns);
                case FetchPlanNode ft: return TryResolveColumns(ft.Source, out columns);
                case TailPlanNode tl: return TryResolveColumns(tl.Source, out columns);
                case SlicePlanNode sl: return TryResolveColumns(sl.Source, out columns);
                case SortPlanNode st: return TryResolveColumns(st.Source, out columns);
                case TopKPlanNode tk: return TryResolveColumns(tk.Source, out columns);
                case UniquePlanNode uq: return TryResolveColumns(uq.Source, out columns);
                case DropNullsPlanNode dn: return TryResolveColumns(dn.Source, out columns);
                case FillNanPlanNode fn: return TryResolveColumns(fn.Source, out columns);
                case DelayPlanNode dl: return TryResolveColumns(dl.Source, out columns);
                case ShiftColumnsPlanNode sc: return TryResolveColumns(sc.Source, out columns);
                case ClearPlanNode cl: return TryResolveColumns(cl.Source, out columns);
                case ShrinkToFitPlanNode sf: return TryResolveColumns(sf.Source, out columns);
                case RechunkPlanNode rc: return TryResolveColumns(rc.Source, out columns);
                case ExplodePlanNode ex: return TryResolveColumns(ex.Source, out columns);

                case SelectPlanNode sel:
                {
                    var outCols = new List<string>(sel.Selections.Length);
                    for (int i = 0; i < sel.Selections.Length; i++)
                    {
                        if (!OptimizerAnalysis.TryInferExprOutputName(sel.Selections[i].Expression, out string? outName) || outName == null)
                            return false;
                        outCols.Add(outName);
                    }
                    columns = outCols;
                    return true;
                }

                case WithColumnsPlanNode wc:
                {
                    if (!TryResolveColumns(wc.Source, out var baseCols))
                        return false;
                    var outCols = new List<string>(baseCols);
                    for (int i = 0; i < wc.Columns.Length; i++)
                    {
                        if (!OptimizerAnalysis.TryInferExprOutputName(wc.Columns[i].Expression, out string? outName) || outName == null)
                            return false;
                        if (!outCols.Contains(outName))
                            outCols.Add(outName);
                    }
                    columns = outCols;
                    return true;
                }

                case RenamePlanNode rn:
                {
                    if (!TryResolveColumns(rn.Source, out var baseCols))
                        return false;
                    columns = baseCols.Select(c => rn.Mapping.TryGetValue(c, out var renamed) ? renamed : c).ToList();
                    return true;
                }

                case WithRowIndexPlanNode wri:
                {
                    if (!TryResolveColumns(wri.Source, out var baseCols))
                        return false;
                    var outCols = new List<string>(baseCols.Count + 1) { wri.Name };
                    outCols.AddRange(baseCols);
                    columns = outCols;
                    return true;
                }

                case JoinPlanNode j:
                {
                    if (!TryResolveColumns(j.Left, out var leftCols) ||
                        !TryResolveColumns(j.Right, out var rightCols))
                        return false;

                    if (j.JoinType == JoinType.Semi || j.JoinType == JoinType.Anti)
                    {
                        columns = new List<string>(leftCols);
                        return true;
                    }

                    var outCols = new List<string>(leftCols);
                    var leftSet = new HashSet<string>(leftCols, StringComparer.Ordinal);
                    var rightKeySet = new HashSet<string>(j.RightOn, StringComparer.Ordinal);
                    foreach (var rc in rightCols)
                    {
                        if (j.JoinType != JoinType.Cross && j.JoinType != JoinType.Outer && rightKeySet.Contains(rc))
                            continue;
                        outCols.Add(leftSet.Contains(rc) ? rc + "_right" : rc);
                    }
                    columns = outCols;
                    return true;
                }

                default:
                    return false;
            }
        }
    }

    internal sealed class LogicalPlanColumnTracker
    {
        public HashSet<string> Columns { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, HashSet<string>> NestedFieldPaths { get; } = new(StringComparer.OrdinalIgnoreCase);
        public bool HasGlobalSelect { get; private set; }
        public bool RequiresAllColumns { get; private set; }

        public void Visit(LogicalPlanNode node)
        {
            switch (node)
            {
                case FilterPlanNode f:
                    VisitExpr(f.Predicate.Expression);
                    Visit(f.Source);
                    break;

                case SelectPlanNode sel:
                    HasGlobalSelect = true;
                    foreach (var e in sel.Selections) VisitExpr(e.Expression);
                    Visit(sel.Source);
                    break;

                case WithColumnsPlanNode wc:
                    foreach (var e in wc.Columns) VisitExpr(e.Expression);
                    Visit(wc.Source);
                    break;

                case AggPlanNode ag:
                    if (ag.GroupColumns != null)
                        foreach (var c in ag.GroupColumns) Columns.Add(c);
                    foreach (var e in ag.Aggregations) VisitExpr(e.Expression);
                    Visit(ag.Source);
                    break;

                case JoinPlanNode j:
                    if (j.JoinType != JoinType.Cross)
                    {
                        foreach (var c in j.LeftOn) Columns.Add(c);
                        foreach (var c in j.RightOn) Columns.Add(c);
                    }
                    Visit(j.Left);
                    Visit(j.Right);
                    break;

                case SortPlanNode st:
                    foreach (var c in st.ColumnNames) Columns.Add(c);
                    Visit(st.Source);
                    break;

                case TopKPlanNode tk:
                    foreach (var c in tk.ColumnNames) Columns.Add(c);
                    Visit(tk.Source);
                    break;

                case GroupByPlanNode gb:
                    foreach (var c in gb.Columns) Columns.Add(c);
                    Visit(gb.Source);
                    break;

                case AggGroupsPlanNode agp:
                    foreach (var c in agp.Columns) Columns.Add(c);
                    Visit(agp.Source);
                    break;

                case UnnestPlanNode un:
                    foreach (var c in un.Columns) Columns.Add(c);
                    Visit(un.Source);
                    break;

                case GroupByDynamicPlanNode gbd:
                    Columns.Add(gbd.IndexColumn);
                    Visit(gbd.Source);
                    break;

                case GroupByRollingPlanNode gbr:
                    Columns.Add(gbr.IndexColumn);
                    Visit(gbr.Source);
                    break;

                case ExplodePlanNode ex:
                    Columns.Add(ex.Column);
                    Visit(ex.Source);
                    break;

                case UnpivotPlanNode up:
                    foreach (var c in up.On) Columns.Add(c);
                    foreach (var c in up.Index) Columns.Add(c);
                    Visit(up.Source);
                    break;

                case PivotPlanNode pv:
                    foreach (var c in pv.Index) Columns.Add(c);
                    foreach (var c in pv.Pivot) Columns.Add(c);
                    foreach (var c in pv.Values) Columns.Add(c);
                    Visit(pv.Source);
                    break;

                case DropNullsPlanNode dn:
                    if (dn.Subset is { Length: > 0 })
                        foreach (var c in dn.Subset) Columns.Add(c);
                    else
                        RequiresAllColumns = true;
                    Visit(dn.Source);
                    break;

                case UniquePlanNode uq:
                    if (uq.Subset is { Length: > 0 })
                        foreach (var c in uq.Subset) Columns.Add(c);
                    else
                        RequiresAllColumns = true;
                    Visit(uq.Source);
                    break;

                case RenamePlanNode rn:
                    RequiresAllColumns = true;
                    Visit(rn.Source);
                    break;

                case TransposePlanNode tr:
                    RequiresAllColumns = true;
                    Visit(tr.Source);
                    break;

                case NullCountPlanNode nc:
                    RequiresAllColumns = true;
                    Visit(nc.Source);
                    break;

                case ShiftColumnsPlanNode sc:
                    RequiresAllColumns = true;
                    Visit(sc.Source);
                    break;

                case FillNanPlanNode fn:
                    RequiresAllColumns = true;
                    Visit(fn.Source);
                    break;

                case MapPlanNode mp:
                    RequiresAllColumns = true;
                    Visit(mp.Source);
                    break;

                case LimitPlanNode lm: Visit(lm.Source); break;
                case FetchPlanNode ft: Visit(ft.Source); break;
                case DelayPlanNode dl: Visit(dl.Source); break;
                case SlicePlanNode sl: Visit(sl.Source); break;
                case TailPlanNode tl: Visit(tl.Source); break;
                case WithRowIndexPlanNode wri: Visit(wri.Source); break;
                case ClearPlanNode cl: Visit(cl.Source); break;
                case ShrinkToFitPlanNode sf: Visit(sf.Source); break;
                case RechunkPlanNode rc: Visit(rc.Source); break;
                case SinkCsvPlanNode scsv: Visit(scsv.Source); break;
                case SinkParquetPlanNode spq: Visit(spq.Source); break;
                case SinkIpcPlanNode sipc: Visit(sipc.Source); break;

                case ExternalExpressionPlanNode ext:
                {
                    var exprTracker = new ColumnTrackerVisitor();
                    exprTracker.Visit(ext.Expression);
                    foreach (var c in exprTracker.Columns) Columns.Add(c);
                    foreach (var kv in exprTracker.NestedFieldPaths)
                    {
                        if (!NestedFieldPaths.TryGetValue(kv.Key, out var set))
                        {
                            set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            NestedFieldPaths[kv.Key] = set;
                        }
                        foreach (var p in kv.Value) set.Add(p);
                    }
                    if (exprTracker.HasGlobalSelect) HasGlobalSelect = true;
                    if (exprTracker.RequiresAllColumns) RequiresAllColumns = true;
                    break;
                }
            }
        }

        private void VisitExpr(Expression expr)
        {
            var exprTracker = new ColumnTrackerVisitor();
            exprTracker.Visit(expr);
            foreach (var c in exprTracker.Columns) Columns.Add(c);
            foreach (var kv in exprTracker.NestedFieldPaths)
            {
                if (!NestedFieldPaths.TryGetValue(kv.Key, out var set))
                {
                    set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    NestedFieldPaths[kv.Key] = set;
                }
                foreach (var p in kv.Value) set.Add(p);
            }
        }
    }

    internal class ColumnTrackerVisitor : ExpressionVisitor
    {
        public HashSet<string> Columns { get; } = new HashSet<string>(StringComparer.Ordinal);
        public Dictionary<string, HashSet<string>> NestedFieldPaths { get; } = new(StringComparer.OrdinalIgnoreCase);
        public bool HasGlobalSelect { get; private set; } = false;
        public bool RequiresAllColumns { get; private set; } = false;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            switch (node.Method.Name)
            {
                case "Col":
                case "RegexMatchOp":
                    if (node.Arguments.Count > 0 && node.Arguments[0] is ConstantExpression colCe && colCe.Value is string col)
                    {
                        Columns.Add(col);
                    }
                    break;

                case "OverOp":
                    if (node.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var overCols))
                    {
                        foreach (var oc in overCols) Columns.Add(oc);
                    }
                    break;

                case "Struct_FieldOp":
                {
                    var path = new List<string>();
                    var current = (Expression)node;
                    while (current is MethodCallExpression mce && mce.Method.Name == "Struct_FieldOp")
                    {
                        if (mce.Arguments[1] is ConstantExpression ce && ce.Value is string fname)
                        {
                            path.Add(fname);
                        }
                        current = mce.Arguments[0];
                        if (current is ConstantExpression exprConst && exprConst.Value is Expr innerExpr)
                        {
                            current = innerExpr.Expression;
                        }
                    }
                    if (current is MethodCallExpression colCall && colCall.Method.Name == "Col" &&
                        colCall.Arguments[0] is ConstantExpression colConst && colConst.Value is string colName)
                    {
                        Columns.Add(colName);
                        path.Reverse();
                        string fullPath = string.Join(".", path);
                        if (!NestedFieldPaths.TryGetValue(colName, out var paths))
                        {
                            paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                            NestedFieldPaths[colName] = paths;
                        }
                        paths.Add(fullPath);
                        if (path.Count > 0) paths.Add(path[0]);
                    }
                    break;
                }
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is Expr expr)
            {
                Visit(expr.Expression);
            }
            else if (node.Value is Expr[] exprArr)
            {
                for (int i = 0; i < exprArr.Length; i++)
                {
                    Visit(exprArr[i].Expression);
                }
            }
            return base.VisitConstant(node);
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (node is Expression<Func<Expr, Expr>> exprLambda)
            {
                try
                {
                    var result = ExprLambdaEvaluator.Evaluate(exprLambda);
                    Visit(result.Expression);
                }
                catch
                {
                    RequiresAllColumns = true;
                }
            }
            return base.VisitLambda(node);
        }
    }
}
