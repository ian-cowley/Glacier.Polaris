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
            // 1. Analyze: Find all columns used in the entire query
            var tracker = new ColumnTrackerVisitor();
            tracker.Visit(plan);
            NestedFieldPaths = tracker.NestedFieldPaths;

            // 2. Transform: Sound Predicate & Limit Pushdown
            var transformed = Visit(plan);

            // 3. Join Reordering: Sound inner-join associative reordering
            transformed = new JoinReorderingVisitor().Visit(transformed);

            // 4. Inject: Projection Pushdown (tell scanners what columns to load)
            if (tracker.HasGlobalSelect && !tracker.RequiresAllColumns && tracker.Columns.Count > 0)
            {
                transformed = new ProjectionPushdownVisitor(tracker.Columns).Visit(transformed);
            }

            return transformed;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var visitedNode = (MethodCallExpression)base.VisitMethodCall(node);

            if (visitedNode.Method.Name == nameof(LazyFrame.FilterOp))
            {
                var source = visitedNode.Arguments[0];
                var predicate = visitedNode.Arguments[1];

                if (source is MethodCallExpression sourceCall && sourceCall.Method.Name == nameof(LazyFrame.SelectOp))
                {
                    if (TryPushFilterThroughSelect(visitedNode, sourceCall, predicate, out var rewrittenSelect))
                    {
                        return rewrittenSelect;
                    }
                }
                else if (source is MethodCallExpression sourceJoin && sourceJoin.Method.Name == nameof(LazyFrame.JoinOp))
                {
                    if (TryPushFilterThroughJoin(visitedNode, sourceJoin, predicate, out var rewrittenJoin))
                    {
                        return rewrittenJoin;
                    }
                }
                else if (source is MethodCallExpression sourceGroupBy && sourceGroupBy.Method.Name == nameof(LazyFrame.GroupByOp))
                {
                    if (OptimizerAnalysis.TryExtractPredicateExpr(predicate, out var predExpr)
                        && OptimizerAnalysis.IsPureElementwise(predExpr.Expression)
                        && OptimizerAnalysis.TryExtractStringArray(sourceGroupBy.Arguments[1], out var groupCols))
                    {
                        var predCols = new HashSet<string>(StringComparer.Ordinal);
                        OptimizerAnalysis.CollectReferencedColumns(predExpr.Expression, predCols);

                        if (predCols.Count > 0 && predCols.All(c => groupCols.Contains(c, StringComparer.Ordinal)))
                        {
                            var newFilter = Expression.Call(null, visitedNode.Method, sourceGroupBy.Arguments[0], predicate);
                            return Expression.Call(null, sourceGroupBy.Method, Visit(newFilter), sourceGroupBy.Arguments[1]);
                        }
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
                        int? existingLimit = (int?)((ConstantExpression)sourceCall.Arguments[2]).Value;
                        int effectiveLimit = existingLimit.HasValue ? Math.Min(existingLimit.Value, n) : n;
                        return Expression.Call(null, sourceCall.Method,
                            sourceCall.Arguments[0],
                            sourceCall.Arguments[1],
                            Expression.Constant((int?)effectiveLimit, typeof(int?)));
                    }
                    else if (sourceCall.Method.Name == nameof(LazyFrame.SortOp))
                    {
                        var method = typeof(LazyFrame).GetMethod(
                            nameof(LazyFrame.TopKOp),
                            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
                        return Expression.Call(null, method, sourceCall.Arguments[0], sourceCall.Arguments[1], sourceCall.Arguments[2], visitedNode.Arguments[1]);
                    }
                    else if (sourceCall.Method.Name == nameof(LazyFrame.SelectOp))
                    {
                        if (OptimizerAnalysis.TryExtractExprArray(sourceCall.Arguments[1], out var selectExprs)
                            && selectExprs.Length > 0
                            && selectExprs.All(se => OptimizerAnalysis.IsPureElementwise(se.Expression)))
                        {
                            var innerSource = sourceCall.Arguments[0];
                            var newLimit = Expression.Call(null, visitedNode.Method, innerSource, visitedNode.Arguments[1]);
                            return Expression.Call(null, sourceCall.Method, Visit(newLimit), sourceCall.Arguments[1]);
                        }
                    }
                }
            }

            return visitedNode;
        }

        private bool TryPushFilterThroughSelect(
            MethodCallExpression filterNode,
            MethodCallExpression selectNode,
            Expression predicateArg,
            out Expression result)
        {
            result = null!;

            if (!OptimizerAnalysis.TryExtractExprArray(selectNode.Arguments[1], out var selectExprs) || selectExprs.Length == 0)
                return false;

            // Never push Filter below Select if any projection contains aggregations, window ops, cumulative/positional ops, or length-changing ops.
            for (int i = 0; i < selectExprs.Length; i++)
            {
                if (!OptimizerAnalysis.IsPureElementwise(selectExprs[i].Expression))
                    return false;
            }

            if (!OptimizerAnalysis.TryExtractPredicateExpr(predicateArg, out var predExpr))
                return false;

            if (!OptimizerAnalysis.IsPureElementwise(predExpr.Expression))
                return false;

            // Build map of output column name -> underlying source column name for pure pass-through projections.
            var passThroughMap = new Dictionary<string, string>(StringComparer.Ordinal);
            var seenOutputNames = new HashSet<string>(StringComparer.Ordinal);

            for (int i = 0; i < selectExprs.Length; i++)
            {
                var body = selectExprs[i].Expression;
                if (body is MethodCallExpression colCall && colCall.Method.Name == "Col"
                    && colCall.Arguments[0] is ConstantExpression colConst && colConst.Value is string colName)
                {
                    if (!seenOutputNames.Add(colName))
                    {
                        // Duplicate output column name: ambiguous, do not push through it
                        passThroughMap.Remove(colName);
                    }
                    else
                    {
                        passThroughMap[colName] = colName;
                    }
                }
                else if (body is MethodCallExpression aliasCall && aliasCall.Method.Name == "AliasOp"
                    && aliasCall.Arguments[1] is ConstantExpression aliasConst && aliasConst.Value is string aliasName)
                {
                    var inner = aliasCall.Arguments[0];
                    if (inner is MethodCallExpression innerCol && innerCol.Method.Name == "Col"
                        && innerCol.Arguments[0] is ConstantExpression innerColConst && innerColConst.Value is string srcColName)
                    {
                        if (!seenOutputNames.Add(aliasName))
                        {
                            passThroughMap.Remove(aliasName);
                        }
                        else
                        {
                            passThroughMap[aliasName] = srcColName;
                        }
                    }
                    else
                    {
                        // Computed expression aliased as aliasName shadows any pass-through of the same name
                        seenOutputNames.Add(aliasName);
                        passThroughMap.Remove(aliasName);
                    }
                }
                else
                {
                    // Unaliased computed expression (e.g. arithmetic producing "res" or unary op keeping source name)
                    if (OptimizerAnalysis.TryInferExprOutputName(body, out string? inferredName) && inferredName != null)
                    {
                        seenOutputNames.Add(inferredName);
                        passThroughMap.Remove(inferredName);
                    }
                    else
                    {
                        // Cannot determine output name safely -> abort pushdown
                        return false;
                    }
                }
            }

            var predCols = new HashSet<string>(StringComparer.Ordinal);
            OptimizerAnalysis.CollectReferencedColumns(predExpr.Expression, predCols);
            if (predCols.Count == 0)
                return false;

            foreach (var col in predCols)
            {
                if (!passThroughMap.ContainsKey(col))
                    return false;
            }

            Expression rewrittenPredicateArg = predicateArg;
            if (predCols.Any(c => !string.Equals(c, passThroughMap[c], StringComparison.Ordinal)))
            {
                var rewrittenBody = OptimizerAnalysis.RewriteColumns(predExpr.Expression, passThroughMap);
                var rewrittenExpr = new Expr(rewrittenBody);
                var param = Expression.Parameter(typeof(Expr), "e");
                var lambda = Expression.Lambda<Func<Expr, Expr>>(Expression.Constant(rewrittenExpr, typeof(Expr)), param);
                rewrittenPredicateArg = Expression.Quote(lambda);
            }

            var innerSource = selectNode.Arguments[0];
            var newFilter = Expression.Call(null, filterNode.Method, innerSource, rewrittenPredicateArg);
            result = Expression.Call(null, selectNode.Method, Visit(newFilter), selectNode.Arguments[1]);
            return true;
        }

        private bool TryPushFilterThroughJoin(
            MethodCallExpression filterNode,
            MethodCallExpression joinNode,
            Expression predicateArg,
            out Expression result)
        {
            result = null!;

            if (!OptimizerAnalysis.TryExtractPredicateExpr(predicateArg, out var predExpr))
                return false;

            if (!OptimizerAnalysis.IsPureElementwise(predExpr.Expression))
                return false;

            var predCols = new HashSet<string>(StringComparer.Ordinal);
            OptimizerAnalysis.CollectReferencedColumns(predExpr.Expression, predCols);

            // Never push constant / zero-column predicates into joins (e.g., Left/Outer/Cross joins).
            if (predCols.Count == 0)
                return false;

            var left = joinNode.Arguments[0];
            var right = joinNode.Arguments[1];
            var on = joinNode.Arguments[2];
            var type = (JoinType)((ConstantExpression)joinNode.Arguments[3]).Value!;
            string onCol = on is ConstantExpression onConst && onConst.Value is string s ? s : string.Empty;

            if (!PlanSchemaResolver.TryResolveColumns(left, out var leftCols) ||
                !PlanSchemaResolver.TryResolveColumns(right, out var rightCols))
            {
                return false;
            }

            var leftSet = new HashSet<string>(leftCols, StringComparer.Ordinal);
            var rightSet = new HashSet<string>(rightCols, StringComparer.Ordinal);

            // Left-side pushdown: in DataFrame.Join, all left columns preserve their exact names in the join output
            // (colliding right columns get renamed with "_right" or dropped if equal to onCol).
            if ((type == JoinType.Inner || type == JoinType.Left || type == JoinType.Semi || type == JoinType.Anti)
                && predCols.All(c => leftSet.Contains(c)))
            {
                var newLeft = Expression.Call(null, filterNode.Method, left, predicateArg);
                result = Expression.Call(null, joinNode.Method, Visit(newLeft), right, on, joinNode.Arguments[3]);
                return true;
            }

            // Right-side pushdown: valid ONLY for Inner join, and only when every referenced column unambiguously refers
            // to a right column (either the shared join key onCol or a right column not shadowed by leftSet).
            if (type == JoinType.Inner
                && predCols.All(c => (c == onCol && rightSet.Contains(c)) || (rightSet.Contains(c) && !leftSet.Contains(c))))
            {
                var newRight = Expression.Call(null, filterNode.Method, right, predicateArg);
                result = Expression.Call(null, joinNode.Method, left, Visit(newRight), on, joinNode.Arguments[3]);
                return true;
            }

            return false;
        }
    }

    internal static class OptimizerAnalysis
    {
        private static readonly HashSet<string> NonElementwiseExprMethods = new(StringComparer.Ordinal)
        {
            // Aggregations / Scalar reductions
            "SumOp", "MeanOp", "MinOp", "MaxOp", "StdOp", "VarOp", "MedianOp",
            "CountOp", "NUniqueOp", "QuantileOp", "FirstOp", "LastOp",
            "AllOp", "AnyOp", "NullCountOp", "ArgMinOp", "ArgMaxOp",
            "DotOp", "EntropyOp", "ApproxNUniqueOp", "ListOp",

            // Window / Rolling / Cumulative / Cross-row / Positional ops
            "OverOp",
            "RollingMeanOp", "RollingSumOp", "RollingMinOp", "RollingMaxOp", "RollingStdOp",
            "EWMMeanOp", "EWMStdOp",
            "ExpandingMeanOp", "ExpandingSumOp", "ExpandingMinOp", "ExpandingMaxOp", "ExpandingStdOp",
            "CumCountOp", "CumProdOp",
            "ShiftOp", "DiffOp", "PctChangeOp", "RankOp",
            "IsDuplicatedOp", "IsUniqueOp", "IsFirstOp",
            "FillNullOp", "IsInExprOp", "SearchSortedOp",

            // Length-changing ops
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
            columns = null!;
            if (plan is not MethodCallExpression mce)
                return false;

            switch (mce.Method.Name)
            {
                case nameof(LazyFrame.DataFrameOp):
                {
                    if (mce.Arguments[0] is ConstantExpression ce && ce.Value is DataFrame df)
                    {
                        columns = df.Columns.Select(c => c.Name).ToList();
                        return true;
                    }
                    return false;
                }

                case nameof(LazyFrame.ScanCsvOp):
                {
                    if (mce.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var projCols) && projCols.Length > 0)
                    {
                        columns = projCols.ToList();
                        return true;
                    }
                    if (mce.Arguments[0] is ConstantExpression ce && ce.Value is string path && System.IO.File.Exists(path))
                    {
                        try
                        {
                            columns = IO.CsvReader.PeekHeaders(path).ToList();
                            return columns.Count > 0;
                        }
                        catch { return false; }
                    }
                    return false;
                }

                case nameof(LazyFrame.ScanParquetOp):
                {
                    if (mce.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var projCols) && projCols.Length > 0)
                    {
                        columns = projCols.ToList();
                        return true;
                    }
                    if (mce.Arguments[0] is ConstantExpression ce && ce.Value is string path && System.IO.File.Exists(path))
                    {
                        try
                        {
                            columns = IO.ParquetReader.PeekHeaders(path).ToList();
                            return columns.Count > 0;
                        }
                        catch { return false; }
                    }
                    return false;
                }

                case nameof(LazyFrame.ScanJsonOp):
                {
                    if (mce.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var projCols) && projCols.Length > 0)
                    {
                        columns = projCols.ToList();
                        return true;
                    }
                    if (mce.Arguments[0] is ConstantExpression ce && ce.Value is string path && System.IO.File.Exists(path))
                    {
                        try
                        {
                            columns = IO.JsonReader.PeekHeaders(path).ToList();
                            return columns.Count > 0;
                        }
                        catch { return false; }
                    }
                    return false;
                }

                case nameof(LazyFrame.FilterOp):
                case nameof(LazyFrame.LimitOp):
                case nameof(LazyFrame.FetchOp):
                case nameof(LazyFrame.TailOp):
                case nameof(LazyFrame.SliceOp):
                case nameof(LazyFrame.SortOp):
                case nameof(LazyFrame.TopKOp):
                case nameof(LazyFrame.UniqueOp):
                case nameof(LazyFrame.DropNullsOp):
                case nameof(LazyFrame.FillNanOp):
                case nameof(LazyFrame.DelayOp):
                case nameof(LazyFrame.ShiftColumnsOp):
                case nameof(LazyFrame.ClearOp):
                case nameof(LazyFrame.ShrinkToFitOp):
                case nameof(LazyFrame.RechunkOp):
                case nameof(LazyFrame.ExplodeOp):
                    return TryResolveColumns(mce.Arguments[0], out columns);

                case nameof(LazyFrame.SelectOp):
                {
                    if (!OptimizerAnalysis.TryExtractExprArray(mce.Arguments[1], out var exprs))
                        return false;

                    var outCols = new List<string>(exprs.Length);
                    for (int i = 0; i < exprs.Length; i++)
                    {
                        if (!OptimizerAnalysis.TryInferExprOutputName(exprs[i].Expression, out string? outName) || outName == null)
                            return false;
                        outCols.Add(outName);
                    }
                    columns = outCols;
                    return true;
                }

                case nameof(LazyFrame.WithColumnsOp):
                {
                    if (!TryResolveColumns(mce.Arguments[0], out var baseCols))
                        return false;
                    if (!OptimizerAnalysis.TryExtractExprArray(mce.Arguments[1], out var exprs))
                        return false;

                    var outCols = new List<string>(baseCols);
                    for (int i = 0; i < exprs.Length; i++)
                    {
                        if (!OptimizerAnalysis.TryInferExprOutputName(exprs[i].Expression, out string? outName) || outName == null)
                            return false;
                        int existing = outCols.IndexOf(outName);
                        if (existing < 0) outCols.Add(outName);
                    }
                    columns = outCols;
                    return true;
                }

                case nameof(LazyFrame.RenameOp):
                {
                    if (!TryResolveColumns(mce.Arguments[0], out var baseCols))
                        return false;
                    if (mce.Arguments[1] is ConstantExpression ce && ce.Value is Dictionary<string, string> map)
                    {
                        columns = baseCols.Select(c => map.TryGetValue(c, out var renamed) ? renamed : c).ToList();
                        return true;
                    }
                    return false;
                }

                case nameof(LazyFrame.WithRowIndexOp):
                {
                    if (!TryResolveColumns(mce.Arguments[0], out var baseCols))
                        return false;
                    if (mce.Arguments[1] is ConstantExpression ce && ce.Value is string idxName)
                    {
                        var outCols = new List<string>(baseCols.Count + 1) { idxName };
                        outCols.AddRange(baseCols);
                        columns = outCols;
                        return true;
                    }
                    return false;
                }

                case nameof(LazyFrame.JoinOp):
                {
                    if (!TryResolveColumns(mce.Arguments[0], out var leftCols) ||
                        !TryResolveColumns(mce.Arguments[1], out var rightCols))
                        return false;

                    string onCol = mce.Arguments[2] is ConstantExpression onCe && onCe.Value is string s ? s : string.Empty;
                    var joinType = (JoinType)((ConstantExpression)mce.Arguments[3]).Value!;

                    if (joinType == JoinType.Semi || joinType == JoinType.Anti)
                    {
                        columns = new List<string>(leftCols);
                        return true;
                    }

                    var outCols = new List<string>(leftCols);
                    var leftSet = new HashSet<string>(leftCols, StringComparer.Ordinal);
                    foreach (var rc in rightCols)
                    {
                        if (joinType != JoinType.Cross && joinType != JoinType.Outer && rc == onCol)
                            continue;
                        string outName = leftSet.Contains(rc) ? rc + "_right" : rc;
                        outCols.Add(outName);
                    }
                    columns = outCols;
                    return true;
                }

                default:
                    return false;
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

                case nameof(LazyFrame.JoinOp):
                {
                    var joinType = (JoinType)((ConstantExpression)node.Arguments[3]).Value!;
                    if (joinType != JoinType.Cross && node.Arguments[2] is ConstantExpression onCe && onCe.Value is string onCol)
                    {
                        Columns.Add(onCol);
                    }
                    break;
                }

                case nameof(LazyFrame.SelectOp):
                    HasGlobalSelect = true;
                    break;

                case nameof(LazyFrame.SortOp):
                case nameof(LazyFrame.TopKOp):
                case nameof(LazyFrame.GroupByOp):
                case nameof(LazyFrame.AggGroupsOp):
                case nameof(LazyFrame.UnnestOp):
                    if (OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var usedCols))
                    {
                        foreach (var c in usedCols) Columns.Add(c);
                    }
                    else
                    {
                        RequiresAllColumns = true;
                    }
                    break;

                case nameof(LazyFrame.GroupByDynamicOp):
                case nameof(LazyFrame.GroupByRollingOp):
                case nameof(LazyFrame.ExplodeOp):
                    if (node.Arguments[1] is ConstantExpression singleColCe && singleColCe.Value is string singleCol)
                    {
                        Columns.Add(singleCol);
                    }
                    else
                    {
                        RequiresAllColumns = true;
                    }
                    break;

                case nameof(LazyFrame.UnpivotOp):
                    if (OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var unpivotOn)
                        && OptimizerAnalysis.TryExtractStringArray(node.Arguments[2], out var unpivotIdx))
                    {
                        foreach (var c in unpivotOn) Columns.Add(c);
                        foreach (var c in unpivotIdx) Columns.Add(c);
                    }
                    else
                    {
                        RequiresAllColumns = true;
                    }
                    break;

                case nameof(LazyFrame.PivotOp):
                    if (OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var pivIdx)
                        && OptimizerAnalysis.TryExtractStringArray(node.Arguments[2], out var pivCol)
                        && OptimizerAnalysis.TryExtractStringArray(node.Arguments[3], out var pivVal))
                    {
                        foreach (var c in pivIdx) Columns.Add(c);
                        foreach (var c in pivCol) Columns.Add(c);
                        foreach (var c in pivVal) Columns.Add(c);
                    }
                    else
                    {
                        RequiresAllColumns = true;
                    }
                    break;

                case nameof(LazyFrame.DropNullsOp):
                case nameof(LazyFrame.UniqueOp):
                    // Note: Expr.UniqueOp / Expr.DropNullsOp take 1 Expr argument; LazyFrame.UniqueOp / LazyFrame.DropNullsOp take LazyFrame as Arguments[0].
                    if (node.Method.DeclaringType == typeof(LazyFrame))
                    {
                        if (node.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var subsetCols) && subsetCols.Length > 0)
                        {
                            foreach (var c in subsetCols) Columns.Add(c);
                        }
                        else
                        {
                            // Null/empty subset deduplicates or drops nulls across ALL columns!
                            RequiresAllColumns = true;
                        }
                    }
                    break;

                case nameof(LazyFrame.RenameOp):
                case nameof(LazyFrame.TransposeOp):
                case nameof(LazyFrame.NullCountOp):
                case nameof(LazyFrame.ShiftColumnsOp):
                case nameof(LazyFrame.FillNanOp):
                case nameof(LazyFrame.MapOp):
                    if (node.Method.DeclaringType == typeof(LazyFrame))
                    {
                        RequiresAllColumns = true;
                    }
                    break;
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

    internal class ProjectionPushdownVisitor : ExpressionVisitor
    {
        private readonly HashSet<string> _usedColumns;
        public ProjectionPushdownVisitor(IEnumerable<string> columns)
        {
            _usedColumns = new HashSet<string>(columns, StringComparer.Ordinal);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(LazyFrame.ScanCsvOp))
            {
                var filePath = (string)((ConstantExpression)node.Arguments[0]).Value!;
                try
                {
                    string[] sourceCols = OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var existing) && existing.Length > 0
                        ? existing
                        : IO.CsvReader.PeekHeaders(filePath);
                    var validColumns = sourceCols.Where(h => _usedColumns.Contains(h)).ToArray();
                    if (validColumns.Length > 0)
                    {
                        return Expression.Call(null, node.Method, node.Arguments[0], Expression.Constant(validColumns, typeof(string[])), node.Arguments[2]);
                    }
                    return node;
                }
                catch
                {
                    return node;
                }
            }
            else if (node.Method.Name == nameof(LazyFrame.ScanParquetOp))
            {
                var filePath = (string)((ConstantExpression)node.Arguments[0]).Value!;
                try
                {
                    string[] sourceCols = OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var existing) && existing.Length > 0
                        ? existing
                        : IO.ParquetReader.PeekHeaders(filePath);
                    var validColumns = sourceCols.Where(h => _usedColumns.Contains(h)).ToArray();
                    if (validColumns.Length > 0)
                    {
                        return Expression.Call(null, node.Method, node.Arguments[0], Expression.Constant(validColumns, typeof(string[])));
                    }
                    return node;
                }
                catch
                {
                    return node;
                }
            }
            else if (node.Method.Name == nameof(LazyFrame.ScanJsonOp))
            {
                var filePath = (string)((ConstantExpression)node.Arguments[0]).Value!;
                try
                {
                    string[] sourceCols = OptimizerAnalysis.TryExtractStringArray(node.Arguments[1], out var existing) && existing.Length > 0
                        ? existing
                        : IO.JsonReader.PeekHeaders(filePath);
                    var validColumns = sourceCols.Where(h => _usedColumns.Contains(h)).ToArray();
                    if (validColumns.Length > 0)
                    {
                        return Expression.Call(null, node.Method, node.Arguments[0], Expression.Constant(validColumns, typeof(string[])));
                    }
                    return node;
                }
                catch
                {
                    return node;
                }
            }
            return base.VisitMethodCall(node);
        }
    }

    /// <summary>
    /// Applies sound associative inner-join reordering while strictly preserving output column names and order.
    /// </summary>
    internal class JoinReorderingVisitor : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == nameof(LazyFrame.JoinOp))
            {
                var left = Visit(node.Arguments[0]);
                var right = Visit(node.Arguments[1]);
                var on = node.Arguments[2];
                var typeExpr = node.Arguments[3];
                var outerType = (JoinType)((ConstantExpression)typeExpr).Value!;

                if (outerType == JoinType.Inner
                    && left is MethodCallExpression leftMethodCall
                    && leftMethodCall.Method.Name == nameof(LazyFrame.JoinOp))
                {
                    var innerTypeExpr = leftMethodCall.Arguments[3];
                    var innerType = (JoinType)((ConstantExpression)innerTypeExpr).Value!;
                    string outerKey = on is ConstantExpression outerOnCe && outerOnCe.Value is string ok ? ok : string.Empty;
                    string innerKey = leftMethodCall.Arguments[2] is ConstantExpression innerOnCe && innerOnCe.Value is string ik ? ik : string.Empty;

                    if (innerType == JoinType.Inner
                        && !string.IsNullOrEmpty(outerKey)
                        && string.Equals(outerKey, innerKey, StringComparison.Ordinal))
                    {
                        var a = leftMethodCall.Arguments[0];
                        var b = leftMethodCall.Arguments[1];
                        var innerOn = leftMethodCall.Arguments[2];

                        var candidateInnerJoin = Expression.Call(null, node.Method, b, right, innerOn, innerTypeExpr);
                        var candidateInnerVisited = Visit(candidateInnerJoin);
                        var candidatePlan = Expression.Call(null, node.Method, a, candidateInnerVisited, on, typeExpr);

                        var originalPlan = Expression.Call(null, node.Method, left, right, on, typeExpr);

                        // Verify output column names and order are strictly preserved before accepting associative rewrite.
                        if (SchemasMatchOrUnresolved(originalPlan, candidatePlan))
                        {
                            return candidatePlan;
                        }
                    }
                }

                return Expression.Call(null, node.Method, left, right, on, typeExpr);
            }

            return base.VisitMethodCall(node);
        }

        private static bool SchemasMatchOrUnresolved(Expression originalPlan, Expression candidatePlan)
        {
            if (PlanSchemaResolver.TryResolveColumns(originalPlan, out var origCols)
                && PlanSchemaResolver.TryResolveColumns(candidatePlan, out var candCols))
            {
                return origCols.SequenceEqual(candCols, StringComparer.Ordinal);
            }
            return true;
        }
    }
}
