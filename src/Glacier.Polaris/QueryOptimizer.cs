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
        public Dictionary<string, HashSet<string>> NestedFieldPaths { get; private set; } = new(StringComparer.OrdinalIgnoreCase);

        public Expression Optimize(Expression plan)
        {
            // 1. Analyze: Find all columns used in the entire query
            var tracker = new ColumnTrackerVisitor();
            tracker.Visit(plan);
            NestedFieldPaths = tracker.NestedFieldPaths;

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
            // else if (visitedNode.Method.Name == nameof(LazyFrame.SelectOp))
            // {
            //     var source = visitedNode.Arguments[0];
            //     var selections = visitedNode.Arguments[1];
            // 
            //     if (source is MethodCallExpression sourceCall && sourceCall.Method.Name == nameof(LazyFrame.SelectOp))
            //     {
            //         // Merge consecutive Selects: Select(Select(src, inner), outer) -> Select(src, outer)
            //         // Note: This is safe if 'outer' doesn't depend on aliases created in 'inner' 
            //         // that are not in 'src'. For this MVP, we assume simple column references.
            //         return Expression.Call(null, visitedNode.Method, sourceCall.Arguments[0], selections);
            //     }
            // }

            return visitedNode;
        }
    }

    internal class ColumnTrackerVisitor : ExpressionVisitor
    {
        public HashSet<string> Columns { get; } = new HashSet<string>();
        public Dictionary<string, HashSet<string>> NestedFieldPaths { get; } = new(StringComparer.OrdinalIgnoreCase);
        public bool HasGlobalSelect { get; private set; } = false;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name == "Col")
            {
                Columns.Add((string)((ConstantExpression)node.Arguments[0]).Value!);
            }
            else if (node.Method.Name == "Struct_FieldOp")
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


