using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace Glacier.Polaris
{
    /// <summary>
    /// Base type for strongly-typed, AOT-safe logical query plan nodes.
    /// </summary>
    internal abstract record LogicalPlanNode
    {
        private static readonly MethodInfo DataFrameMethod = ((Func<DataFrame, LazyFrame>)LazyFrame.DataFrameOp).Method;
        private static readonly MethodInfo ScanCsvMethod = ((Func<string, string[]?, int?, LazyFrame>)LazyFrame.ScanCsvOp).Method;
        private static readonly MethodInfo ScanParquetMethod = ((Func<string, string[]?, LazyFrame>)LazyFrame.ScanParquetOp).Method;
        private static readonly MethodInfo ScanJsonMethod = ((Func<string, string[]?, LazyFrame>)LazyFrame.ScanJsonOp).Method;
        private static readonly MethodInfo ScanSqlMethod = ((Func<IDbConnection, string, LazyFrame>)LazyFrame.ScanSqlOp).Method;
        private static readonly MethodInfo FilterMethod = ((Func<LazyFrame, Expression<Func<Expr, Expr>>, LazyFrame>)LazyFrame.FilterOp).Method;
        private static readonly MethodInfo SelectMethod = ((Func<LazyFrame, Expr[], LazyFrame>)LazyFrame.SelectOp).Method;
        private static readonly MethodInfo WithColumnsMethod = ((Func<LazyFrame, Expr[], LazyFrame>)LazyFrame.WithColumnsOp).Method;
        private static readonly MethodInfo GroupByMethod = ((Func<LazyFrame, string[], LazyFrame>)LazyFrame.GroupByOp).Method;
        private static readonly MethodInfo AggMethod = ((Func<LazyFrame, Expr[], LazyFrame>)LazyFrame.AggOp).Method;
        private static readonly MethodInfo GroupByAggMethod = ((Func<LazyFrame, string[], Expr[], LazyFrame>)LazyFrame.GroupByAggOp).Method;
        private static readonly MethodInfo AggGroupsMethod = ((Func<LazyFrame, string[], LazyFrame>)LazyFrame.AggGroupsOp).Method;
        private static readonly MethodInfo GroupByDynamicMethod = ((Func<LazyFrame, string, string, string?, string?, string, string, LazyFrame>)LazyFrame.GroupByDynamicOp).Method;
        private static readonly MethodInfo GroupByRollingMethod = ((Func<LazyFrame, string, string, string?, string, LazyFrame>)LazyFrame.GroupByRollingOp).Method;
        private static readonly MethodInfo JoinMethod = ((Func<LazyFrame, LazyFrame, string, JoinType, LazyFrame>)LazyFrame.JoinOp).Method;
        private static readonly MethodInfo JoinMultiMethod = ((Func<LazyFrame, LazyFrame, string[], string[], JoinType, LazyFrame>)LazyFrame.JoinMultiOp).Method;
        private static readonly MethodInfo SortMethod = ((Func<LazyFrame, string[], bool[], LazyFrame>)LazyFrame.SortOp).Method;
        private static readonly MethodInfo TopKMethod = ((Func<LazyFrame, string[], bool[], int, LazyFrame>)LazyFrame.TopKOp).Method;
        private static readonly MethodInfo LimitMethod = ((Func<LazyFrame, int, LazyFrame>)LazyFrame.LimitOp).Method;
        private static readonly MethodInfo FetchMethod = ((Func<LazyFrame, int, LazyFrame>)LazyFrame.FetchOp).Method;
        private static readonly MethodInfo DelayMethod = ((Func<LazyFrame, int, LazyFrame>)LazyFrame.DelayOp).Method;
        private static readonly MethodInfo ExplodeMethod = ((Func<LazyFrame, string, LazyFrame>)LazyFrame.ExplodeOp).Method;
        private static readonly MethodInfo UnnestMethod = ((Func<LazyFrame, string[], LazyFrame>)LazyFrame.UnnestOp).Method;
        private static readonly MethodInfo UnpivotMethod = ((Func<LazyFrame, string[], string[], LazyFrame>)LazyFrame.UnpivotOp).Method;
        private static readonly MethodInfo PivotMethod = ((Func<LazyFrame, string[], string[], string[], string, LazyFrame>)LazyFrame.PivotOp).Method;
        private static readonly MethodInfo TransposeMethod = ((Func<LazyFrame, bool, string, string[]?, LazyFrame>)LazyFrame.TransposeOp).Method;
        private static readonly MethodInfo UniqueMethod = ((Func<LazyFrame, string[]?, bool, LazyFrame>)LazyFrame.UniqueOp).Method;
        private static readonly MethodInfo SliceMethod = ((Func<LazyFrame, int, int, LazyFrame>)LazyFrame.SliceOp).Method;
        private static readonly MethodInfo TailMethod = ((Func<LazyFrame, int, LazyFrame>)LazyFrame.TailOp).Method;
        private static readonly MethodInfo DropNullsMethod = ((Func<LazyFrame, string[]?, bool, LazyFrame>)LazyFrame.DropNullsOp).Method;
        private static readonly MethodInfo FillNanMethod = ((Func<LazyFrame, double, LazyFrame>)LazyFrame.FillNanOp).Method;
        private static readonly MethodInfo WithRowIndexMethod = ((Func<LazyFrame, string, LazyFrame>)LazyFrame.WithRowIndexOp).Method;
        private static readonly MethodInfo RenameMethod = ((Func<LazyFrame, Dictionary<string, string>, LazyFrame>)LazyFrame.RenameOp).Method;
        private static readonly MethodInfo NullCountMethod = ((Func<LazyFrame, LazyFrame>)LazyFrame.NullCountOp).Method;
        private static readonly MethodInfo ShiftColumnsMethod = ((Func<LazyFrame, int, LazyFrame>)LazyFrame.ShiftColumnsOp).Method;
        private static readonly MethodInfo ClearMethod = ((Func<LazyFrame, LazyFrame>)LazyFrame.ClearOp).Method;
        private static readonly MethodInfo ShrinkToFitMethod = ((Func<LazyFrame, LazyFrame>)LazyFrame.ShrinkToFitOp).Method;
        private static readonly MethodInfo RechunkMethod = ((Func<LazyFrame, LazyFrame>)LazyFrame.RechunkOp).Method;
        private static readonly MethodInfo MapMethod = ((Func<LazyFrame, Func<DataFrame, DataFrame>, LazyFrame>)LazyFrame.MapOp).Method;
        private static readonly MethodInfo SinkCsvMethod = ((Func<LazyFrame, string, LazyFrame>)LazyFrame.SinkCsvOp).Method;
        private static readonly MethodInfo SinkParquetMethod = ((Func<LazyFrame, string, LazyFrame>)LazyFrame.SinkParquetOp).Method;
        private static readonly MethodInfo SinkIpcMethod = ((Func<LazyFrame, string, LazyFrame>)LazyFrame.SinkIpcOp).Method;

        internal Expression ToLegacyExpression() => this switch
        {
            DataFramePlanNode n => Expression.Call(null, DataFrameMethod, Expression.Constant(n.DataFrame)),
            ScanCsvPlanNode n => Expression.Call(null, ScanCsvMethod, Expression.Constant(n.FilePath), Expression.Constant(n.Columns, typeof(string[])), Expression.Constant(n.NRows, typeof(int?))),
            ScanParquetPlanNode n => Expression.Call(null, ScanParquetMethod, Expression.Constant(n.FilePath), Expression.Constant(n.Columns, typeof(string[]))),
            ScanJsonPlanNode n => Expression.Call(null, ScanJsonMethod, Expression.Constant(n.FilePath), Expression.Constant(n.Columns, typeof(string[]))),
            ScanSqlPlanNode n => Expression.Call(null, ScanSqlMethod, Expression.Constant(n.Connection, typeof(IDbConnection)), Expression.Constant(n.Sql)),
            FilterPlanNode n => Expression.Call(null, FilterMethod, n.Source.ToLegacyExpression(), Expression.Quote(BuildFilterLambda(n))),
            SelectPlanNode n => Expression.Call(null, SelectMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Selections, typeof(Expr[]))),
            WithColumnsPlanNode n => Expression.Call(null, WithColumnsMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Columns, typeof(Expr[]))),
            GroupByPlanNode n => Expression.Call(null, GroupByMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Columns, typeof(string[]))),
            AggPlanNode n => n.GroupColumns == null
                ? Expression.Call(null, AggMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Aggregations, typeof(Expr[])))
                : Expression.Call(null, GroupByAggMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.GroupColumns, typeof(string[])), Expression.Constant(n.Aggregations, typeof(Expr[]))),
            AggGroupsPlanNode n => Expression.Call(null, AggGroupsMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Columns, typeof(string[]))),
            GroupByDynamicPlanNode n => Expression.Call(null, GroupByDynamicMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.IndexColumn), Expression.Constant(n.Every), Expression.Constant(n.Period, typeof(string)), Expression.Constant(n.Offset, typeof(string)), Expression.Constant(n.Closed), Expression.Constant(n.StartBy)),
            GroupByRollingPlanNode n => Expression.Call(null, GroupByRollingMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.IndexColumn), Expression.Constant(n.Period), Expression.Constant(n.Offset, typeof(string)), Expression.Constant(n.Closed)),
            JoinPlanNode n => BuildJoinExpression(n),
            SortPlanNode n => Expression.Call(null, SortMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.ColumnNames, typeof(string[])), Expression.Constant(n.Descending, typeof(bool[]))),
            TopKPlanNode n => Expression.Call(null, TopKMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.ColumnNames, typeof(string[])), Expression.Constant(n.Descending, typeof(bool[])), Expression.Constant(n.K)),
            LimitPlanNode n => Expression.Call(null, LimitMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.N)),
            FetchPlanNode n => Expression.Call(null, FetchMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.N)),
            DelayPlanNode n => Expression.Call(null, DelayMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Ms)),
            ExplodePlanNode n => Expression.Call(null, ExplodeMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Column)),
            UnnestPlanNode n => Expression.Call(null, UnnestMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Columns, typeof(string[]))),
            UnpivotPlanNode n => Expression.Call(null, UnpivotMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.On, typeof(string[])), Expression.Constant(n.Index, typeof(string[]))),
            PivotPlanNode n => Expression.Call(null, PivotMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Index, typeof(string[])), Expression.Constant(n.Pivot, typeof(string[])), Expression.Constant(n.Values, typeof(string[])), Expression.Constant(n.Aggregate)),
            TransposePlanNode n => Expression.Call(null, TransposeMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.IncludeHeader), Expression.Constant(n.HeaderName), Expression.Constant(n.ColumnNames, typeof(string[]))),
            UniquePlanNode n => Expression.Call(null, UniqueMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Subset, typeof(string[])), Expression.Constant(n.MaintainOrder)),
            SlicePlanNode n => Expression.Call(null, SliceMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Offset), Expression.Constant(n.Length)),
            TailPlanNode n => Expression.Call(null, TailMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.N)),
            DropNullsPlanNode n => Expression.Call(null, DropNullsMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Subset, typeof(string[])), Expression.Constant(n.AnyNull)),
            FillNanPlanNode n => Expression.Call(null, FillNanMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Value)),
            WithRowIndexPlanNode n => Expression.Call(null, WithRowIndexMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Name)),
            RenamePlanNode n => Expression.Call(null, RenameMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Mapping)),
            NullCountPlanNode n => Expression.Call(null, NullCountMethod, n.Source.ToLegacyExpression()),
            ShiftColumnsPlanNode n => Expression.Call(null, ShiftColumnsMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.N)),
            ClearPlanNode n => Expression.Call(null, ClearMethod, n.Source.ToLegacyExpression()),
            ShrinkToFitPlanNode n => Expression.Call(null, ShrinkToFitMethod, n.Source.ToLegacyExpression()),
            RechunkPlanNode n => Expression.Call(null, RechunkMethod, n.Source.ToLegacyExpression()),
            MapPlanNode n => Expression.Call(null, MapMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.Func)),
            SinkCsvPlanNode n => Expression.Call(null, SinkCsvMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.FilePath)),
            SinkParquetPlanNode n => Expression.Call(null, SinkParquetMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.FilePath)),
            SinkIpcPlanNode n => Expression.Call(null, SinkIpcMethod, n.Source.ToLegacyExpression(), Expression.Constant(n.FilePath)),
            ExternalExpressionPlanNode n => n.Expression,
            _ => throw new NotSupportedException($"Unsupported plan node: {GetType().Name}")
        };

        private static Expression<Func<Expr, Expr>> BuildFilterLambda(FilterPlanNode n)
        {
            if (n.RawLambda != null)
            {
                return n.RawLambda;
            }
            var param = Expression.Parameter(typeof(Expr), "e");
            return Expression.Lambda<Func<Expr, Expr>>(Expression.Constant(n.Predicate, typeof(Expr)), param);
        }

        private static MethodCallExpression BuildJoinExpression(JoinPlanNode n)
        {
            if (n.JoinType == JoinType.Cross && n.LeftOn.Length == 0 && n.RightOn.Length == 0)
            {
                return Expression.Call(null, JoinMethod, n.Left.ToLegacyExpression(), n.Right.ToLegacyExpression(), Expression.Constant(""), Expression.Constant(JoinType.Cross));
            }
            if (n.LeftOn.Length == 1 && n.RightOn.Length == 1 && string.Equals(n.LeftOn[0], n.RightOn[0], StringComparison.Ordinal))
            {
                return Expression.Call(null, JoinMethod, n.Left.ToLegacyExpression(), n.Right.ToLegacyExpression(), Expression.Constant(n.LeftOn[0]), Expression.Constant(n.JoinType));
            }
            return Expression.Call(null, JoinMultiMethod, n.Left.ToLegacyExpression(), n.Right.ToLegacyExpression(), Expression.Constant(n.LeftOn, typeof(string[])), Expression.Constant(n.RightOn, typeof(string[])), Expression.Constant(n.JoinType));
        }

        internal static LogicalPlanNode FromLegacyExpression(Expression expr)
        {
            if (expr is not MethodCallExpression mce)
            {
                return new ExternalExpressionPlanNode(expr);
            }

            switch (mce.Method.Name)
            {
                case nameof(LazyFrame.DataFrameOp):
                    return new DataFramePlanNode((DataFrame)((ConstantExpression)mce.Arguments[0]).Value!);

                case nameof(LazyFrame.ScanCsvOp):
                    return new ScanCsvPlanNode(
                        (string)((ConstantExpression)mce.Arguments[0]).Value!,
                        mce.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var csvCols) ? csvCols : null,
                        mce.Arguments.Count > 2 ? (int?)((ConstantExpression)mce.Arguments[2]).Value : null);

                case nameof(LazyFrame.ScanParquetOp):
                    return new ScanParquetPlanNode(
                        (string)((ConstantExpression)mce.Arguments[0]).Value!,
                        mce.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var pqCols) ? pqCols : null);

                case nameof(LazyFrame.ScanJsonOp):
                    return new ScanJsonPlanNode(
                        (string)((ConstantExpression)mce.Arguments[0]).Value!,
                        mce.Arguments.Count > 1 && OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var jCols) ? jCols : null);

                case nameof(LazyFrame.ScanSqlOp):
                    return new ScanSqlPlanNode(
                        (IDbConnection)((ConstantExpression)mce.Arguments[0]).Value!,
                        (string)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.FilterOp):
                {
                    var src = FromLegacyExpression(mce.Arguments[0]);
                    Expression arg = mce.Arguments[1];
                    Expression<Func<Expr, Expr>>? rawLambda = null;
                    Expression unwrapped = arg;
                    while (unwrapped is UnaryExpression ue && ue.NodeType == ExpressionType.Quote)
                        unwrapped = ue.Operand;
                    if (unwrapped is Expression<Func<Expr, Expr>> lam)
                        rawLambda = lam;

                    if (OptimizerAnalysis.TryExtractPredicateExpr(arg, out var predExpr))
                        return new FilterPlanNode(src, predExpr, rawLambda);
                    return new ExternalExpressionPlanNode(expr);
                }

                case nameof(LazyFrame.SelectOp):
                    return OptimizerAnalysis.TryExtractExprArray(mce.Arguments[1], out var selExprs)
                        ? new SelectPlanNode(FromLegacyExpression(mce.Arguments[0]), selExprs)
                        : new ExternalExpressionPlanNode(expr);

                case nameof(LazyFrame.WithColumnsOp):
                    return OptimizerAnalysis.TryExtractExprArray(mce.Arguments[1], out var wcExprs)
                        ? new WithColumnsPlanNode(FromLegacyExpression(mce.Arguments[0]), wcExprs)
                        : new ExternalExpressionPlanNode(expr);

                case nameof(LazyFrame.GroupByOp):
                    return OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var gbCols)
                        ? new GroupByPlanNode(FromLegacyExpression(mce.Arguments[0]), gbCols)
                        : new ExternalExpressionPlanNode(expr);

                case nameof(LazyFrame.AggOp):
                    return OptimizerAnalysis.TryExtractExprArray(mce.Arguments[1], out var aggExprs)
                        ? new AggPlanNode(FromLegacyExpression(mce.Arguments[0]), null, aggExprs)
                        : new ExternalExpressionPlanNode(expr);

                case nameof(LazyFrame.GroupByAggOp):
                    return OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var gbaCols)
                        && OptimizerAnalysis.TryExtractExprArray(mce.Arguments[2], out var gbaExprs)
                        ? new AggPlanNode(FromLegacyExpression(mce.Arguments[0]), gbaCols, gbaExprs)
                        : new ExternalExpressionPlanNode(expr);

                case nameof(LazyFrame.AggGroupsOp):
                    return new AggGroupsPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var agCols) ? agCols : Array.Empty<string>());

                case nameof(LazyFrame.GroupByDynamicOp):
                    return new GroupByDynamicPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string)((ConstantExpression)mce.Arguments[1]).Value!,
                        (string)((ConstantExpression)mce.Arguments[2]).Value!,
                        (string?)((ConstantExpression)mce.Arguments[3]).Value,
                        (string?)((ConstantExpression)mce.Arguments[4]).Value,
                        (string)((ConstantExpression)mce.Arguments[5]).Value!,
                        (string)((ConstantExpression)mce.Arguments[6]).Value!);

                case nameof(LazyFrame.GroupByRollingOp):
                    return new GroupByRollingPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string)((ConstantExpression)mce.Arguments[1]).Value!,
                        (string)((ConstantExpression)mce.Arguments[2]).Value!,
                        (string?)((ConstantExpression)mce.Arguments[3]).Value,
                        (string)((ConstantExpression)mce.Arguments[4]).Value!);

                case nameof(LazyFrame.JoinOp):
                {
                    var on = (string)((ConstantExpression)mce.Arguments[2]).Value!;
                    var jt = (JoinType)((ConstantExpression)mce.Arguments[3]).Value!;
                    var keys = jt == JoinType.Cross && string.IsNullOrEmpty(on) ? Array.Empty<string>() : new[] { on };
                    return new JoinPlanNode(FromLegacyExpression(mce.Arguments[0]), FromLegacyExpression(mce.Arguments[1]), keys, keys, jt);
                }

                case nameof(LazyFrame.JoinMultiOp):
                    return new JoinPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        FromLegacyExpression(mce.Arguments[1]),
                        (string[])((ConstantExpression)mce.Arguments[2]).Value!,
                        (string[])((ConstantExpression)mce.Arguments[3]).Value!,
                        (JoinType)((ConstantExpression)mce.Arguments[4]).Value!);

                case nameof(LazyFrame.SortOp):
                    return new SortPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string[])((ConstantExpression)mce.Arguments[1]).Value!,
                        (bool[])((ConstantExpression)mce.Arguments[2]).Value!);

                case nameof(LazyFrame.TopKOp):
                    return new TopKPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string[])((ConstantExpression)mce.Arguments[1]).Value!,
                        (bool[])((ConstantExpression)mce.Arguments[2]).Value!,
                        (int)((ConstantExpression)mce.Arguments[3]).Value!);

                case nameof(LazyFrame.LimitOp):
                    return new LimitPlanNode(FromLegacyExpression(mce.Arguments[0]), (int)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.FetchOp):
                    return new FetchPlanNode(FromLegacyExpression(mce.Arguments[0]), (int)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.DelayOp):
                    return new DelayPlanNode(FromLegacyExpression(mce.Arguments[0]), (int)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.ExplodeOp):
                    return new ExplodePlanNode(FromLegacyExpression(mce.Arguments[0]), (string)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.UnnestOp):
                    return new UnnestPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        OptimizerAnalysis.TryExtractStringArray(mce.Arguments[1], out var uCols) ? uCols : Array.Empty<string>());

                case nameof(LazyFrame.UnpivotOp):
                    return new UnpivotPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string[])((ConstantExpression)mce.Arguments[1]).Value!,
                        (string[])((ConstantExpression)mce.Arguments[2]).Value!);

                case nameof(LazyFrame.PivotOp):
                    return new PivotPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string[])((ConstantExpression)mce.Arguments[1]).Value!,
                        (string[])((ConstantExpression)mce.Arguments[2]).Value!,
                        (string[])((ConstantExpression)mce.Arguments[3]).Value!,
                        (string)((ConstantExpression)mce.Arguments[4]).Value!);

                case nameof(LazyFrame.TransposeOp):
                    return new TransposePlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (bool)((ConstantExpression)mce.Arguments[1]).Value!,
                        (string)((ConstantExpression)mce.Arguments[2]).Value!,
                        (string[]?)((ConstantExpression)mce.Arguments[3]).Value);

                case nameof(LazyFrame.UniqueOp):
                    return new UniquePlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        mce.Arguments.Count > 1 ? (string[]?)((ConstantExpression)mce.Arguments[1]).Value : null,
                        mce.Arguments.Count > 2 ? (bool)((ConstantExpression)mce.Arguments[2]).Value! : true);

                case nameof(LazyFrame.SliceOp):
                    return new SlicePlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (int)((ConstantExpression)mce.Arguments[1]).Value!,
                        (int)((ConstantExpression)mce.Arguments[2]).Value!);

                case nameof(LazyFrame.TailOp):
                    return new TailPlanNode(FromLegacyExpression(mce.Arguments[0]), (int)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.DropNullsOp):
                    return new DropNullsPlanNode(
                        FromLegacyExpression(mce.Arguments[0]),
                        (string[]?)((ConstantExpression)mce.Arguments[1]).Value,
                        (bool)((ConstantExpression)mce.Arguments[2]).Value!);

                case nameof(LazyFrame.FillNanOp):
                    return new FillNanPlanNode(FromLegacyExpression(mce.Arguments[0]), (double)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.WithRowIndexOp):
                    return new WithRowIndexPlanNode(FromLegacyExpression(mce.Arguments[0]), (string)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.RenameOp):
                    return new RenamePlanNode(FromLegacyExpression(mce.Arguments[0]), (Dictionary<string, string>)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.NullCountOp):
                    return new NullCountPlanNode(FromLegacyExpression(mce.Arguments[0]));

                case nameof(LazyFrame.ShiftColumnsOp):
                    return new ShiftColumnsPlanNode(FromLegacyExpression(mce.Arguments[0]), (int)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.ClearOp):
                    return new ClearPlanNode(FromLegacyExpression(mce.Arguments[0]));

                case nameof(LazyFrame.ShrinkToFitOp):
                    return new ShrinkToFitPlanNode(FromLegacyExpression(mce.Arguments[0]));

                case nameof(LazyFrame.RechunkOp):
                    return new RechunkPlanNode(FromLegacyExpression(mce.Arguments[0]));

                case nameof(LazyFrame.MapOp):
                    return new MapPlanNode(FromLegacyExpression(mce.Arguments[0]), (Func<DataFrame, DataFrame>)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.SinkCsvOp):
                    return new SinkCsvPlanNode(FromLegacyExpression(mce.Arguments[0]), (string)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.SinkParquetOp):
                    return new SinkParquetPlanNode(FromLegacyExpression(mce.Arguments[0]), (string)((ConstantExpression)mce.Arguments[1]).Value!);

                case nameof(LazyFrame.SinkIpcOp):
                    return new SinkIpcPlanNode(FromLegacyExpression(mce.Arguments[0]), (string)((ConstantExpression)mce.Arguments[1]).Value!);

                default:
                    return new ExternalExpressionPlanNode(expr);
            }
        }
    }

    internal sealed record DataFramePlanNode(DataFrame DataFrame) : LogicalPlanNode;
    internal sealed record ScanCsvPlanNode(string FilePath, string[]? Columns = null, int? NRows = null) : LogicalPlanNode;
    internal sealed record ScanParquetPlanNode(string FilePath, string[]? Columns = null) : LogicalPlanNode;
    internal sealed record ScanJsonPlanNode(string FilePath, string[]? Columns = null) : LogicalPlanNode;
    internal sealed record ScanSqlPlanNode(IDbConnection Connection, string Sql) : LogicalPlanNode;
    internal sealed record FilterPlanNode(LogicalPlanNode Source, Expr Predicate, Expression<Func<Expr, Expr>>? RawLambda = null) : LogicalPlanNode;
    internal sealed record SelectPlanNode(LogicalPlanNode Source, Expr[] Selections) : LogicalPlanNode;
    internal sealed record WithColumnsPlanNode(LogicalPlanNode Source, Expr[] Columns) : LogicalPlanNode;
    internal sealed record GroupByPlanNode(LogicalPlanNode Source, string[] Columns) : LogicalPlanNode;
    internal sealed record AggPlanNode(LogicalPlanNode Source, string[]? GroupColumns, Expr[] Aggregations) : LogicalPlanNode;
    internal sealed record AggGroupsPlanNode(LogicalPlanNode Source, string[] Columns) : LogicalPlanNode;
    internal sealed record GroupByDynamicPlanNode(LogicalPlanNode Source, string IndexColumn, string Every, string? Period, string? Offset, string Closed, string StartBy) : LogicalPlanNode;
    internal sealed record GroupByRollingPlanNode(LogicalPlanNode Source, string IndexColumn, string Period, string? Offset, string Closed) : LogicalPlanNode;
    internal sealed record JoinPlanNode(LogicalPlanNode Left, LogicalPlanNode Right, string[] LeftOn, string[] RightOn, JoinType JoinType) : LogicalPlanNode;
    internal sealed record SortPlanNode(LogicalPlanNode Source, string[] ColumnNames, bool[] Descending) : LogicalPlanNode;
    internal sealed record TopKPlanNode(LogicalPlanNode Source, string[] ColumnNames, bool[] Descending, int K) : LogicalPlanNode;
    internal sealed record LimitPlanNode(LogicalPlanNode Source, int N) : LogicalPlanNode;
    internal sealed record FetchPlanNode(LogicalPlanNode Source, int N) : LogicalPlanNode;
    internal sealed record DelayPlanNode(LogicalPlanNode Source, int Ms) : LogicalPlanNode;
    internal sealed record ExplodePlanNode(LogicalPlanNode Source, string Column) : LogicalPlanNode;
    internal sealed record UnnestPlanNode(LogicalPlanNode Source, string[] Columns) : LogicalPlanNode;
    internal sealed record UnpivotPlanNode(LogicalPlanNode Source, string[] On, string[] Index) : LogicalPlanNode;
    internal sealed record PivotPlanNode(LogicalPlanNode Source, string[] Index, string[] Pivot, string[] Values, string Aggregate) : LogicalPlanNode;
    internal sealed record TransposePlanNode(LogicalPlanNode Source, bool IncludeHeader, string HeaderName, string[]? ColumnNames) : LogicalPlanNode;
    internal sealed record UniquePlanNode(LogicalPlanNode Source, string[]? Subset, bool MaintainOrder) : LogicalPlanNode;
    internal sealed record SlicePlanNode(LogicalPlanNode Source, int Offset, int Length) : LogicalPlanNode;
    internal sealed record TailPlanNode(LogicalPlanNode Source, int N) : LogicalPlanNode;
    internal sealed record DropNullsPlanNode(LogicalPlanNode Source, string[]? Subset, bool AnyNull) : LogicalPlanNode;
    internal sealed record FillNanPlanNode(LogicalPlanNode Source, double Value) : LogicalPlanNode;
    internal sealed record WithRowIndexPlanNode(LogicalPlanNode Source, string Name) : LogicalPlanNode;
    internal sealed record RenamePlanNode(LogicalPlanNode Source, Dictionary<string, string> Mapping) : LogicalPlanNode;
    internal sealed record NullCountPlanNode(LogicalPlanNode Source) : LogicalPlanNode;
    internal sealed record ShiftColumnsPlanNode(LogicalPlanNode Source, int N) : LogicalPlanNode;
    internal sealed record ClearPlanNode(LogicalPlanNode Source) : LogicalPlanNode;
    internal sealed record ShrinkToFitPlanNode(LogicalPlanNode Source) : LogicalPlanNode;
    internal sealed record RechunkPlanNode(LogicalPlanNode Source) : LogicalPlanNode;
    internal sealed record MapPlanNode(LogicalPlanNode Source, Func<DataFrame, DataFrame> Func) : LogicalPlanNode;
    internal sealed record SinkCsvPlanNode(LogicalPlanNode Source, string FilePath) : LogicalPlanNode;
    internal sealed record SinkParquetPlanNode(LogicalPlanNode Source, string FilePath) : LogicalPlanNode;
    internal sealed record SinkIpcPlanNode(LogicalPlanNode Source, string FilePath) : LogicalPlanNode;
    internal sealed record ExternalExpressionPlanNode(Expression Expression) : LogicalPlanNode;
}
