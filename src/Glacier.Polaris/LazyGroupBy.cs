using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections.Generic;

namespace Glacier.Polaris
{
    public sealed class LazyGroupBy
    {
        private readonly Expression _plan;
        private readonly string[] _columns;

        internal LazyGroupBy(Expression plan, string[] columns)
        {
            _plan = plan;
            _columns = columns;
        }

        public LazyFrame Agg(params Expr[] aggregations)
        {
            var method = typeof(LazyFrame).GetMethod("GroupByAggOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var columnsExpr = Expression.Constant(_columns);
            var aggExprs = Expression.NewArrayInit(typeof(Expr), aggregations.Select(a => Expression.Constant(a, typeof(Expr))));
            var methodCall = Expression.Call(null, method, _plan, columnsExpr, aggExprs);
            return new LazyFrame(methodCall);
        }
        /// <summary>
        /// Returns a LazyFrame that, when collected, produces group keys and a "groups" column with row indices.
        /// Polars API: agg_groups()
        /// </summary>
        public LazyFrame AggGroups()
        {
            var method = typeof(LazyFrame).GetMethod("AggGroupsOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
            var columnsExpr = Expression.Constant(_columns);
            var methodCall = Expression.Call(null, method, _plan, columnsExpr);
            return new LazyFrame(methodCall);
        }
    }
}
