using System;
using System.Linq.Expressions;

namespace Glacier.Polaris
{
    public sealed class LazyGroupBy
    {
        private readonly LogicalPlanNode _plan;
        private readonly string[] _columns;

        internal LazyGroupBy(Expression plan, string[] columns)
        {
            _plan = LogicalPlanNode.FromLegacyExpression(plan);
            _columns = columns;
        }

        internal LazyGroupBy(LogicalPlanNode plan, string[] columns)
        {
            _plan = plan;
            _columns = columns;
        }

        public LazyFrame Agg(params Expr[] aggregations)
        {
            return new LazyFrame(new AggPlanNode(_plan, _columns, aggregations));
        }

        /// <summary>
        /// Returns a LazyFrame that, when collected, produces group keys and a "groups" column with row indices.
        /// Polars API: agg_groups()
        /// </summary>
        public LazyFrame AggGroups()
        {
            return new LazyFrame(new AggGroupsPlanNode(_plan, _columns));
        }
    }
}
