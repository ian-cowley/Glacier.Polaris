using System;
using System.Linq.Expressions;

namespace Glacier.Polaris
{
    public static class CardinalityEstimator
    {
        public static long Estimate(Expression plan)
        {
            return Estimate(LogicalPlanNode.FromLegacyExpression(plan));
        }

        internal static long Estimate(LogicalPlanNode plan)
        {
            switch (plan)
            {
                case DataFramePlanNode dfNode:
                    return dfNode.DataFrame.RowCount;

                case FilterPlanNode filterNode:
                    return (long)(Estimate(filterNode.Source) * 0.33);

                case LimitPlanNode limitNode:
                    return Math.Min(Estimate(limitNode.Source), limitNode.N);

                case FetchPlanNode fetchNode:
                    return Math.Min(Estimate(fetchNode.Source), fetchNode.N);

                case JoinPlanNode joinNode:
                {
                    long leftEst = Estimate(joinNode.Left);
                    long rightEst = Estimate(joinNode.Right);
                    return joinNode.JoinType switch
                    {
                        JoinType.Inner => Math.Min(leftEst, rightEst),
                        JoinType.Left => leftEst,
                        JoinType.Outer => Math.Max(leftEst, rightEst),
                        JoinType.Cross => leftEst * rightEst,
                        _ => leftEst
                    };
                }

                case SelectPlanNode selNode:
                    return Estimate(selNode.Source);
                case GroupByPlanNode gbNode:
                    return Estimate(gbNode.Source);
                case AggPlanNode aggNode:
                    return Estimate(aggNode.Source);
                case WithColumnsPlanNode wcNode:
                    return Estimate(wcNode.Source);
                case SortPlanNode sortNode:
                    return Estimate(sortNode.Source);
                case TopKPlanNode topKNode:
                    return Math.Min(Estimate(topKNode.Source), topKNode.K);

                case ScanCsvPlanNode:
                case ScanParquetPlanNode:
                case ScanJsonPlanNode:
                    return 1000000;

                default:
                    return 1000;
            }
        }
    }
}
