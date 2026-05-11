using System;
using System.Linq;
using System.Linq.Expressions;
using Glacier.Polaris.Data;

namespace Glacier.Polaris
{
    public static class CardinalityEstimator
    {
        public static long Estimate(Expression plan)
        {
            if (plan is MethodCallExpression mce)
            {
                switch (mce.Method.Name)
                {
                    case "DataFrameOp":
                        var df = (DataFrame)((ConstantExpression)mce.Arguments[0]).Value!;
                        return df.RowCount;

                    case "FilterOp":
                        // Naive selectivity: assume filter reduces rows by 2/3
                        return (long)(Estimate(mce.Arguments[0]) * 0.33);

                    case "LimitOp":
                        long sourceEst = Estimate(mce.Arguments[0]);
                        int limit = (int)((ConstantExpression)mce.Arguments[1]).Value!;
                        return Math.Min(sourceEst, limit);

                    case "JoinOp":
                        long leftEst = Estimate(mce.Arguments[0]);
                        long rightEst = Estimate(mce.Arguments[1]);
                        var type = (JoinType)((ConstantExpression)mce.Arguments[3]).Value!;
                        
                        if (type == JoinType.Inner) return Math.Min(leftEst, rightEst);
                        if (type == JoinType.Left) return leftEst;
                        if (type == JoinType.Outer) return Math.Max(leftEst, rightEst);
                        if (type == JoinType.Cross) return leftEst * rightEst;
                        return leftEst;

                    case "SelectOp":
                    case "GroupByOp":
                    case "AggOp":
                    case "WithColumnsOp":
                    case "SortOp":
                        return Estimate(mce.Arguments[0]);

                    case "ScanCsvOp":
                    case "ScanParquetOp":
                        // Default large estimate for scans if unknown
                        return 1000000;
                }
            }
            return 1000; // Default fallback
        }
    }
}
