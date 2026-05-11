using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class PivotKernels
    {
        public static DataFrame Pivot(DataFrame df, string[] index, string pivot, string values, string agg = "sum")
        {
            var indexCols = index.Select(i => df.GetColumn(i)).ToArray();
            var pivotCol = df.GetColumn(pivot) as Utf8StringSeries ?? throw new ArgumentException("Pivot must be Utf8String");
            var valueCol = df.GetColumn(values);

            // 1. Find unique pivot values
            var uniquePivots = new List<string>();
            var pivotSet = new HashSet<string>();
            for (int i = 0; i < pivotCol.Length; i++)
            {
                var s = System.Text.Encoding.UTF8.GetString(pivotCol.GetStringSpan(i));
                if (pivotSet.Add(s)) uniquePivots.Add(s);
            }
            // Preserve order of appearance (matches Python Polars behavior)

            // 2. Group by multiple index columns
            var groups = GroupByKernels.GroupBy(indexCols);

            // 3. Create result columns
            var resultIndexCols = new List<ISeries>();
            int[] firstIndices = groups.Select(g => g[0]).ToArray();
            foreach (var col in indexCols)
            {
                resultIndexCols.Add(ComputeKernels.Take(col, firstIndices));
            }

            // Create pivot columns
            var pivotResultCols = uniquePivots.Select(p => CreateResultColumn(p, agg, groups.Count, valueCol)).ToArray();
            var pivotMap = uniquePivots.Select((p, i) => (p, i)).ToDictionary(x => x.p, x => x.i);

            // Count tracking for mean aggregation
            int[,]? countTracker = agg == "mean" ? new int[groups.Count, uniquePivots.Count] : null;

            // 4. Distribute and Aggregate
            for (int rowIdx = 0; rowIdx < groups.Count; rowIdx++)
            {
                var group = groups[rowIdx];
                foreach (int dataIdx in group)
                {
                    var pVal = System.Text.Encoding.UTF8.GetString(pivotCol.GetStringSpan(dataIdx));
                    if (pivotMap.TryGetValue(pVal, out int colIdx))
                    {
                        ApplyAggregation(pivotResultCols[colIdx], rowIdx, valueCol, dataIdx, agg, countTracker, rowIdx, colIdx);
                    }
                }
            }

            // If aggregation is mean, divide by count for each cell
            if (agg == "mean" && countTracker != null)
            {
                for (int r = 0; r < groups.Count; r++)
                {
                    for (int c = 0; c < uniquePivots.Count; c++)
                    {
                        if (countTracker[r, c] > 0 && pivotResultCols[c] is Float64Series f64)
                        {
                            f64.Memory.Span[r] /= countTracker[r, c];
                        }
                    }
                }
            }

            var allCols = new List<ISeries>(resultIndexCols);
            allCols.AddRange(pivotResultCols);
            return new DataFrame(allCols);
        }

        private static ISeries CreateResultColumn(string name, string agg, int length, ISeries sourceCol)
        {
            if (agg == "count") return new Int32Series(name, length);
            if (sourceCol is Float64Series || agg == "mean") return new Float64Series(name, length);
            return new Int32Series(name, length);
        }
        private static void ApplyAggregation(ISeries dest, int destIdx, ISeries source, int sourceIdx, string agg, int[,]? countTracker = null, int rowIdx = 0, int colIdx = 0)
        {
            // Skip null source values
            if (source.ValidityMask.IsNull(sourceIdx)) return;

            if (agg == "sum")
            {
                if (dest is Int32Series d32 && source is Int32Series s32)
                {
                    d32.Memory.Span[destIdx] += s32.Memory.Span[sourceIdx];
                }
                else if (dest is Float64Series df64)
                {
                    double val = (source is Float64Series sf64) ? sf64.Memory.Span[sourceIdx] : ((Int32Series)source).Memory.Span[sourceIdx];
                    df64.Memory.Span[destIdx] += val;
                }
                dest.ValidityMask.SetValid(destIdx);
            }
            else if (agg == "count")
            {
                if (dest is Int32Series d32)
                {
                    d32.Memory.Span[destIdx]++;
                    d32.ValidityMask.SetValid(destIdx);
                }
            }
            else if (agg == "mean")
            {
                if (dest is Float64Series df64)
                {
                    double val = (source is Float64Series sf64) ? sf64.Memory.Span[sourceIdx] : ((Int32Series)source).Memory.Span[sourceIdx];
                    df64.Memory.Span[destIdx] += val;
                    df64.ValidityMask.SetValid(destIdx);
                    if (countTracker != null) countTracker[rowIdx, colIdx]++;
                }
            }
        }
    }
}
