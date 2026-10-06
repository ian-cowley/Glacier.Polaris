using System;
using System.Collections.Generic;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class GroupByKernels
    {
        /// <summary>
        /// Generates groups for group_by_dynamic (time-window-based grouping).
        /// Splits the index column into windows of the specified period/offset.
        /// Ported from Glacier.Polaris_OLD.
        /// </summary>
        public static (List<List<int>> Groups, ISeries WindowKeys) GenerateDynamicGroups(
            ISeries indexColumn,
            double every,
            double period,
            double offset,
            string closed = "left",
            string startBy = "window")
        {
            int rowCount = indexColumn.Length;
            if (rowCount == 0)
            {
                return (new List<List<int>>(), CreateResultColumn(indexColumn.Name, "first", 0, indexColumn));
            }

            var vals = new double[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                vals[i] = GetValueAsDouble(indexColumn, i);
            }

            double vMin = vals[0];
            double vMax = vals[rowCount - 1];

            double start;
            if (startBy == "datapoint")
            {
                start = vMin;
            }
            else
            {
                start = Math.Floor(vMin / every) * every;
            }

            var groups = new List<List<int>>();
            var keysList = new List<double>();

            for (double w = start; w <= vMax || Math.Abs(w - vMax) < 1e-9; w += every)
            {
                double startK = w + offset;
                double endK = startK + period;

                int startIdx = FindFirstIndex(vals, startK, closed == "left" || closed == "both");
                int endIdx = FindLastIndex(vals, endK, closed == "right" || closed == "both");

                if (startIdx <= endIdx && startIdx >= 0 && endIdx >= 0)
                {
                    var group = new List<int>();
                    for (int idx = startIdx; idx <= endIdx; idx++)
                    {
                        group.Add(idx);
                    }
                    groups.Add(group);
                    keysList.Add(w);
                }
            }

            ISeries windowKeys;
            if (indexColumn is Int32Series)
            {
                var wk = new Int32Series(indexColumn.Name, keysList.Count);
                for (int i = 0; i < keysList.Count; i++) wk.Memory.Span[i] = (int)keysList[i];
                windowKeys = wk;
            }
            else if (indexColumn is Int64Series)
            {
                var wk = new Int64Series(indexColumn.Name, keysList.Count);
                for (int i = 0; i < keysList.Count; i++) wk.Memory.Span[i] = (long)keysList[i];
                windowKeys = wk;
            }
            else
            {
                var wk = new Float64Series(indexColumn.Name, keysList.Count);
                for (int i = 0; i < keysList.Count; i++) wk.Memory.Span[i] = keysList[i];
                windowKeys = wk;
            }

            return (groups, windowKeys);
        }

        /// <summary>
        /// Generates groups for rolling group-by (sliding-window-based grouping).
        /// Each row becomes a window with the specified period centered/adjusted by offset.
        /// Ported from Glacier.Polaris_OLD.
        /// </summary>
        public static List<List<int>> GenerateRollingGroups(
            ISeries indexColumn,
            double period,
            double offset,
            string closed = "right")
        {
            int rowCount = indexColumn.Length;
            var groups = new List<List<int>>(rowCount);
            if (rowCount == 0) return groups;

            var vals = new double[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                vals[i] = GetValueAsDouble(indexColumn, i);
            }

            int leftPtr = 0;
            int rightPtr = 0;

            for (int r = 0; r < rowCount; r++)
            {
                double valR = vals[r];
                double startK = valR + offset;
                double endK = startK + period;

                bool includeLeft = closed == "left" || closed == "both";
                while (leftPtr < rowCount && (includeLeft ? vals[leftPtr] < startK : vals[leftPtr] <= startK))
                {
                    leftPtr++;
                }

                bool includeRight = closed == "right" || closed == "both";
                if (rightPtr < leftPtr) rightPtr = leftPtr;
                while (rightPtr < rowCount && (includeRight ? vals[rightPtr] <= endK : vals[rightPtr] < endK))
                {
                    rightPtr++;
                }

                var group = new List<int>();
                for (int i = leftPtr; i < rightPtr; i++)
                {
                    group.Add(i);
                }
                groups.Add(group);
            }

            return groups;
        }

        private static int FindFirstIndex(double[] vals, double bound, bool includeEqual)
        {
            int low = 0;
            int high = vals.Length - 1;
            int result = -1;

            while (low <= high)
            {
                int mid = low + (high - low) / 2;
                bool condition = includeEqual ? vals[mid] >= bound : vals[mid] > bound;
                if (condition)
                {
                    result = mid;
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }
            return result;
        }

        private static int FindLastIndex(double[] vals, double bound, bool includeEqual)
        {
            int low = 0;
            int high = vals.Length - 1;
            int result = -1;

            while (low <= high)
            {
                int mid = low + (high - low) / 2;
                bool condition = includeEqual ? vals[mid] <= bound : vals[mid] < bound;
                if (condition)
                {
                    result = mid;
                    low = mid + 1;
                }
                else
                {
                    high = mid - 1;
                }
            }
            return result;
        }
    }
}
