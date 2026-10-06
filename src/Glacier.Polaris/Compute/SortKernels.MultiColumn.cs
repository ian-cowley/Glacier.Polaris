using System;
using System.Collections.Generic;
using System.Linq;

namespace Glacier.Polaris.Compute
{
    public static partial class SortKernels
    {
        /// <summary>
        /// Multi-column sort via stable chained radix sorts in reverse order.
        /// Uses ArgSort for each column in reverse order (stable sort by last column first).
        /// </summary>
        public static int[] MultiColumnSort(DataFrame df, string[] columnNames, bool[] descending)
        {
            if (columnNames.Length == 0 || df.Columns.Count == 0) return Array.Empty<int>();
            int rowCount = df.Columns[0].Length;
            int[] indices = new int[rowCount];
            for (int i = 0; i < rowCount; i++) indices[i] = i;

            for (int i = columnNames.Length - 1; i >= 0; i--)
            {
                var col = df.GetColumn(columnNames[i]);
                bool desc = descending.Length > i && descending[i];
                if (col.ValidityMask.HasNulls)
                {
                    indices = desc
                        ? indices.OrderBy(idx => col.ValidityMask.IsNull(idx) ? 1 : 0)
                                 .ThenByDescending(idx => col.Get(idx) as IComparable)
                                 .ToArray()
                        : indices.OrderBy(idx => col.ValidityMask.IsNull(idx) ? 1 : 0)
                                 .ThenBy(idx => col.Get(idx) as IComparable)
                                 .ToArray();
                }
                else
                {
                    if (col is Data.Int32Series intCol)
                        ArgSort(intCol.Memory.Span, indices, desc);
                    else if (col is Data.Float64Series doubleCol)
                        ArgSort(doubleCol.Memory.Span, indices, desc);
                    else if (col is Data.Utf8StringSeries u8)
                    {
                        var comparer = new Utf8IndexComparer(u8);
                        indices = desc
                            ? indices.OrderByDescending(idx => idx, comparer).ToArray()
                            : indices.OrderBy(idx => idx, comparer).ToArray();
                    }
                    else
                    {
                        indices = desc
                            ? indices.OrderByDescending(idx => col.Get(idx) as IComparable).ToArray()
                            : indices.OrderBy(idx => col.Get(idx) as IComparable).ToArray();
                    }
                }
            }
            return indices;
        }

        public static int[] TopK(DataFrame df, string[] columnNames, bool[] descending, int k)
        {
            if (k <= 0) return Array.Empty<int>();
            int rowCount = df.Columns[0].Length;
            if (k >= rowCount) return MultiColumnSort(df, columnNames, descending);
            var indices = MultiColumnSort(df, columnNames, descending);
            var result = new int[k];
            Array.Copy(indices, result, k);
            return result;
        }

        private class Utf8IndexComparer : IComparer<int>
        {
            private readonly Data.Utf8StringSeries _series;
            public Utf8IndexComparer(Data.Utf8StringSeries series) => _series = series;
            public int Compare(int x, int y)
            {
                var spanX = _series.GetStringSpan(x);
                var spanY = _series.GetStringSpan(y);
                return spanX.SequenceCompareTo(spanY);
            }
        }
    }
}
