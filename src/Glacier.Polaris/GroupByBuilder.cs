using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris
{
    public sealed class GroupByBuilder
    {
        private readonly DataFrame _source;
        private readonly string[] _columns;
        private readonly List<List<int>> _groups;

        public GroupByBuilder(DataFrame source, params string[] columns)
        {
            _source = source;
            _columns = columns;
            var cols = columns.Select(c => source.GetColumn(c)).ToArray();
            _groups = GroupByKernels.GroupBy(cols);
        }

        public DataFrame Agg(params (string col, string agg)[] aggregations)
        {
            return GroupByKernels.Aggregate(_source, _columns, _groups, aggregations);
        }

        public DataFrame Agg(params Expr[] exprs)
        {
            // Delegate to lazy engine using the actual groupby columns, not all df columns
            return _source.Lazy().GroupBy(_columns).Agg(exprs).Collect().GetAwaiter().GetResult();
        }
        /// <summary>
        /// Returns a DataFrame with the group keys and a "groups" column containing the row indices for each group.
        /// Polars API: agg_groups()
        /// </summary>
        public DataFrame AggGroups()
        {
            var groupKeyCols = new List<ISeries>();
            for (int ci = 0; ci < _columns.Length; ci++)
            {
                var colName = _columns[ci];
                var src = _source.GetColumn(colName);
                ISeries keyCol;
                if (src is Data.Int32Series i32)
                {
                    keyCol = new Data.Int32Series(colName, _groups.Count);
                    var span = ((Data.Int32Series)keyCol).Memory.Span;
                    for (int gi = 0; gi < _groups.Count; gi++)
                        span[gi] = i32.Memory.Span[_groups[gi][0]];
                }
                else if (src is Data.Float64Series f64)
                {
                    keyCol = new Data.Float64Series(colName, _groups.Count);
                    var span = ((Data.Float64Series)keyCol).Memory.Span;
                    for (int gi = 0; gi < _groups.Count; gi++)
                        span[gi] = f64.Memory.Span[_groups[gi][0]];
                }
                else if (src is Data.Utf8StringSeries u8)
                {
                    var strings = new string[_groups.Count];
                    for (int gi = 0; gi < _groups.Count; gi++)
                        strings[gi] = u8.GetString(_groups[gi][0]) ?? "";
                    keyCol = new Data.Utf8StringSeries(colName, strings);
                }
                else if (src is Data.Int64Series i64)
                {
                    keyCol = new Data.Int64Series(colName, _groups.Count);
                    var span = ((Data.Int64Series)keyCol).Memory.Span;
                    for (int gi = 0; gi < _groups.Count; gi++)
                        span[gi] = i64.Memory.Span[_groups[gi][0]];
                }
                else if (src is Data.BooleanSeries bs)
                {
                    keyCol = new Data.BooleanSeries(colName, _groups.Count);
                    var span = ((Data.BooleanSeries)keyCol).Memory.Span;
                    for (int gi = 0; gi < _groups.Count; gi++)
                        span[gi] = bs.Memory.Span[_groups[gi][0]];
                }
                else
                {
                    keyCol = (ISeries)Activator.CreateInstance(src.GetType(), colName, _groups.Count)!;
                    for (int gi = 0; gi < _groups.Count; gi++)
                        src.Take(keyCol, _groups[gi][0], gi);
                }
                groupKeyCols.Add(keyCol);
            }

            // Build the "groups" column as a ListSeries where each element is an Int32Series of row indices
            var offsets = new int[_groups.Count + 1];
            int totalIndices = 0;
            for (int gi = 0; gi < _groups.Count; gi++)
            {
                offsets[gi] = totalIndices;
                totalIndices += _groups[gi].Count;
            }
            offsets[_groups.Count] = totalIndices;

            var allIndices = new int[totalIndices];
            int pos = 0;
            for (int gi = 0; gi < _groups.Count; gi++)
            {
                foreach (var idx in _groups[gi])
                    allIndices[pos++] = idx;
            }

            var offsetsSeries = new Data.Int32Series("offsets", offsets);
            var values = new Data.Int32Series("values", allIndices);
            var groupsCol = new Data.ListSeries("groups", offsetsSeries, values);

            groupKeyCols.Add(groupsCol);
            return new DataFrame(groupKeyCols);
        }
    }
}
