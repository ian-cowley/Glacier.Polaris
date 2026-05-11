using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris
{
    /// <summary>
    /// Builder for group_by_dynamic operations (time-window-based grouping).
    /// Ported from Glacier.Polaris_OLD.
    /// </summary>
    public sealed class GroupByDynamicBuilder
    {
        private readonly DataFrame _source;
        private readonly string _indexColumn;
        private readonly List<List<int>> _groups;
        private readonly ISeries _windowKeys;

        public GroupByDynamicBuilder(
            DataFrame source,
            string indexColumn,
            string every,
            string? period = null,
            string? offset = null,
            string closed = "left",
            string startBy = "window")
        {
            _source = source;
            _indexColumn = indexColumn;

            var idxCol = source.GetColumn(indexColumn);
            double parsedEvery = Duration.Parse(every, idxCol.DataType);
            double parsedPeriod = string.IsNullOrEmpty(period) ? parsedEvery : Duration.Parse(period, idxCol.DataType);
            double parsedOffset = string.IsNullOrEmpty(offset) ? 0 : Duration.Parse(offset, idxCol.DataType);

            var res = GroupByKernels.GenerateDynamicGroups(idxCol, parsedEvery, parsedPeriod, parsedOffset, closed, startBy);
            _groups = res.Groups;
            _windowKeys = res.WindowKeys;
        }

        public DataFrame Agg(params (string col, string agg)[] aggregations)
        {
            var resultColumns = new List<ISeries>();
            resultColumns.Add(_windowKeys);

            foreach (var (col, agg) in aggregations)
            {
                var sourceCol = _source.GetColumn(col);
                var resultCol = GroupByKernels.Aggregate(sourceCol, _groups, agg);
                resultColumns.Add(resultCol);
            }

            return new DataFrame(resultColumns);
        }

        public DataFrame Agg(params Expr[] exprs)
        {
            // Fallback: LazyGroupByDynamic on eager DataFrames can delegate to LazyFrame execution
            var lazyFrame = _source.Lazy().GroupByDynamic(_indexColumn, "1h", period: "1h");
            return lazyFrame.Agg(exprs).Collect().GetAwaiter().GetResult();
        }
    }
}
