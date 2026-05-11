using System;
using System.Collections.Generic;
using System.Linq;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris
{
    /// <summary>
    /// Builder for rolling group-by operations (sliding-window-based grouping).
    /// Ported from Glacier.Polaris_OLD.
    /// </summary>
    public sealed class GroupByRollingBuilder
    {
        private readonly DataFrame _source;
        private readonly string _indexColumn;
        private readonly List<List<int>> _groups;

        public GroupByRollingBuilder(
            DataFrame source,
            string indexColumn,
            string period,
            string? offset = null,
            string closed = "right")
        {
            _source = source;
            _indexColumn = indexColumn;

            var idxCol = source.GetColumn(indexColumn);
            double parsedPeriod = Duration.Parse(period, idxCol.DataType);
            double parsedOffset = string.IsNullOrEmpty(offset) ? -parsedPeriod : Duration.Parse(offset, idxCol.DataType);

            _groups = GroupByKernels.GenerateRollingGroups(idxCol, parsedPeriod, parsedOffset, closed);
        }

        public DataFrame Agg(params (string col, string agg)[] aggregations)
        {
            var resultColumns = new List<ISeries>();
            resultColumns.Add(_source.GetColumn(_indexColumn));

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
            var lazyFrame = _source.Lazy().GroupByRolling(_indexColumn, "1h");
            return lazyFrame.Agg(exprs).Collect().GetAwaiter().GetResult();
        }
    }
}
