using System;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class LazyTests
    {
        [Fact]
        public void MapGroups_SortEachGroup()
        {
            // Create a DataFrame with groups: A=1,2,3, B=4,5
            var groupCol = new Data.Utf8StringSeries("group", new[] { "a", "a", "a", "b", "b" });
            var valCol = new Data.Int32Series("val", new[] { 3, 1, 2, 5, 4 });
            var df = new DataFrame(new ISeries[] { groupCol, valCol });

            // Apply map_groups: sort each group by val
            var gb = new GroupByBuilder(df, "group");
            var result = gb.MapGroups(g => g.Sort("val"));

            // Groups appear in order (a then b)
            // Group a sorted: 1, 2, 3
            // Group b sorted: 4, 5
            Assert.Equal(5, result.RowCount);
            var resultGroup = (Data.Utf8StringSeries)result.GetColumn("group");
            var resultVal = (Data.Int32Series)result.GetColumn("val");
            Assert.Equal("a", resultGroup.GetString(0));
            Assert.Equal(1, resultVal.Memory.Span[0]);
            Assert.Equal("a", resultGroup.GetString(1));
            Assert.Equal(2, resultVal.Memory.Span[1]);
            Assert.Equal("a", resultGroup.GetString(2));
            Assert.Equal(3, resultVal.Memory.Span[2]);
            Assert.Equal("b", resultGroup.GetString(3));
            Assert.Equal(4, resultVal.Memory.Span[3]);
            Assert.Equal("b", resultGroup.GetString(4));
            Assert.Equal(5, resultVal.Memory.Span[4]);
        }

[Fact]
public void MapGroups_FilterEachGroup()
{
    var groupCol = new Data.Utf8StringSeries("group", new[] { "a", "a", "a", "b", "b" });
    var valCol = new Data.Int32Series("val", new[] { 10, 20, 30, 5, 15 });
    var df = new DataFrame(new ISeries[] { groupCol, valCol });

    // Filter each group: keep only rows where val > 10
    var gb = new GroupByBuilder(df, "group");
    var result = gb.MapGroups(g => g.Filter(Expr.Col("val").Gt(Expr.Lit(10))));

    // Group a: 20, 30 (10 removed)
    // Group b: 15 (5 removed)
    Assert.Equal(3, result.RowCount);
    var resultVal = (Data.Int32Series)result.GetColumn("val");
    Assert.Equal(20, resultVal.Memory.Span[0]);
    Assert.Equal(30, resultVal.Memory.Span[1]);
    Assert.Equal(15, resultVal.Memory.Span[2]);
}

[Fact]
public void MapGroups_SelectColumn()
{
    var groupCol = new Data.Utf8StringSeries("group", new[] { "a", "a", "b", "b" });
    var val1 = new Data.Int32Series("val1", new[] { 1, 2, 3, 4 });
    var val2 = new Data.Float64Series("val2", new[] { 1.5, 2.5, 3.5, 4.5 });
    var df = new DataFrame(new ISeries[] { groupCol, val1, val2 });

    // Each group: select only val1
    var gb = new GroupByBuilder(df, "group");
    var result = gb.MapGroups(g => g.Select(Expr.Col("val1")));

    Assert.Equal(4, result.RowCount);
    Assert.Single(result.Columns);
    Assert.Equal("val1", result.Columns[0].Name);
    var resultVal = (Data.Int32Series)result.GetColumn("val1");
    Assert.Equal(1, resultVal.Memory.Span[0]);
    Assert.Equal(2, resultVal.Memory.Span[1]);
    Assert.Equal(3, resultVal.Memory.Span[2]);
    Assert.Equal(4, resultVal.Memory.Span[3]);
}

[Fact]
public async Task CollectStreaming_TrueYieldsChunks()
{
    var data = Enumerable.Range(1, 50).Select(x => (double)x).ToArray();
    var df = new DataFrame(new ISeries[] { new Data.Float64Series("val", data) });

    var lazy = df.Lazy();
    var chunks = new List<DataFrame>();
    await foreach (var chunk in lazy.Collect(streaming: true))
    {
        chunks.Add(chunk);
    }

    // CsvReader and DataFrameOp currently produce single chunks
    Assert.Single(chunks);
    Assert.Equal(50, chunks[0].RowCount);
}

[Fact]
public async Task CollectStreaming_FalseReturnsSingleChunk()
{
    var data = Enumerable.Range(1, 50).Select(x => (double)x).ToArray();
    var df = new DataFrame(new ISeries[] { new Data.Float64Series("val", data) });

    var lazy = df.Lazy();
    var chunks = new List<DataFrame>();
    await foreach (var chunk in lazy.Collect(streaming: false))
    {
        chunks.Add(chunk);
    }

    Assert.Single(chunks);
    Assert.Equal(50, chunks[0].RowCount);
}    [Fact]
        public async Task TestProjectionPushdown()
        {
            var csv = "A,B,C,D,E\n1,2,3,4,5\n6,7,8,9,10";
            var path = "test_proj_pushdown.csv";
            await System.IO.File.WriteAllTextAsync(path, csv);

            var lf = LazyFrame.ScanCsv(path);

            var result = await lf.Select(Expr.Col("C"))
                .Collect();

            // Result should ONLY have column C
            Assert.Single(result.Columns);
            Assert.Equal("C", result.Columns[0].Name);
            Assert.Equal(2, result.RowCount);
            Assert.Equal(8, ((Data.Int32Series)result.GetColumn("C")).Memory.Span[1]);
        }

        [Fact]
        public async Task TestPredicatePushdown()
        {
            var csv = "A,B,C,D,E\n1,2,3,4,5\n6,7,8,9,10";
            var path = "test_pred_pushdown.csv";
            await System.IO.File.WriteAllTextAsync(path, csv);

            var lf = LazyFrame.ScanCsv(path);

            var result = await lf.Select(Expr.Col("A"), Expr.Col("B"))
                .Filter(c => Expr.Col("A") == 6)
                .Collect();

            Assert.Equal(1, result.RowCount);
            Assert.Equal(6, ((Data.Int32Series)result.GetColumn("A")).Memory.Span[0]);
            Assert.Equal(7, ((Data.Int32Series)result.GetColumn("B")).Memory.Span[0]);
        }

        [Fact]
        public async Task TestSelectMerging()
        {
            var csv = "A,B,C,D,E\n1,2,3,4,5\n6,7,8,9,10";
            var path = "test_select_merge.csv";
            await System.IO.File.WriteAllTextAsync(path, csv);

            var lf = LazyFrame.ScanCsv(path);

            // Select(Select(Scan)) -> Should merge into one Select
            var result = await lf.Select(Expr.Col("A"), Expr.Col("B"), Expr.Col("C"))
                .Select(Expr.Col("B"), Expr.Col("C"))
                .Collect();

            Assert.Equal(2, result.Columns.Count);
            Assert.Equal("B", result.Columns[0].Name);
            Assert.Equal("C", result.Columns[1].Name);
        }
    }
}
