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
        public async Task TestProjectionPushdown()
        {
            var csv = "A,B,C,D,E\n1,2,3,4,5\n6,7,8,9,10";
            var path = "test_large.csv";
            await System.IO.File.WriteAllTextAsync(path, csv);

            // test_large.csv has A,B,C,D,E
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
            var path = "test_large.csv";
            // Filter after Select should be pushed down
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
            var path = "test_large.csv";
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
