using System;
using System.IO;
using System.Threading.Tasks;
using Glacier.Polaris;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class PushdownTests
    {
        [Fact]
        public async Task TestJoinPredicatePushdown()
        {
            var leftCsv = "ID,Name\n1,Alice\n2,Bob\n3,Charlie";
            var rightCsv = "ID,Salary\n1,50000\n2,60000\n3,70000";
            
            await File.WriteAllTextAsync("left.csv", leftCsv);
            await File.WriteAllTextAsync("right.csv", rightCsv);

            var lf_left = LazyFrame.ScanCsv("left.csv");
            var lf_right = LazyFrame.ScanCsv("right.csv");

            // Filter on Salary (Right side) should be pushed down into lf_right
            var result = await lf_left.Join(lf_right, on: "ID")
                .Filter(c => Expr.Col("Salary") > 65000)
                .Collect();

            Assert.Equal(1, result.RowCount);
            var nameCol = (Data.Utf8StringSeries)result.GetColumn("Name");
            Assert.Equal("Charlie", System.Text.Encoding.UTF8.GetString(nameCol.GetStringSpan(0)));
        }

        [Fact]
        public async Task TestJoinProjectionPushdown()
        {
            var leftCsv = "ID,Name,Age\n1,Alice,30\n2,Bob,25";
            var rightCsv = "ID,Salary,Dept\n1,50000,IT\n2,60000,HR";
            
            await File.WriteAllTextAsync("left_proj.csv", leftCsv);
            await File.WriteAllTextAsync("right_proj.csv", rightCsv);

            var lf_left = LazyFrame.ScanCsv("left_proj.csv");
            var lf_right = LazyFrame.ScanCsv("right_proj.csv");

            // We only use Name and Salary.
            // Optimizer should tell left scanner to skip 'Age' and right scanner to skip 'Dept'.
            var result = await lf_left.Join(lf_right, on: "ID")
                .Select(Expr.Col("Name"), Expr.Col("Salary"))
                .Collect();

            Assert.Equal(2, result.Columns.Count);
            Assert.Contains(result.Columns, c => c.Name == "Name");
            Assert.Contains(result.Columns, c => c.Name == "Salary");
            Assert.DoesNotContain(result.Columns, c => c.Name == "Age");
            Assert.DoesNotContain(result.Columns, c => c.Name == "Dept");
        }

        [Fact]
        public async Task TestSlicePushdown()
        {
            var csv = "ID,Val\n1,10\n2,20\n3,30\n4,40\n5,50";
            await File.WriteAllTextAsync("slice.csv", csv);

            var lf = LazyFrame.ScanCsv("slice.csv");
            
            // Limit(5) should be pushed into ScanCsv
            var result = await lf.Limit(2).Collect();

            Assert.Equal(2, result.RowCount);
            var valCol = (Data.Int32Series)result.GetColumn("Val");
            Assert.Equal(10, valCol.Memory.Span[0]);
            Assert.Equal(20, valCol.Memory.Span[1]);
        }
    }
}
