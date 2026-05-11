using System;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using System.IO;

namespace Glacier.Polaris.Tests
{
    public class FeatureTests
    {
        [Fact]
        public async Task TestWithColumns()
        {
            var csv = "ID,Name,Salary\n1,Alice,50000\n2,Bob,60000\n3,Charlie,70000";
            var path = Path.GetTempFileName();
            File.WriteAllText(path, csv);

            try
            {
                var lf = LazyFrame.ScanCsv(path);
                
                // Add a new column and overwrite an existing one
                var result = await lf
                    .WithColumns(
                        (Expr.Col("Salary") * 1.1).Alias("SalaryBonus"),
                        (Expr.Col("Salary") + 500).Alias("Salary") // Overwrite
                    )
                    .Collect();

                Assert.Equal(4, result.Columns.Count);
                Assert.Equal(50500, ((Data.Int32Series)result.GetColumn("Salary")).Memory.Span[0]);
                Assert.Equal(55000.0, ((Data.Float64Series)result.GetColumn("SalaryBonus")).Memory.Span[0], 5);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task TestWhenThenOtherwise()
        {
            var csv = "ID,Name,Salary\n1,Alice,50000\n2,Bob,60000\n3,Charlie,70000";
            var path = Path.GetTempFileName();
            File.WriteAllText(path, csv);

            try
            {
                var lf = LazyFrame.ScanCsv(path);
                
                // Conditional logic
                var result = await lf
                    .WithColumns(
                        Expr.When(Expr.Col("Salary") > 55000)
                            .Then("High")
                            .Otherwise("Low")
                            .Alias("Level")
                    )
                    .Collect();

                var levelCol = (Data.Utf8StringSeries)result.GetColumn("Level");
                Assert.Equal("Low", System.Text.Encoding.UTF8.GetString(levelCol.GetStringSpan(0)));
                Assert.Equal("High", System.Text.Encoding.UTF8.GetString(levelCol.GetStringSpan(1)));
                Assert.Equal("High", System.Text.Encoding.UTF8.GetString(levelCol.GetStringSpan(2)));
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
