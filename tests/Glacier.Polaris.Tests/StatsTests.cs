using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class StatsTests
    {
        [Fact]
        public async Task TestGroupByAggregates()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 1, 1, 2, 2, 2 }),
                new Int32Series("b", new[] { 10, 20, 30, 40, 50 })
            });

            var result = await df.Lazy()
                .GroupBy("a")
                .Agg(
                    Expr.Col("b").Sum().Alias("sum_b"),
                    Expr.Col("b").Mean().Alias("mean_b"),
                    Expr.Col("b").Median().Alias("median_b"),
                    Expr.Col("b").NUnique().Alias("nunique_b"),
                    Expr.Col("b").Quantile(0.5).Alias("q50_b")
                )
                .Collect();

            // We expect 2 rows (a=1, a=2)
            Assert.Equal(2, result.RowCount);
            
            // Row where a=1
            var aCol = (Int32Series)result.GetColumn("a");
            int idx1 = -1;
            for(int i=0; i<2; i++) if (aCol.Memory.Span[i] == 1) idx1 = i;
            Assert.True(idx1 != -1);
            
            Assert.Equal(30, (int)result.GetColumn("sum_b").Get(idx1)!);
            Assert.Equal(15.0, (double)result.GetColumn("mean_b").Get(idx1)!);
            Assert.Equal(15.0, (double)result.GetColumn("median_b").Get(idx1)!);
            Assert.Equal(2, (int)result.GetColumn("nunique_b").Get(idx1)!);
            Assert.Equal(15.0, (double)result.GetColumn("q50_b").Get(idx1)!);

            // Row where a=2
            int idx2 = 1 - idx1;
            Assert.Equal(120, (int)result.GetColumn("sum_b").Get(idx2)!);
            Assert.Equal(40.0, (double)result.GetColumn("mean_b").Get(idx2)!);
            Assert.Equal(40.0, (double)result.GetColumn("median_b").Get(idx2)!);
            Assert.Equal(3, (int)result.GetColumn("nunique_b").Get(idx2)!);
            Assert.Equal(40.0, (double)result.GetColumn("q50_b").Get(idx2)!);
        }
    }
}
