using System;
using System.Linq;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class WindowTests
    {
        [Fact]
        public async Task TestOverSum()
        {
            var id = new Int32Series("id", new[] { 1, 1, 2, 2 });
            var val = new Int32Series("val", new[] { 10, 20, 5, 15 });
            var df = new DataFrame(new ISeries[] { id, val });

            // select id, val, val.sum().over("id") as id_sum
            var result = await df.Lazy()
                .Select(
                    Expr.Col("id"),
                    Expr.Col("val"),
                    Expr.Col("val").Sum().Over("id").Alias("id_sum")
                )
                .CollectAsync()
                .FirstAsync();

            var sumCol = result.GetColumn("id_sum") as Int32Series;
            Assert.NotNull(sumCol);

            // Group 1 (id=1): 10 + 20 = 30
            // Group 2 (id=2): 5 + 15 = 20
            Assert.Equal(30, sumCol.Memory.Span[0]);
            Assert.Equal(30, sumCol.Memory.Span[1]);
            Assert.Equal(20, sumCol.Memory.Span[2]);
            Assert.Equal(20, sumCol.Memory.Span[3]);
        }

        [Fact]
        public async Task TestRollingMean()
        {
            var val = new Int32Series("val", new[] { 10, 20, 30, 40 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("val").RollingMean(2)
                )
                .CollectAsync()
                .FirstAsync();

            var meanCol = result.GetColumn("val_rolling_mean") as Float64Series;
            Assert.NotNull(meanCol);

            // window=2
            // row 0: null (partial)
            // row 1: (10+20)/2 = 15
            // row 2: (20+30)/2 = 25
            // row 3: (30+40)/2 = 35

            Assert.True(meanCol.ValidityMask.IsNull(0));
            Assert.Equal(15.0, meanCol.Memory.Span[1]);
            Assert.Equal(25.0, meanCol.Memory.Span[2]);
            Assert.Equal(35.0, meanCol.Memory.Span[3]);
        }
    }
}
