using System;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class RollingExpandingTests
    {
        [Fact]
        public async Task TestRollingStd()
        {
            var val = new Float64Series("val", new[] { 10.0, 20.0, 30.0 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(Expr.Col("val").RollingStd(2))
                .CollectAsync()
                .FirstAsync();

            var stdCol = result.GetColumn("val_rolling_std") as Float64Series;
            Assert.NotNull(stdCol);

            // window=2
            // row 0: null
            // row 1: std(10, 20) = 7.071...
            // row 2: std(20, 30) = 7.071...

            Assert.True(stdCol.ValidityMask.IsNull(0));
            Assert.Equal(Math.Sqrt(50), stdCol.Memory.Span[1], 5);
            Assert.Equal(Math.Sqrt(50), stdCol.Memory.Span[2], 5);
        }

        [Fact]
        public async Task TestRollingMinMax()
        {
            var val = new Int32Series("val", new[] { 10, 5, 15, 7 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("val").RollingMin(2),
                    Expr.Col("val").RollingMax(2)
                )
                .CollectAsync()
                .FirstAsync();

            var minCol = result.GetColumn("val_rolling_min") as Int32Series;
            var maxCol = result.GetColumn("val_rolling_max") as Int32Series;

            Assert.Equal(5, minCol.Memory.Span[1]); // min(10, 5)
            Assert.Equal(5, minCol.Memory.Span[2]); // min(5, 15)
            Assert.Equal(7, minCol.Memory.Span[3]); // min(15, 7)

            Assert.Equal(10, maxCol.Memory.Span[1]); // max(10, 5)
            Assert.Equal(15, maxCol.Memory.Span[2]); // max(5, 15)
            Assert.Equal(15, maxCol.Memory.Span[3]); // max(15, 7)
        }

        [Fact]
        public async Task TestExpandingSum()
        {
            var val = new Int32Series("val", new[] { 1, 2, 3, 4 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(Expr.Col("val").ExpandingSum())
                .CollectAsync()
                .FirstAsync();

            var sumCol = result.GetColumn("val_expanding_sum") as Int32Series;
            Assert.Equal(1, sumCol.Memory.Span[0]);
            Assert.Equal(3, sumCol.Memory.Span[1]);
            Assert.Equal(6, sumCol.Memory.Span[2]);
            Assert.Equal(10, sumCol.Memory.Span[3]);
        }

        [Fact]
        public async Task TestExpandingMean()
        {
            var val = new Float64Series("val", new[] { 1.0, 3.0, 2.0 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(Expr.Col("val").ExpandingMean())
                .CollectAsync()
                .FirstAsync();

            var meanCol = result.GetColumn("val_expanding_mean") as Float64Series;
            Assert.Equal(1.0, meanCol.Memory.Span[0]);
            Assert.Equal(2.0, meanCol.Memory.Span[1]);
            Assert.Equal(2.0, meanCol.Memory.Span[2]); // (1+3+2)/3 = 2
        }

        [Fact]
        public async Task TestEWMMean()
        {
            var val = new Float64Series("val", new[] { 1.0, 2.0, 3.0 });
            var df = new DataFrame(new ISeries[] { val });

            // y[0] = 1
            // y[1] = 0.5 * 2 + 0.5 * 1 = 1.5
            // y[2] = 0.5 * 3 + 0.5 * 1.5 = 2.25
            var result = await df.Lazy()
                .Select(Expr.Col("val").EWMMean(0.5))
                .CollectAsync()
                .FirstAsync();

            var ewmCol = result.GetColumn("val_ewm_mean") as Float64Series;
            Assert.Equal(1.0, ewmCol.Memory.Span[0]);
            Assert.Equal(1.5, ewmCol.Memory.Span[1]);
            Assert.Equal(2.25, ewmCol.Memory.Span[2]);
        }

        [Fact]
        public async Task TestExpandingMinMax()
        {
            var val = new Int32Series("val", new[] { 10, 5, 15, 7 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("val").ExpandingMin(),
                    Expr.Col("val").ExpandingMax()
                )
                .CollectAsync()
                .FirstAsync();

            var minCol = result.GetColumn("val_expanding_min") as Int32Series;
            var maxCol = result.GetColumn("val_expanding_max") as Int32Series;

            Assert.Equal(10, minCol.Memory.Span[0]);
            Assert.Equal(5, minCol.Memory.Span[1]);
            Assert.Equal(5, minCol.Memory.Span[2]);
            Assert.Equal(5, minCol.Memory.Span[3]);

            Assert.Equal(10, maxCol.Memory.Span[0]);
            Assert.Equal(10, maxCol.Memory.Span[1]);
            Assert.Equal(15, maxCol.Memory.Span[2]);
            Assert.Equal(15, maxCol.Memory.Span[3]);
        }

        [Fact]
        public async Task TestExpandingStd()
        {
            var val = new Float64Series("val", new[] { 10.0, 20.0, 30.0 });
            var df = new DataFrame(new ISeries[] { val });

            var result = await df.Lazy()
                .Select(Expr.Col("val").ExpandingStd())
                .CollectAsync()
                .FirstAsync();

            var stdCol = result.GetColumn("val_expanding_std") as Float64Series;
            Assert.NotNull(stdCol);

            // row 0: null
            // row 1: std(10, 20) = 7.071...
            // row 2: std(10, 20, 30) = 10.0

            Assert.True(stdCol.ValidityMask.IsNull(0));
            Assert.Equal(Math.Sqrt(50), stdCol.Memory.Span[1], 5);
            Assert.Equal(10.0, stdCol.Memory.Span[2], 5);
        }
    }
}
