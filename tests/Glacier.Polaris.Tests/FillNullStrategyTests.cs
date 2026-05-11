using System;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class FillNullStrategyTests
    {
        [Fact]
        public async Task TestForwardFill()
        {
            var series = new Int32Series("A", 5);
            series.Memory.Span[0] = 1;
            series.Memory.Span[2] = 2;
            series.ValidityMask.SetValid(0);
            series.ValidityMask.SetNull(1);
            series.ValidityMask.SetValid(2);
            series.ValidityMask.SetNull(3);
            series.ValidityMask.SetNull(4);

            var df = new DataFrame(new[] { series });
            var result = await df.Lazy()
                .Select(Expr.Col("A").FillNull(FillStrategy.Forward))
                .Collect();

            var resSpan = ((Int32Series)result.GetColumn("A")).Memory.Span;
            Assert.Equal(1, resSpan[0]);
            Assert.Equal(1, resSpan[1]);
            Assert.Equal(2, resSpan[2]);
            Assert.Equal(2, resSpan[3]);
            Assert.Equal(2, resSpan[4]);
        }

        [Fact]
        public async Task TestBackwardFill()
        {
            var series = new Int32Series("A", 5);
            series.Memory.Span[1] = 1;
            series.Memory.Span[3] = 2;
            series.ValidityMask.SetNull(0);
            series.ValidityMask.SetValid(1);
            series.ValidityMask.SetNull(2);
            series.ValidityMask.SetValid(3);
            series.ValidityMask.SetNull(4);

            var df = new DataFrame(new[] { series });
            var result = await df.Lazy()
                .Select(Expr.Col("A").FillNull(FillStrategy.Backward))
                .Collect();

            var resSpan = ((Int32Series)result.GetColumn("A")).Memory.Span;
            Assert.Equal(1, resSpan[0]);
            Assert.Equal(1, resSpan[1]);
            Assert.Equal(2, resSpan[2]);
            Assert.Equal(2, resSpan[3]);
            Assert.True(result.GetColumn("A").ValidityMask.IsNull(4));
        }
        
        [Fact]
        public async Task TestFillZero()
        {
            var series = new Int32Series("A", 3);
            series.Memory.Span[0] = 1;
            series.ValidityMask.SetValid(0);
            series.ValidityMask.SetNull(1);
            series.ValidityMask.SetNull(2);

            var df = new DataFrame(new[] { series });
            var result = await df.Lazy()
                .Select(Expr.Col("A").FillNull(FillStrategy.Zero))
                .Collect();

            var resSpan = ((Int32Series)result.GetColumn("A")).Memory.Span;
            Assert.Equal(1, resSpan[0]);
            Assert.Equal(0, resSpan[1]);
            Assert.Equal(0, resSpan[2]);
        }
    }
}
