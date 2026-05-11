using System;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class AggComplexTests
    {
        [Fact]
        public async Task TestMinMaxAggregation()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("key", new[] { "A", "A", "B", "B" }),
                new Int32Series("val", new[] { 1, 10, 2, 20 })
            });

            var result = await df.Lazy()
                .GroupBy("key")
                .Agg(
                    Expr.Col("val").Min().Alias("min_val"),
                    Expr.Col("val").Max().Alias("max_val")
                )
                .Collect();

            var rows = result.GetColumn("key").Length;
            Assert.Equal(2, rows);

            var keyAIdx = -1;
            var keyBIdx = -1;
            for (int i = 0; i < rows; i++)
            {
                if (result.GetColumn("key").Get(i)?.ToString() == "A") keyAIdx = i;
                if (result.GetColumn("key").Get(i)?.ToString() == "B") keyBIdx = i;
            }

            Assert.Equal(1, result.GetColumn("min_val").Get(keyAIdx));
            Assert.Equal(10, result.GetColumn("max_val").Get(keyAIdx));
            Assert.Equal(2, result.GetColumn("min_val").Get(keyBIdx));
            Assert.Equal(20, result.GetColumn("max_val").Get(keyBIdx));
        }

        [Fact]
        public async Task TestComplexArithmeticAggregation()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("key", new[] { "A", "A", "B", "B" }),
                new Int32Series("a", new[] { 1, 2, 3, 4 }),
                new Int32Series("b", new[] { 10, 20, 30, 40 })
            });

            // (a.sum + b.sum) / 2
            var result = await df.Lazy()
                .GroupBy("key")
                .Agg(
                    ((Expr.Col("a").Sum() + Expr.Col("b").Sum()) / 2.0).Alias("total_half")
                )
                .Collect();

            var rows = result.GetColumn("key").Length;
            Assert.Equal(2, rows);

            var keyAIdx = -1;
            var keyBIdx = -1;
            for (int i = 0; i < rows; i++)
            {
                if (result.GetColumn("key").Get(i)?.ToString() == "A") keyAIdx = i;
                if (result.GetColumn("key").Get(i)?.ToString() == "B") keyBIdx = i;
            }

            // A: (1+2 + 10+20) / 2 = 33 / 2 = 16.5
            // B: (3+4 + 30+40) / 2 = 77 / 2 = 38.5
            Assert.Equal(16.5, result.GetColumn("total_half").Get(keyAIdx));
            Assert.Equal(38.5, result.GetColumn("total_half").Get(keyBIdx));
        }

        [Fact]
        public async Task TestAggregationWithLiteral()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("key", new[] { "A", "A" }),
                new Int32Series("val", new[] { 10, 20 })
            });

            var result = await df.Lazy()
                .GroupBy("key")
                .Agg(
                    (Expr.Col("val").Sum() + 100).Alias("boosted")
                )
                .Collect();

            // (10+20) + 100 = 130
            Assert.Equal(130, result.GetColumn("boosted").Get(0));
        }
    }
}
