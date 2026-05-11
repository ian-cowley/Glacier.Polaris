using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using static Glacier.Polaris.Functions;

namespace Glacier.Polaris.Tests
{
    public class AnalyticsTests
    {
        [Fact]
        public async Task TestRollingMeanIntegration()
        {
            var df = new DataFrame(new ISeries[] {
                new Float64Series("val", new double[] { 1, 2, 3, 4, 5 })
            });

            var result = await df.Lazy()
                .Select(RollingMean("val", 3))
                .CollectAsync()
                .FirstAsync();

            var col = result.GetColumn("val_rolling_mean") as Float64Series;
            Assert.NotNull(col);
            // Index 2 is (1+2+3)/3 = 2
            Assert.Equal(2.0, col.Memory.Span[2]);
            Assert.Equal(3.0, col.Memory.Span[3]);
            Assert.Equal(4.0, col.Memory.Span[4]);
        }

        [Fact]
        public async Task TestRegexIntegration()
        {
            var strings = new string[] { "apple", "banana", "cherry" };
            int totalBytes = strings.Sum(s => System.Text.Encoding.UTF8.GetByteCount(s));
            var series = new Utf8StringSeries("name", 3, totalBytes);

            var offsetSpan = series.Offsets.Span;
            var dataSpan = series.DataBytes.Span;
            int currentOffset = 0;
            for (int i = 0; i < strings.Length; i++)
            {
                offsetSpan[i] = currentOffset;
                var bytes = System.Text.Encoding.UTF8.GetBytes(strings[i]);
                bytes.CopyTo(dataSpan.Slice(currentOffset));
                currentOffset += bytes.Length;
            }
            offsetSpan[3] = currentOffset;

            var df = new DataFrame(new ISeries[] { series });

            var result = await df.Lazy()
                .Select(RegexMatch("name", "a.*a"))
                .CollectAsync()
                .FirstAsync();

            var col = result.GetColumn("name_match") as BooleanSeries;
            Assert.NotNull(col);
            Assert.False(col.Memory.Span[0]); // apple (doesn't match "a.*a")
            Assert.True(col.Memory.Span[1]);  // banana (matches "a.*a")
            Assert.False(col.Memory.Span[2]); // cherry (doesn't match "a.*a")
        }
        [Fact]
        public async Task TestFloor()
        {
            var df = new DataFrame(new ISeries[] {
                new Float64Series("val", new double[] { 1.5, -2.3, 3.0, double.NaN, 4.7 })
            });
            // Mark index 3 as null
            df.GetColumn("val").ValidityMask.SetNull(3);

            var result = await df.Lazy()
                .Select(Expr.Col("val").Floor())
                .CollectAsync()
                .FirstAsync();

            var col = result.GetColumn("val_floor") as Float64Series;
            Assert.NotNull(col);
            Assert.Equal(1.0, col.Memory.Span[0]);
            Assert.Equal(-3.0, col.Memory.Span[1]);
            Assert.Equal(3.0, col.Memory.Span[2]);
            Assert.True(col.ValidityMask.IsNull(3));
            Assert.Equal(4.0, col.Memory.Span[4]);
        }

        [Fact]
        public async Task TestCumProd()
        {
            var df = new DataFrame(new ISeries[] {
                new Int32Series("val", new int[] { 2, 3, 4, 0, 5 })
            });
            df.GetColumn("val").ValidityMask.SetNull(3);

            var result = await df.Lazy()
                .Select(Expr.Col("val").CumProd())
                .CollectAsync()
                .FirstAsync();

            var col = result.GetColumn("val_cum_prod") as Int64Series;
            Assert.NotNull(col);
            Assert.Equal(2L, col.Memory.Span[0]);    // 2
            Assert.Equal(6L, col.Memory.Span[1]);    // 2*3
            Assert.Equal(24L, col.Memory.Span[2]);   // 2*3*4
            Assert.True(col.ValidityMask.IsNull(3)); // null → result is null
            Assert.Equal(120L, col.Memory.Span[4]);  // 24*5 (carries through null)
        }

        [Fact]
        public async Task TestRound()
        {
            var df = new DataFrame(new ISeries[] {
                new Float64Series("val", new double[] { 1.5, 2.3, 3.7, double.NaN, -1.5 })
            });
            df.GetColumn("val").ValidityMask.SetNull(3);

            var result = await df.Lazy()
                .Select(Expr.Col("val").Round())
                .CollectAsync()
                .FirstAsync();

            var col = result.GetColumn("val_round") as Float64Series;
            Assert.NotNull(col);
            Assert.Equal(2.0, col.Memory.Span[0]);  // 1.5 → 2 (AwayFromZero)
            Assert.Equal(2.0, col.Memory.Span[1]);  // 2.3 → 2
            Assert.Equal(4.0, col.Memory.Span[2]);  // 3.7 → 4
            Assert.True(col.ValidityMask.IsNull(3));
            Assert.Equal(-2.0, col.Memory.Span[4]); // -1.5 → -2 (AwayFromZero)
        }

        [Fact]
        public async Task TestCumCount()
        {
            var df = new DataFrame(new ISeries[] {
                new Int32Series("val", new int[] { 1, 0, 3, 0, 5 })
            });
            // Mark index 1 and 3 as null
            df.GetColumn("val").ValidityMask.SetNull(1);
            df.GetColumn("val").ValidityMask.SetNull(3);

            var result = await df.Lazy()
                .Select(Expr.Col("val").CumCount())
                .CollectAsync()
                .FirstAsync();

            var col = result.GetColumn("val_cum_count") as Int32Series;
            Assert.NotNull(col);
            // Indices of non-null: 0=1, 1=null(skip), 2=2, 3=null(skip), 4=3
            Assert.Equal(1, col.Memory.Span[0]);
            Assert.Equal(1, col.Memory.Span[1]);  // still 1
            Assert.Equal(2, col.Memory.Span[2]);
            Assert.Equal(2, col.Memory.Span[3]);  // still 2
            Assert.Equal(3, col.Memory.Span[4]);
        }

    }

    public static class AsyncEnumerableExtensions
    {
        public static async Task<T> FirstAsync<T>(this IAsyncEnumerable<T> source)
        {
            await foreach (var item in source) return item;
            throw new InvalidOperationException("No elements");
        }
    }
}
