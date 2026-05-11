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
        public void Histogram_BasicIntegerSeries()
        {
            var series = new Float64Series("x", new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 10.0 });

            // Histogram with 5 buckets (bins)
            var histDf = series.Hist(5);
            Assert.Equal(5, histDf.RowCount);
            var startCol = (Float64Series)histDf.GetColumn("bin_start");
            var endCol = (Float64Series)histDf.GetColumn("bin_end");
            var countCol = (Int32Series)histDf.GetColumn("count");

            // Min = 1.0, Max = 10.0. Bin size = (10 - 1) / 5 = 1.8.
            // Bins:
            // [1.0, 2.8) -> 1.0, 2.0 -> count = 2
            // [2.8, 4.6) -> 3.0, 4.0 -> count = 2
            // [4.6, 6.4) -> 5.0 -> count = 1
            // [6.4, 8.2) -> count = 0
            // [8.2, 10.0] -> 10.0 -> count = 1
            Assert.Equal(1.0, startCol.Memory.Span[0]);
            Assert.Equal(2.8, endCol.Memory.Span[0], 4);
            Assert.Equal(2, countCol.Memory.Span[0]);
            Assert.Equal(2, countCol.Memory.Span[1]);
            Assert.Equal(1, countCol.Memory.Span[2]);
            Assert.Equal(0, countCol.Memory.Span[3]);
            Assert.Equal(1, countCol.Memory.Span[4]);
        }

[Fact]
public void Histogram_DataFrameApi()
{
    var df = new DataFrame(new ISeries[] {
        new Float64Series("x", new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 10.0 })
    });

    var histDf = df.Hist("x", 5);
    Assert.Equal(5, histDf.RowCount);
    var countCol = (Int32Series)histDf.GetColumn("count");
    Assert.Equal(2, countCol.Memory.Span[0]);
}

[Fact]
public void Kde_Basic()
{
    var series = new Float64Series("x", new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 10.0 });

    // KDE evaluation with bandwidth=1.0 and 3 grid points
    var kdeDf = series.Kde(1.0, 3);
    Assert.Equal(3, kdeDf.RowCount);
    var gridCol = (Float64Series)kdeDf.GetColumn("grid");
    var densityCol = (Float64Series)kdeDf.GetColumn("density");

    // Grid bounds: min - 3h = 1 - 3 = -2. max + 3h = 10 + 3 = 13.
    // step = 15 / 2 = 7.5. Grid points: -2.0, 5.5, 13.0
    Assert.Equal(-2.0, gridCol.Memory.Span[0]);
    Assert.Equal(5.5, gridCol.Memory.Span[1], 4);
    Assert.Equal(13.0, gridCol.Memory.Span[2]);

    // Density must be positive at all grid points
    Assert.True(densityCol.Memory.Span[0] > 0);
    Assert.True(densityCol.Memory.Span[1] > 0);
    Assert.True(densityCol.Memory.Span[2] > 0);
}

[Fact]
public void Kde_DataFrameApi()
{
    var df = new DataFrame(new ISeries[] {
        new Float64Series("x", new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 10.0 })
    });

    var kdeDf = df.Kde("x", 1.0, 3);
    Assert.Equal(3, kdeDf.RowCount);
    var densityCol = (Float64Series)kdeDf.GetColumn("density");
    Assert.True(densityCol.Memory.Span[0] > 0);
}

[Fact]
public async Task CollectStreaming_SlicesDataFrameCorrectly()
{
    // Set up a large DataFrame
    var data = Enumerable.Range(1, 100).Select(x => (double)x).ToArray();
    var timeCol = new Float64Series("time", data);
    var df = new DataFrame(new ISeries[] { timeCol });

    // Collect streaming with batchSize = 15
    var lazy = df.Lazy();
    var batches = new List<DataFrame>();
    await foreach (var batch in lazy.CollectStreaming(15))
    {
        batches.Add(batch);
    }

    // 100 elements divided by batchSize 15 yields 7 batches
    Assert.Equal(7, batches.Count);
    Assert.Equal(15, batches[0].RowCount);
    Assert.Equal(15, batches[1].RowCount);
    Assert.Equal(10, batches[6].RowCount);

    // Reconstruct and check equivalence
    var combined = DataFrame.Concat(batches);
    Assert.Equal(100, combined.RowCount);
    var combinedTime = (Float64Series)combined.GetColumn("time");
    Assert.Equal(1.0, combinedTime.Memory.Span[0]);
    Assert.Equal(100.0, combinedTime.Memory.Span[99]);
}        [Fact]
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
