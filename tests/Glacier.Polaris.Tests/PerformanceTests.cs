using System;
using System.Diagnostics;
using Glacier.Polaris.Data;
using Glacier.Polaris.Compute;
using Xunit;
using Xunit.Abstractions;

namespace Glacier.Polaris.Tests
{
    public class PerformanceTests
    {
        private readonly ITestOutputHelper _output;

        public PerformanceTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void Benchmark_InnerJoin_Large()
        {
            int n = 50_000;
            using var left = new Int32Series("left", n);
            using var right = new Int32Series("right", n);

            var lSpan = left.Memory.Span;
            var rSpan = right.Memory.Span;

            for (int i = 0; i < n; i++)
            {
                lSpan[i] = i % 10_000;
                rSpan[i] = i % 10_000;
            }

            var sw = Stopwatch.StartNew();
            var result = JoinKernels.InnerJoin(left, right);
            sw.Stop();

            _output.WriteLine($"InnerJoin {n} x {n}: {sw.ElapsedMilliseconds}ms, Rows: {result.LeftIndices.Length}");
            Assert.True(result.LeftIndices.Length > 0);
        }

        [Fact]
        public void Benchmark_GroupBy_Large()
        {
            int n = 100_000;
            using var col = new Int32Series("col", n);
            var span = col.Memory.Span;
            for (int i = 0; i < n; i++) span[i] = i % 1000;

            var sw = Stopwatch.StartNew();
            var groups = GroupByKernels.GroupBy(col);
            sw.Stop();

            _output.WriteLine($"GroupBy {n} (1000 groups): {sw.ElapsedMilliseconds}ms, Groups: {groups.Count}");
            Assert.Equal(1000, groups.Count);
        }

        [Fact]
        public void Benchmark_StringToUpper_Large()
        {
            int n = 50_000;
            string[] data = new string[n];
            for (int i = 0; i < n; i++) data[i] = "QuickBrownFoxJumpedOverTheLazyDog_" + i;

            using var series = new Utf8StringSeries("s", data);

            var sw = Stopwatch.StartNew();
            using var upper = StringKernels.ToUppercase(series);
            sw.Stop();

            _output.WriteLine($"String ToUpper {n}: {sw.ElapsedMilliseconds}ms");
            Assert.Equal(n, upper.Length);
        }
    }
}
