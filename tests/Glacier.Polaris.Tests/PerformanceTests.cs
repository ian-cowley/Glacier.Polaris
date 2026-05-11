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
            int n = 1_000_000;
            var left = new Int32Series("left", n);
            var right = new Int32Series("right", n);
            
            var lSpan = left.Memory.Span;
            var rSpan = right.Memory.Span;
            
            for (int i = 0; i < n; i++)
            {
                lSpan[i] = i % 100_000;
                rSpan[i] = i % 100_000;
            }

            GC.Collect();
            long memBefore = GC.GetTotalMemory(true);
            var sw = Stopwatch.StartNew();
            
            var result = JoinKernels.InnerJoin(left, right);
            
            sw.Stop();
            long memAfter = GC.GetTotalMemory(true);

            _output.WriteLine($"InnerJoin 1M x 1M: {sw.ElapsedMilliseconds}ms, Mem: {(memAfter - memBefore) / 1024 / 1024}MB, Rows: {result.LeftIndices.Length}");
            
            Assert.True(result.LeftIndices.Length > 0);
        }

        [Fact]
        public void Benchmark_GroupBy_Large()
        {
            int n = 1_000_000;
            var col = new Int32Series("col", n);
            var span = col.Memory.Span;
            for (int i = 0; i < n; i++) span[i] = i % 1000;

            GC.Collect();
            long memBefore = GC.GetTotalMemory(true);
            var sw = Stopwatch.StartNew();
            
            var groups = GroupByKernels.GroupBy(col);
            
            sw.Stop();
            long memAfter = GC.GetTotalMemory(true);

            _output.WriteLine($"GroupBy 1M (1000 groups): {sw.ElapsedMilliseconds}ms, Mem: {(memAfter - memBefore) / 1024 / 1024}MB, Groups: {groups.Count}");
            
            Assert.Equal(1000, groups.Count);
        }

        [Fact]
        public void Benchmark_StringToUpper_Large()
        {
            int n = 500_000;
            string[] data = new string[n];
            for (int i = 0; i < n; i++) data[i] = "QuickBrownFoxJumpedOverTheLazyDog_" + i;
            
            var series = new Utf8StringSeries("s", data);

            GC.Collect();
            long memBefore = GC.GetTotalMemory(true);
            var sw = Stopwatch.StartNew();
            
            var upper = StringKernels.ToUppercase(series);
            
            sw.Stop();
            long memAfter = GC.GetTotalMemory(true);

            _output.WriteLine($"String ToUpper 500k: {sw.ElapsedMilliseconds}ms, Mem: {(memAfter - memBefore) / 1024 / 1024}MB");
            
            Assert.Equal(n, upper.Length);
        }
    }
}
