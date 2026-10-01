using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class SortTests
    {
        [Fact]
        public async Task TestSort()
        {
            var ids = new Int32Series("id", new[] { 3, 1, 4, 2 });
            var values = new Utf8StringSeries("val", new[] { "three", "one", "four", "two" });
            var df = new DataFrame(new List<ISeries> { ids, values });

            var lf = df.Lazy().Sort("id");
            var result = await lf.Collect();

            Assert.Equal(4, result.RowCount);
            var resIds = (Int32Series)result.GetColumn("id");
            Assert.Equal(1, resIds.Memory.Span[0]);
            Assert.Equal(2, resIds.Memory.Span[1]);
            Assert.Equal(3, resIds.Memory.Span[2]);
            Assert.Equal(4, resIds.Memory.Span[3]);
        }

        [Fact]
        public async Task TestTopK()
        {
            var ids = new Int32Series("id", new[] { 10, 5, 20, 1, 15 });
            var df = new DataFrame(new List<ISeries> { ids });

            // Sort().Limit(2) should return 1 and 5
            var lf = df.Lazy().Sort("id").Limit(2);
            var result = await lf.Collect();

            Assert.Equal(2, result.RowCount);
            var resIds = (Int32Series)result.GetColumn("id");
            Assert.Equal(1, resIds.Memory.Span[0]);
            Assert.Equal(5, resIds.Memory.Span[1]);
        }

        [Fact]
        public async Task TestStringSort()
        {
            // (a, 2), (a, 1), (b, 3)
            var col1 = new Utf8StringSeries("c1", new[] { "b", "a", "a" });
            var col2 = new Int32Series("c2", new[] { 3, 2, 1 });
            var df = new DataFrame(new List<ISeries> { col1, col2 });

            // Sort by c1 (ascending), then c2 (ascending)
            // c1: a, a, b
            // c2: 1, 2, 3
            var result = await df.Lazy().Sort("c1", "c2").Collect();

            var resC1 = (Utf8StringSeries)result.GetColumn("c1");
            var resC2 = (Int32Series)result.GetColumn("c2");

            Assert.Equal("a", System.Text.Encoding.UTF8.GetString(resC1.GetStringSpan(0)));
            Assert.Equal(1, resC2.Memory.Span[0]);
            
            Assert.Equal("a", System.Text.Encoding.UTF8.GetString(resC1.GetStringSpan(1)));
            Assert.Equal(2, resC2.Memory.Span[1]);
            
            Assert.Equal("b", System.Text.Encoding.UTF8.GetString(resC1.GetStringSpan(2)));
            Assert.Equal(3, resC2.Memory.Span[2]);
        }

        [Fact]
        public void TestFloat64ArgSort_MixedAndSpecialValues()
        {
            double[] data = new[] { 3.14, -100.5, 0.0, -0.0, 42.0, -999.9, double.PositiveInfinity, double.NegativeInfinity, 1.0 };
            int[] idx = Compute.SortKernels.ArgSort(data);

            Assert.Equal(data.Length, idx.Length);
            for (int i = 0; i < data.Length - 1; i++)
            {
                Assert.True(data[idx[i]] <= data[idx[i + 1]], $"Ordering violation at index {i}: {data[idx[i]]} > {data[idx[i + 1]]}");
            }

            // Descending
            int[] descIdx = new int[data.Length];
            for (int i = 0; i < data.Length; i++) descIdx[i] = i;
            Compute.SortKernels.ArgSort(data, descIdx, descending: true);
            for (int i = 0; i < data.Length - 1; i++)
            {
                Assert.True(data[descIdx[i]] >= data[descIdx[i + 1]], $"Descending ordering violation at {i}: {data[descIdx[i]]} < {data[descIdx[i + 1]]}");
            }
        }

        [Fact]
        public void TestFloat64ArgSort_LargeRandomArray()
        {
            const int n = 100_000;
            var rng = new Random(12345);
            double[] data = new double[n];
            for (int i = 0; i < n; i++) data[i] = (rng.NextDouble() - 0.5) * 1_000_000;

            int[] idx = Compute.SortKernels.ArgSort(data);
            Assert.Equal(n, idx.Length);

            for (int i = 0; i < n - 1; i++)
            {
                Assert.True(data[idx[i]] <= data[idx[i + 1]], $"Large array sort failed at index {i}: {data[idx[i]]} > {data[idx[i + 1]]}");
            }
        }

        [Fact]
        public void TestFloat64ArgSort_StabilityWithTies()
        {
            double[] data = new[] { 5.0, 2.0, 5.0, 1.0, 2.0, 5.0 };
            int[] idx = Compute.SortKernels.ArgSort(data);

            // Equal 1.0 at index 3
            Assert.Equal(3, idx[0]);
            // Equal 2.0 at indices 1 and 4
            Assert.Equal(1, idx[1]);
            Assert.Equal(4, idx[2]);
            // Equal 5.0 at indices 0, 2, 5
            Assert.Equal(0, idx[3]);
            Assert.Equal(2, idx[4]);
            Assert.Equal(5, idx[5]);
        }
    }
}
