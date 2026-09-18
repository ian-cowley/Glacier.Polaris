using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class NullTests
    {
        [Fact]
        public async Task TestNullPropagation()
        {
            var a = new Int32Series("a", new[] { 1, 2, 3 });
            a.ValidityMask.SetNull(1); // Row 1 is null

            var df = new DataFrame(new System.Collections.Generic.List<ISeries> { a });
            
            var lf = df.Lazy()
                .Select(Expr.Col("a") + 10);

            var result = await lf.Collect();
            var resCol = (Int32Series)result.GetColumn("res");

            Assert.True(resCol.ValidityMask.IsValid(0));
            Assert.True(resCol.ValidityMask.IsNull(1));
            Assert.True(resCol.ValidityMask.IsValid(2));
            
            Assert.Equal(11, resCol.Memory.Span[0]);
            Assert.Equal(13, resCol.Memory.Span[2]);
        }

        [Fact]
        public async Task TestIsNullExpression()
        {
            var a = new Int32Series("a", new[] { 1, 2, 3 });
            a.ValidityMask.SetNull(1);

            var df = new DataFrame(new System.Collections.Generic.List<ISeries> { a });
            
            var lf = df.Lazy()
                .Select(Expr.Col("a").IsNull().Alias("is_null"), 
                        Expr.Col("a").IsNotNull().Alias("is_not_null"));

            var result = await lf.Collect();
            
            var isNull = (BooleanSeries)result.GetColumn("is_null");
            var isNotNull = (BooleanSeries)result.GetColumn("is_not_null");

            Assert.False(isNull.Memory.Span[0]);
            Assert.True(isNull.Memory.Span[1]);
            Assert.False(isNull.Memory.Span[2]);

            Assert.True(isNotNull.Memory.Span[0]);
            Assert.False(isNotNull.Memory.Span[1]);
            Assert.True(isNotNull.Memory.Span[2]);
        }

        [Fact]
        public void TestCategoricalCreation()
        {
            var strings = new[] { "a", "b", null, "a" };
            var cat = CategoricalSeries.FromStrings("cat", strings);

            Assert.Equal(4, cat.Length);
            Assert.Equal(2, cat.RevMap.Length); // "a", "b"
            Assert.Equal(0u, cat.Memory.Span[0]);
            Assert.Equal(1u, cat.Memory.Span[1]);
            Assert.True(cat.ValidityMask.IsNull(2));
            Assert.Equal(0u, cat.Memory.Span[3]);
        }
        [Fact]
        public async Task TestCategoricalCast()
        {
            var df = new DataFrame(new System.Collections.Generic.List<ISeries>
            {
                new Utf8StringSeries("s", new[] { "apple", "banana", "apple", "cherry" })
            });

            var lf = df.Lazy()
                .Select(Expr.Col("s").Cast(typeof(CategoricalSeries)).Alias("c"));

            var result = await lf.Collect();
            var cat = (CategoricalSeries)result.GetColumn("c");

            Assert.Equal(4, cat.Length);
            Assert.Equal(3, cat.RevMap.Length); // apple, banana, cherry
            Assert.Equal(0u, cat.Memory.Span[0]); // apple
            Assert.Equal(1u, cat.Memory.Span[1]); // banana
            Assert.Equal(0u, cat.Memory.Span[2]); // apple
            Assert.Equal(2u, cat.Memory.Span[3]); // cherry
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(250)]
        [InlineData(1000)]
        public void TestInt32Series_SimdSumAndMean_WithNulls(int length)
        {
            var rawData = new int[length];
            for (int i = 0; i < length; i++)
            {
                rawData[i] = (i + 1) * 3;
            }

            var series = new Int32Series("data", rawData);

            // Invalidate multiple entries across vector lanes
            for (int i = 0; i < length; i++)
            {
                if (i % 3 == 0 || i % 7 == 0)
                {
                    series.ValidityMask.SetNull(i);
                }
            }

            // Reference calculation using scalar LINQ over non-null elements
            var validElements = rawData.Where((val, idx) => series.ValidityMask.IsValid(idx)).ToList();
            long expectedSum = validElements.Select(x => (long)x).Sum();
            double expectedMean = validElements.Count > 0 ? (double)expectedSum / validElements.Count : 0.0;

            var sumSeries = (Int32Series)Glacier.Polaris.Compute.AggregationKernels.Sum(series);
            var meanSeries = (Float64Series)Glacier.Polaris.Compute.AggregationKernels.Mean(series);

            Assert.Equal((int)expectedSum, sumSeries.Memory.Span[0]);
            if (validElements.Count > 0)
            {
                Assert.True(meanSeries.ValidityMask.IsValid(0));
                Assert.Equal(expectedMean, meanSeries.Memory.Span[0], precision: 6);
            }
            else
            {
                Assert.True(meanSeries.ValidityMask.IsNull(0));
            }
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(32)]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(250)]
        [InlineData(1000)]
        public void TestFloat64Series_SimdSumAndMean_WithNulls(int length)
        {
            var rawData = new double[length];
            for (int i = 0; i < length; i++)
            {
                rawData[i] = (i + 1) * 1.5;
            }

            var series = new Float64Series("data", rawData);

            // Invalidate multiple entries across vector lanes
            for (int i = 0; i < length; i++)
            {
                if (i % 4 == 0 || i % 5 == 0)
                {
                    series.ValidityMask.SetNull(i);
                }
            }

            var validElements = rawData.Where((val, idx) => series.ValidityMask.IsValid(idx)).ToList();
            double expectedSum = validElements.Sum();
            double expectedMean = validElements.Count > 0 ? expectedSum / validElements.Count : 0.0;

            var sumSeries = (Float64Series)Glacier.Polaris.Compute.AggregationKernels.Sum(series);
            var meanSeries = (Float64Series)Glacier.Polaris.Compute.AggregationKernels.Mean(series);

            Assert.Equal(expectedSum, sumSeries.Memory.Span[0], precision: 6);
            if (validElements.Count > 0)
            {
                Assert.True(meanSeries.ValidityMask.IsValid(0));
                Assert.Equal(expectedMean, meanSeries.Memory.Span[0], precision: 6);
            }
            else
            {
                Assert.True(meanSeries.ValidityMask.IsNull(0));
            }
        }

        [Fact]
        public void TestAllNull_SimdSumAndMean()
        {
            int length = 64;
            var rawData = new int[length];
            Array.Fill(rawData, 42);

            var series = new Int32Series("all_null", rawData);
            series.ValidityMask.SetAllNull();

            var sumSeries = (Int32Series)Glacier.Polaris.Compute.AggregationKernels.Sum(series);
            var meanSeries = (Float64Series)Glacier.Polaris.Compute.AggregationKernels.Mean(series);

            Assert.Equal(0, sumSeries.Memory.Span[0]);
            Assert.True(meanSeries.ValidityMask.IsNull(0));
        }

        [Fact]
        public async Task TestDataFrameAggregation_WithSimdNulls()
        {
            int length = 128;
            var rawInts = new int[length];
            for (int i = 0; i < length; i++) rawInts[i] = i * 2;
            var sInt = new Int32Series("a", rawInts);

            for (int i = 0; i < length; i += 2)
                sInt.ValidityMask.SetNull(i);

            var df = new DataFrame(new System.Collections.Generic.List<ISeries> { sInt });
            var lf = df.Lazy().Select(Expr.Col("a").Sum().Alias("sum_a"));

            var result = await lf.Collect();
            var resCol = (Int32Series)result.GetColumn("sum_a");

            int expected = rawInts.Where((v, idx) => idx % 2 != 0).Sum();
            Assert.Equal(expected, resCol.Memory.Span[0]);
        }
    }
}
