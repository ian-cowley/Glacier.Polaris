using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Glacier.Polaris.Compute;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class AdversarialFilterTests
    {
        private static int[] RunOracle<T>(ReadOnlySpan<T> data, T threshold, FilterOperation op) where T : IComparable<T>
        {
            var result = new List<int>();
            for (int i = 0; i < data.Length; i++)
            {
                int cmp = data[i].CompareTo(threshold);
                bool match = op switch
                {
                    FilterOperation.Equal => cmp == 0,
                    FilterOperation.NotEqual => cmp != 0,
                    FilterOperation.GreaterThan => cmp > 0,
                    FilterOperation.GreaterThanOrEqual => cmp >= 0,
                    FilterOperation.LessThan => cmp < 0,
                    FilterOperation.LessThanOrEqual => cmp <= 0,
                    _ => false
                };
                if (match) result.Add(i);
            }
            return result.ToArray();
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(63)]
        [InlineData(64)]
        [InlineData(65)]
        [InlineData(127)]
        [InlineData(128)]
        [InlineData(129)]
        public void Adversarial_Filter_BoundarySizes_MatchesOracle_AcrossOperations(int length)
        {
            var ops = new[]
            {
                FilterOperation.Equal,
                FilterOperation.NotEqual,
                FilterOperation.GreaterThan,
                FilterOperation.GreaterThanOrEqual,
                FilterOperation.LessThan,
                FilterOperation.LessThanOrEqual
            };

            var rng = new Random(42 + length);
            int[] data = new int[length];
            for (int i = 0; i < length; i++)
            {
                data[i] = rng.Next(0, 10);
            }
            int threshold = 5;

            foreach (var op in ops)
            {
                var (rented, count) = FilterKernels.Filter<int>(data, threshold, op);
                try
                {
                    var oracle = RunOracle<int>(data, threshold, op);
                    Assert.Equal(oracle.Length, count);
                    for (int i = 0; i < count; i++)
                    {
                        Assert.Equal(oracle[i], rented[i]);
                    }
                }
                finally
                {
                    ArrayPool<int>.Shared.Return(rented);
                }
            }
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(300_000)]
        [InlineData(1_000_000)]
        public void Adversarial_Filter_AllZerosBitmask_MatchesOracle(int length)
        {
            // All-zeros bitmask: 0 matching elements
            long[] data = new long[length];
            Array.Fill(data, 10L);
            long threshold = 50L; // predicate: > 50 -> 0 matches

            var (rented, count) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThan);
            try
            {
                Assert.Equal(0, count);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(300_000)]
        [InlineData(1_000_000)]
        public void Adversarial_Filter_AllOnesBitmask_MatchesOracle(int length)
        {
            // All-ones bitmask: 100% matching elements (exercises 4x Vector512<int> bulk store)
            long[] data = new long[length];
            Array.Fill(data, 100L);
            long threshold = 50L; // predicate: > 50 -> 100% matches

            var (rented, count) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThan);
            try
            {
                Assert.Equal(length, count);
                for (int i = 0; i < length; i++)
                {
                    Assert.Equal(i, rented[i]);
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(300_000)]
        [InlineData(1_000_000)]
        public void Adversarial_Filter_AlternatingBits_0x5555_MatchesOracle(int length)
        {
            // Alternating bits: even indices match, odd indices do not (0x5555... bitmask)
            long[] data = new long[length];
            for (int i = 0; i < length; i++)
            {
                data[i] = (i % 2 == 0) ? 100L : 10L;
            }
            long threshold = 50L;

            var (rented, count) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThan);
            try
            {
                var oracle = RunOracle<long>(data, threshold, FilterOperation.GreaterThan);
                Assert.Equal(oracle.Length, count);
                for (int i = 0; i < count; i++)
                {
                    Assert.Equal(oracle[i], rented[i]);
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }

        [Theory]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(1024)]
        [InlineData(300_000)]
        [InlineData(1_000_000)]
        public void Adversarial_Filter_AlternatingBits_0xAAAA_MatchesOracle(int length)
        {
            // Alternating bits: odd indices match, even indices do not (0xAAAA... bitmask)
            long[] data = new long[length];
            for (int i = 0; i < length; i++)
            {
                data[i] = (i % 2 == 1) ? 100L : 10L;
            }
            long threshold = 50L;

            var (rented, count) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThan);
            try
            {
                var oracle = RunOracle<long>(data, threshold, FilterOperation.GreaterThan);
                Assert.Equal(oracle.Length, count);
                for (int i = 0; i < count; i++)
                {
                    Assert.Equal(oracle[i], rented[i]);
                }
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }

        [Fact]
        public void Adversarial_Filter_ParallelThresholdBoundaries_MatchesOracle()
        {
            int parallelThreshold = ParallelThresholds.GetFilterParallelThreshold<int>();
            int[] testSizes = new[]
            {
                parallelThreshold - 65,
                parallelThreshold - 1,
                parallelThreshold,
                parallelThreshold + 1,
                parallelThreshold + 63,
                parallelThreshold + 64,
                parallelThreshold + 65
            };

            var rng = new Random(12345);
            foreach (int size in testSizes)
            {
                if (size <= 0) continue;
                int[] data = new int[size];
                for (int i = 0; i < size; i++)
                {
                    data[i] = rng.Next(-100, 100);
                }
                int threshold = 0;

                var (rented, count) = FilterKernels.Filter<int>(data, threshold, FilterOperation.GreaterThan);
                try
                {
                    var oracle = RunOracle<int>(data, threshold, FilterOperation.GreaterThan);
                    Assert.Equal(oracle.Length, count);
                    for (int i = 0; i < count; i++)
                    {
                        Assert.Equal(oracle[i], rented[i]);
                    }
                }
                finally
                {
                    ArrayPool<int>.Shared.Return(rented);
                }
            }
        }

        [Fact]
        public void Adversarial_Filter_10Million_Int64_StressAndCorrectness()
        {
            const int n = 10_000_000;
            long[] data = new long[n];
            var rng = new Random(999);

            // Populate with known distribution: ~30% elements match
            int expectedCount = 0;
            for (int i = 0; i < n; i++)
            {
                long val = rng.Next(0, 100);
                data[i] = val;
                if (val >= 70) expectedCount++;
            }

            long threshold = 70L;
            var sw = Stopwatch.StartNew();
            var (rented, count) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThanOrEqual);
            sw.Stop();

            try
            {
                Assert.Equal(expectedCount, count);

                // Spot-check first 10,000, last 10,000, and 10,000 in the middle
                int expectedIdx = 0;
                for (int i = 0; i < n; i++)
                {
                    if (data[i] >= threshold)
                    {
                        if (expectedIdx < 10_000 || (expectedIdx >= count - 10_000) || (expectedIdx % 1000 == 0))
                        {
                            Assert.Equal(i, rented[expectedIdx]);
                        }
                        expectedIdx++;
                    }
                }
                Assert.Equal(expectedCount, expectedIdx);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }

        [Fact]
        public void Adversarial_Filter_10Million_Float64_StressAndCorrectness()
        {
            const int n = 10_000_000;
            double[] data = new double[n];
            var rng = new Random(888);

            int expectedCount = 0;
            for (int i = 0; i < n; i++)
            {
                double val = rng.NextDouble();
                data[i] = val;
                if (val < 0.25) expectedCount++;
            }

            double threshold = 0.25;
            var sw = Stopwatch.StartNew();
            var (rented, count) = FilterKernels.Filter<double>(data, threshold, FilterOperation.LessThan);
            sw.Stop();

            try
            {
                Assert.Equal(expectedCount, count);

                int expectedIdx = 0;
                for (int i = 0; i < n; i++)
                {
                    if (data[i] < threshold)
                    {
                        if (expectedIdx < 5_000 || (expectedIdx >= count - 5_000) || (expectedIdx % 2000 == 0))
                        {
                            Assert.Equal(i, rented[expectedIdx]);
                        }
                        expectedIdx++;
                    }
                }
                Assert.Equal(expectedCount, expectedIdx);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }
    }
}
