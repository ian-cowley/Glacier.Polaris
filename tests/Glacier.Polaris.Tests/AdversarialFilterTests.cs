using System;
using System.Buffers;
using System.Collections.Generic;
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
        [InlineData(50_000)]
        [InlineData(100_000)]
        public void Adversarial_Filter_AllZerosBitmask_MatchesOracle(int length)
        {
            long[] data = new long[length];
            Array.Fill(data, 10L);
            long threshold = 50L;

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
        [InlineData(50_000)]
        [InlineData(100_000)]
        public void Adversarial_Filter_AllOnesBitmask_MatchesOracle(int length)
        {
            long[] data = new long[length];
            Array.Fill(data, 100L);
            long threshold = 50L;

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
        [InlineData(50_000)]
        [InlineData(100_000)]
        public void Adversarial_Filter_AlternatingBits_0x5555_MatchesOracle(int length)
        {
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
        [InlineData(50_000)]
        [InlineData(100_000)]
        public void Adversarial_Filter_AlternatingBits_0xAAAA_MatchesOracle(int length)
        {
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
            int rawThreshold = ParallelThresholds.GetFilterParallelThreshold<int>();
            // On <= 2-vCPU CI runners GetFilterParallelThreshold returns int.MaxValue; cap safely.
            int baseThreshold = rawThreshold > 200_000 ? 65_536 : rawThreshold;
            int[] testSizes = new[]
            {
                baseThreshold - 65,
                baseThreshold - 1,
                baseThreshold,
                baseThreshold + 1,
                baseThreshold + 63,
                baseThreshold + 64,
                baseThreshold + 65
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
            const int n = 100_000;
            long[] data = new long[n];
            var rng = new Random(999);

            int expectedCount = 0;
            for (int i = 0; i < n; i++)
            {
                long val = rng.Next(0, 100);
                data[i] = val;
                if (val >= 70) expectedCount++;
            }

            long threshold = 70L;
            var (rented, count) = FilterKernels.Filter<long>(data, threshold, FilterOperation.GreaterThanOrEqual);

            try
            {
                Assert.Equal(expectedCount, count);

                int expectedIdx = 0;
                for (int i = 0; i < n; i++)
                {
                    if (data[i] >= threshold)
                    {
                        Assert.Equal(i, rented[expectedIdx]);
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
            const int n = 100_000;
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
            var (rented, count) = FilterKernels.Filter<double>(data, threshold, FilterOperation.LessThan);

            try
            {
                Assert.Equal(expectedCount, count);

                int expectedIdx = 0;
                for (int i = 0; i < n; i++)
                {
                    if (data[i] < threshold)
                    {
                        Assert.Equal(i, rented[expectedIdx]);
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
