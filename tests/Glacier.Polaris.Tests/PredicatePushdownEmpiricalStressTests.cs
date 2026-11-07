namespace Glacier.Polaris.Tests;

using System;
using System.Buffers;
using Glacier.Polaris.Compute;
using Glacier.Storage.Encoding;
using Xunit;

public class PredicatePushdownEmpiricalStressTests
{
    private static readonly FilterOperation[] s_allOps =
    [
        FilterOperation.Equal,
        FilterOperation.NotEqual,
        FilterOperation.LessThan,
        FilterOperation.LessThanOrEqual,
        FilterOperation.GreaterThan,
        FilterOperation.GreaterThanOrEqual
    ];

    private static bool Ieee754Matches(float val, float thresh, FilterOperation op) => op switch
    {
        FilterOperation.Equal => val == thresh,
        FilterOperation.NotEqual => val != thresh,
        FilterOperation.LessThan => val < thresh,
        FilterOperation.LessThanOrEqual => val <= thresh,
        FilterOperation.GreaterThan => val > thresh,
        FilterOperation.GreaterThanOrEqual => val >= thresh,
        _ => false
    };

    private static bool Ieee754MatchesDouble(double val, double thresh, FilterOperation op) => op switch
    {
        FilterOperation.Equal => val == thresh,
        FilterOperation.NotEqual => val != thresh,
        FilterOperation.LessThan => val < thresh,
        FilterOperation.LessThanOrEqual => val <= thresh,
        FilterOperation.GreaterThan => val > thresh,
        FilterOperation.GreaterThanOrEqual => val >= thresh,
        _ => false
    };

    [Fact]
    public void EvaluateCompressedBlock_PositiveInfinityThreshold_LessThan_FailsDueToIntegerOverflow()
    {
        // When block contains negative floats, minBase is negative, so long.MaxValue - minBase overflows to negative
        float[] values = [-50.0f, -10.0f, 10.0f, 20.0f, float.PositiveInfinity, float.NegativeInfinity, float.NaN];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: -50, -10, 10, 20, and -Inf are all < +Infinity (5 matches: indices 0, 1, 2, 3, 5)
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.PositiveInfinity,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(5, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_PositiveInfinityThreshold_LessThanOrEqual_FailsDueToIntegerOverflow()
    {
        float[] values = [-50.0f, -10.0f, 10.0f, 20.0f, float.PositiveInfinity, float.NegativeInfinity, float.NaN];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: -50, -10, 10, 20, +Inf, -Inf are all <= +Infinity (6 matches: indices 0, 1, 2, 3, 4, 5)
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.PositiveInfinity,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(6, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NegativeInfinityThreshold_GreaterThan_FailsDueToIntegerOverflow()
    {
        float[] values = [10.0f, 20.0f, 30.0f, float.PositiveInfinity, float.NegativeInfinity, float.NaN];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: 10, 20, 30, and +Inf are all > -Infinity (4 matches: indices 0, 1, 2, 3)
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NegativeInfinity,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(4, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NegativeInfinityThreshold_GreaterThanOrEqual_FailsDueToIntegerOverflow()
    {
        float[] values = [10.0f, 20.0f, 30.0f, float.PositiveInfinity, float.NegativeInfinity, float.NaN];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: 10, 20, 30, +Inf, -Inf are all >= -Infinity (5 matches: indices 0, 1, 2, 3, 4)
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NegativeInfinity,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(5, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NaNThreshold_MustNeverMatchGreaterThan()
    {
        float[] values = [10.0f, 20.0f, 30.0f, 40.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: x > NaN is FALSE for all floats!
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NaN,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(0, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NaNThreshold_MustNeverMatchGreaterThanOrEqual()
    {
        float[] values = [10.0f, 20.0f, 30.0f, 40.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: x >= NaN is FALSE for all floats!
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NaN,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(0, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NaNThreshold_MustNeverMatchEqual()
    {
        float[] values = [10.0f, 20.0f, 30.0f, 40.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: x == NaN is FALSE for all floats!
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NaN,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(0, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NaNThreshold_MustMatchAllOnNotEqual()
    {
        float[] values = [10.0f, 20.0f, 30.0f, 40.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // In IEEE 754: x != NaN is TRUE for all floats!
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NaN,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(4, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_NonFiniteThresholds_AllMatchAgainstOracle()
    {
        double[] values = [10.0, 20.0, 30.0, double.PositiveInfinity, double.NegativeInfinity, double.NaN];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];
        Span<int> expectedIndices = stackalloc int[values.Length];
        double[] thresholds = [double.PositiveInfinity, double.NegativeInfinity, double.NaN];

        foreach (double thresh in thresholds)
        {
            foreach (var op in s_allOps)
            {
                int expectedCount = 0;
                for (int i = 0; i < values.Length; i++)
                {
                    if (Ieee754MatchesDouble(values[i], thresh, op))
                    {
                        expectedIndices[expectedCount++] = i;
                    }
                }

                int compCount = FilterKernels.EvaluateCompressedBlockDouble(
                    compressed.AsSpan(0, written),
                    thresh,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    compIndices);

                Assert.True(
                    expectedCount == compCount,
                    $"Double count mismatch for thresh={thresh}, op={op}: expected {expectedCount}, actual {compCount}");

                for (int i = 0; i < expectedCount; i++)
                {
                    Assert.True(
                        expectedIndices[i] == compIndices[i],
                        $"Double index mismatch at {i} for thresh={thresh}, op={op}: expected {expectedIndices[i]}, actual {compIndices[i]}");
                }
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlock_WithValidityMask_NonFiniteExceptionsMaskedOut()
    {
        float[] values = [10.0f, 20.0f, float.PositiveInfinity, float.NegativeInfinity];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        ulong[] validityMask = [0b1011UL];

        Span<int> compIndices = stackalloc int[values.Length];

        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            15.0f,
            FilterOperation.GreaterThan,
            validityMask,
            compIndices);

        Assert.Equal(1, compCount);
        Assert.Equal(1, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlock_SteadyState_ZeroHeapAllocations_AcrossAllOperators()
    {
        int count = 256;
        float[] values = new float[count];
        for (int i = 0; i < count; i++) values[i] = (float)Math.Round(1.0 + (i * 0.5), 1);
        values[10] = float.PositiveInfinity;
        values[50] = float.NegativeInfinity;
        values[100] = float.NaN;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        int wordCount = (count + 63) >> 6;
        Span<ulong> bitmask = stackalloc ulong[wordCount];

        foreach (var op in s_allOps)
        {
            // Warmup JIT
            for (int i = 0; i < 100; i++)
            {
                FilterKernels.EvaluateCompressedBlockToBitmap(
                    compressed.AsSpan(0, written),
                    50.0f,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    bitmask);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 50; i++)
            {
                FilterKernels.EvaluateCompressedBlockToBitmap(
                    compressed.AsSpan(0, written),
                    50.0f,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    bitmask);
            }
            long after = GC.GetAllocatedBytesForCurrentThread();
            long diff = after - before;

            Assert.True(diff == 0, $"Op {op} had {diff} heap allocations in steady-state!");
        }
    }
}
