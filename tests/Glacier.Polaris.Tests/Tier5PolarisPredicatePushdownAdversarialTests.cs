namespace Glacier.Polaris.Tests;

using System;
using System.Buffers;
using Glacier.Polaris.Compute;
using Glacier.Storage.Encoding;
using Xunit;

/// <summary>
/// Tier-5 White-Box Adversarial Coverage Hardening for Glacier.Polaris.
/// Stress-tests predicate pushdown on compressed blocks across edge-case IEEE 754 boundaries,
/// non-finite exceptions, and validates zero steady-state heap allocations.
/// </summary>
public class Tier5PolarisPredicatePushdownAdversarialTests
{
    private static readonly FilterOperation[] s_allOps =
    [
        FilterOperation.GreaterThan,
        FilterOperation.GreaterThanOrEqual,
        FilterOperation.LessThan,
        FilterOperation.LessThanOrEqual,
        FilterOperation.Equal,
        FilterOperation.NotEqual
    ];

    [Fact]
    public void ZoneMapPruning_ExhaustiveBoundaryTruthTable()
    {
        const float min = 10.0f;
        const float max = 50.0f;
        const int count = 100;

        // 1. Constant strictly below min (c = 5.0f)
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 5.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 5.0f, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 5.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 5.0f, FilterOperation.LessThanOrEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 5.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 5.0f, FilterOperation.NotEqual));

        // 2. Constant strictly above max (c = 60.0f)
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 60.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 60.0f, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 60.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 60.0f, FilterOperation.LessThanOrEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 60.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 60.0f, FilterOperation.NotEqual));

        // 3. Constant exact at min (c = 10.0f)
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.LessThanOrEqual));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.NotEqual));

        // 4. Constant exact at max (c = 50.0f)
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.LessThanOrEqual));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.NotEqual));
    }

    [Fact]
    public void ZoneMapPruning_Ieee754NonFiniteBoundaries()
    {
        const float min = 10.0f;
        const float max = 50.0f;
        const int count = 100;

        // NaN threshold: NotEqual returns AllMatch; all others return Skip
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, float.NaN, FilterOperation.NotEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NaN, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NaN, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NaN, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NaN, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NaN, FilterOperation.LessThanOrEqual));

        // Positive Infinity threshold
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.PositiveInfinity, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.PositiveInfinity, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, float.PositiveInfinity, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, float.PositiveInfinity, FilterOperation.LessThanOrEqual));

        // Negative Infinity threshold
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, float.NegativeInfinity, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, float.NegativeInfinity, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NegativeInfinity, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, float.NegativeInfinity, FilterOperation.LessThanOrEqual));
    }

    [Fact]
    public void PredicatePushdown_MassiveThresholdExceedingInt64_DoesNotCrashAndMatchesIeee754()
    {
        // Block has finite floats [10.0f, 20.0f, 30.0f]
        float[] values = [10.0f, 20.0f, 30.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        Span<int> compIndices = stackalloc int[values.Length];

        // 1. Huge threshold: 1e30f
        // In IEEE 754: all values are < 1e30f
        int count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e30f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(3, count);

        // In IEEE 754: no value is > 1e30f
        count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e30f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(0, count);

        // 2. Very small negative threshold: -1e30f
        // In IEEE 754: all values are > -1e30f
        count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            -1e30f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(3, count);

        // In IEEE 754: no value is < -1e30f
        count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            -1e30f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(0, count);
    }

    [Fact]
    public void PredicatePushdown_SignedZero_EqualAndComparisonSemantics()
    {
        // IEEE 754: +0.0f == -0.0f is TRUE
        float[] values = [+0.0f, -0.0f, 5.0f, -5.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        Span<int> compIndices = stackalloc int[values.Length];

        // x == +0.0f: matches indices 0 and 1
        int count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            0.0f,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, count);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);

        // x == -0.0f: also matches indices 0 and 1 in IEEE 754
        count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            -0.0f,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, count);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);

        // x > 0.0f: matches index 2 (5.0f)
        count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            0.0f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, count);
        Assert.Equal(2, compIndices[0]);

        // x < 0.0f: matches index 3 (-5.0f)
        count = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            0.0f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, count);
        Assert.Equal(3, compIndices[0]);
    }

    [Fact]
    public void PredicatePushdown_ZeroSteadyStateHeapAllocations_BitmapAndIndices_AllOperators()
    {
        const int count = 256;
        float[] values = new float[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (float)Math.Round(-20.0 + (i * 0.25), 2);
        }
        // Insert non-finite edge cases
        values[10] = float.PositiveInfinity;
        values[20] = float.NegativeInfinity;
        values[30] = float.NaN;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        int wordCount = (count + 63) >> 6;
        Span<ulong> bitmask = stackalloc ulong[wordCount];
        Span<int> matchingIndices = stackalloc int[count];

        foreach (var op in s_allOps)
        {
            // 1. Warmup JIT and internal caches
            for (int w = 0; w < 100; w++)
            {
                FilterKernels.EvaluateCompressedBlockToBitmap(
                    compressed.AsSpan(0, written),
                    12.5f,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    bitmask);

                FilterKernels.EvaluateCompressedBlock(
                    compressed.AsSpan(0, written),
                    12.5f,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    matchingIndices);
            }

            // 2. Measure steady-state allocations across 50 iterations
            long allocBeforeBitmap = GC.GetAllocatedBytesForCurrentThread();
            for (int iter = 0; iter < 50; iter++)
            {
                FilterKernels.EvaluateCompressedBlockToBitmap(
                    compressed.AsSpan(0, written),
                    12.5f,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    bitmask);
            }
            long allocAfterBitmap = GC.GetAllocatedBytesForCurrentThread();
            long diffBitmap = allocAfterBitmap - allocBeforeBitmap;

            Assert.True(diffBitmap == 0,
                $"EvaluateCompressedBlockToBitmap with op {op} allocated {diffBitmap} bytes in steady state!");

            long allocBeforeIndices = GC.GetAllocatedBytesForCurrentThread();
            for (int iter = 0; iter < 50; iter++)
            {
                FilterKernels.EvaluateCompressedBlock(
                    compressed.AsSpan(0, written),
                    12.5f,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    matchingIndices);
            }
            long allocAfterIndices = GC.GetAllocatedBytesForCurrentThread();
            long diffIndices = allocAfterIndices - allocBeforeIndices;

            Assert.True(diffIndices == 0,
                $"EvaluateCompressedBlock with op {op} allocated {diffIndices} bytes in steady state!");
        }
    }
}
