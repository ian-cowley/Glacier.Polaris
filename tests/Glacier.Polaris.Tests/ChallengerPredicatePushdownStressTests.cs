using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using Glacier.Polaris.Compute;
using Glacier.Storage.Encoding;
using Xunit;

namespace Glacier.Polaris.Tests;

[CollectionDefinition("ZeroAllocTests", DisableParallelization = true)]
public sealed class ZeroAllocTestsCollectionDefinition
{
}

[Collection("ZeroAllocTests")]
public class ChallengerPredicatePushdownStressTests
{
    [Fact]
    public void PruneBlock_EdgeEquality_MatchesSemanticDefinition()
    {
        float min = 10.0f;
        float max = 50.0f;
        int count = 100;

        // GreaterThan at exact max: C == 50.0f -> Skip (no value can be > 50.0)
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.GreaterThan));

        // GreaterThan at exact min: C == 10.0f -> Partial (values > 10.0 might exist)
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.GreaterThan));

        // GreaterThanOrEqual at exact max: C == 50.0f -> Partial (values == 50.0 can match)
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.GreaterThanOrEqual));

        // GreaterThanOrEqual at exact min: C == 10.0f -> AllMatch (all values in [10, 50] are >= 10.0)
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.GreaterThanOrEqual));

        // LessThan at exact min: C == 10.0f -> Skip (no value in [10, 50] is < 10.0)
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.LessThan));

        // LessThan at exact max: C == 50.0f -> Partial (values < 50.0 might exist)
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.LessThan));

        // LessThanOrEqual at exact min: C == 10.0f -> Partial (values == 10.0 can match)
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.LessThanOrEqual));

        // LessThanOrEqual at exact max: C == 50.0f -> AllMatch (all values in [10, 50] are <= 50.0)
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.LessThanOrEqual));

        // Equal at exact min: C == 10.0f -> Partial
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 10.0f, FilterOperation.Equal));

        // Equal at exact max: C == 50.0f -> Partial
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 0, 50.0f, FilterOperation.Equal));

        // Equal on single-value block: min == max == 10.0f
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(10.0f, 10.0f, count, 0, 10.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(10.0f, 10.0f, count, 0, 15.0f, FilterOperation.Equal));

        // NotEqual on single-value block: min == max == 10.0f
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(10.0f, 10.0f, count, 0, 10.0f, FilterOperation.NotEqual));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(10.0f, 10.0f, count, 0, 15.0f, FilterOperation.NotEqual));
    }

    [Fact]
    public void PruneBlock_NullAndEmptyBlocks_AlwaysReturnSkip()
    {
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(10.0f, 50.0f, rowCount: 0, nullCount: 0, 20.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(10.0f, 50.0f, rowCount: 100, nullCount: 100, 20.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(10.0f, 50.0f, rowCount: 100, nullCount: 100, 20.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(10.0f, 50.0f, rowCount: 100, nullCount: 100, 20.0f, FilterOperation.NotEqual));
    }

    [Fact]
    public void EvaluateCompressedBlock_PositiveInfinityException_MustNotBePrunedByZoneMap()
    {
        // Block has finite values [10.0f, 20.0f, 30.0f] and an exception +Infinity
        float[] values = [10.0f, 20.0f, 30.0f, float.PositiveInfinity];

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);
        Assert.True(exCount > 0);

        // Query: x > 100.0f
        // In IEEE 754: 10.0 > 100 (F), 20.0 > 100 (F), 30.0 > 100 (F), +Inf > 100 (TRUE)
        // Expected matching count: 1 (index 3)
        var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(values, 100.0f, FilterOperation.GreaterThan);
        Assert.Equal(1, uncompCount);
        Assert.Equal(3, uncompIndices[0]);
        ArrayPool<int>.Shared.Return(uncompIndices);

        Span<int> compIndices = stackalloc int[values.Length];
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            100.0f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        // If zone-map pruning erroneously pruned the block because maxFinite (30.0f) <= 100.0f,
        // compCount will be 0 instead of 1!
        Assert.True(compCount == 1, $"Positive infinity was dropped by zone-map pruning! Expected 1 match, got {compCount}");
        Assert.Equal(3, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlock_NegativeInfinityException_MustNotBePrunedByZoneMap()
    {
        // Block has finite values [10.0f, 20.0f, 30.0f] and an exception -Infinity
        float[] values = [10.0f, 20.0f, 30.0f, float.NegativeInfinity];

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);
        Assert.True(exCount > 0);

        // Query: x < 0.0f
        // In IEEE 754: -Inf < 0.0f is TRUE
        var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(values, 0.0f, FilterOperation.LessThan);
        Assert.Equal(1, uncompCount);
        Assert.Equal(3, uncompIndices[0]);
        ArrayPool<int>.Shared.Return(uncompIndices);

        Span<int> compIndices = stackalloc int[values.Length];
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            0.0f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.True(compCount == 1, $"Negative infinity was dropped by zone-map pruning! Expected 1 match, got {compCount}");
        Assert.Equal(3, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlock_InfiniteThreshold_EvaluatesCorrectly()
    {
        float[] values = [10.0f, 20.0f, 30.0f, float.PositiveInfinity];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // x >= +Infinity -> exactly 1 match (index 3)
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.PositiveInfinity,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(3, compIndices[0]);

        // x == +Infinity -> exactly 1 match (index 3)
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.PositiveInfinity,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(3, compIndices[0]);

        // x > +Infinity -> 0 matches in IEEE 754 (+Inf is not > +Inf)
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.PositiveInfinity,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(0, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlock_NegativeInfiniteThreshold_EvaluatesCorrectly()
    {
        float[] values = [10.0f, 20.0f, 30.0f, float.NegativeInfinity];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // x <= -Infinity -> exactly 1 match (index 3)
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NegativeInfinity,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(3, compIndices[0]);

        // x == -Infinity -> exactly 1 match (index 3)
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NegativeInfinity,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(3, compIndices[0]);

        // x < -Infinity -> 0 matches in IEEE 754 (-Inf is not < -Inf)
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            float.NegativeInfinity,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(0, compCount);
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_NonFiniteExceptions_MustNotBePrunedByZoneMap()
    {
        // Double with +Infinity
        double[] valuesPos = [10.0, 20.0, 30.0, double.PositiveInfinity];
        byte[] compPos = new byte[AlpCompressor.GetMaxCompressedLength(valuesPos.Length, sizeof(double))];
        int writtenPos = AlpCompressor.CompressDouble(valuesPos, compPos, out int exCountPos);
        Assert.True(exCountPos > 0);

        Span<int> compIndices = stackalloc int[valuesPos.Length];
        int compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compPos.AsSpan(0, writtenPos),
            100.0,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(3, compIndices[0]);

        // Double with -Infinity
        double[] valuesNeg = [10.0, 20.0, 30.0, double.NegativeInfinity];
        byte[] compNeg = new byte[AlpCompressor.GetMaxCompressedLength(valuesNeg.Length, sizeof(double))];
        int writtenNeg = AlpCompressor.CompressDouble(valuesNeg, compNeg, out int exCountNeg);
        Assert.True(exCountNeg > 0);

        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compNeg.AsSpan(0, writtenNeg),
            0.0,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(3, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlock_AllMatchWithNaNException_ClearsNaNBit()
    {
        // Block has finite values [10.0f, 20.0f, 30.0f] and float.NaN
        float[] values = [10.0f, 20.0f, 30.0f, float.NaN];
        byte[] comp = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, comp, out int exCount);
        Assert.True(exCount > 0);

        // Query: x > 5.0f -> Zone-map returns AllMatch for finite [10, 30]
        // But NaN > 5.0f is FALSE in IEEE 754
        Span<int> compIndices = stackalloc int[values.Length];
        int compCount = FilterKernels.EvaluateCompressedBlock(
            comp.AsSpan(0, written),
            5.0f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(3, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);
        Assert.Equal(2, compIndices[2]);
    }

    [Fact]
    public void EvaluateCompressedBlock_NegativeFloatsAndThresholds_ParityWithUncompressed()
    {
        int count = 256;
        float[] values = new float[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (float)Math.Round(-50.0 + (i * 0.4), 1); // -50.0 to +52.0
        }

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        float[] testThresholds = [-25.5f, -10.0f, 0.0f, 15.5f];
        FilterOperation[] ops =
        [
            FilterOperation.GreaterThan,
            FilterOperation.GreaterThanOrEqual,
            FilterOperation.LessThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.Equal,
            FilterOperation.NotEqual
        ];

        Span<int> compIndices = stackalloc int[count];

        foreach (float thresh in testThresholds)
        {
            foreach (var op in ops)
            {
                var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(values, thresh, op);
                int compCount = FilterKernels.EvaluateCompressedBlock(
                    compressed.AsSpan(0, written),
                    thresh,
                    op,
                    ReadOnlySpan<ulong>.Empty,
                    compIndices);

                Assert.Equal(uncompCount, compCount);
                for (int i = 0; i < uncompCount; i++)
                {
                    Assert.Equal(uncompIndices[i], compIndices[i]);
                }
                ArrayPool<int>.Shared.Return(uncompIndices);
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlock_SkewedDistribution_ParityWithUncompressed()
    {
        int count = 1024;
        float[] values = new float[count];
        // 99% constant 42.0f, 1% outliers
        Array.Fill(values, 42.0f);
        values[10] = 100.0f;
        values[250] = 5.0f;
        values[500] = 77.5f;
        values[900] = 1.0f;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        Span<int> compIndices = stackalloc int[count];

        var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(values, 50.0f, FilterOperation.GreaterThan);
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            50.0f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(uncompCount, compCount);
        Assert.Equal(2, compCount); // index 10 and 500
        Assert.Equal(10, compIndices[0]);
        Assert.Equal(500, compIndices[1]);
        ArrayPool<int>.Shared.Return(uncompIndices);
    }

    [Fact]
    public void EvaluateCompressedBlock_ZeroHeapAllocations_InSteadyState()
    {
        int count = 512;
        float[] values = new float[count];
        for (int i = 0; i < count; i++) values[i] = (float)Math.Round(10.0 + (i * 0.1), 1);

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        int wordCount = (count + 63) >> 6;
        Span<ulong> bitmask = stackalloc ulong[wordCount];

        // Warmup JIT
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        for (int i = 0; i < 200; i++)
        {
            FilterKernels.EvaluateCompressedBlockToBitmap(
                compressed.AsSpan(0, written),
                25.0f,
                FilterOperation.GreaterThan,
                ReadOnlySpan<ulong>.Empty,
                bitmask);
        }

        // Measure allocations
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long bytesBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++)
        {
            FilterKernels.EvaluateCompressedBlockToBitmap(
                compressed.AsSpan(0, written),
                25.0f,
                FilterOperation.GreaterThan,
                ReadOnlySpan<ulong>.Empty,
                bitmask);
        }
        long bytesAfter = GC.GetAllocatedBytesForCurrentThread();

        long diff = bytesAfter - bytesBefore;
        Assert.True(diff == 0, $"Steady-state EvaluateCompressedBlockToBitmap must have 0 heap allocations, but allocated {diff} bytes.");
    }

    [Fact]
    public void EvaluateCompressedBlock_ZeroFloat_MustNotMatchSubnormalThresholdOnEqual()
    {
        // Block contains regular zero floats and non-zero values
        float[] values = [0.0f, -0.0f, 10.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // 1. Equal: 0.0f == 1e-45f is FALSE in IEEE 754. Expected 0 matches.
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(0, compCount);

        // 2. NotEqual: 0.0f != 1e-45f is TRUE for all 3 values in IEEE 754. Expected 3 matches.
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(3, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);
        Assert.Equal(2, compIndices[2]);

        // 3. LessThan: 0.0f < 1e-45f is TRUE, -0.0f < 1e-45f is TRUE, 10.0f < 1e-45f is FALSE. Expected 2 matches.
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);

        // 4. GreaterThan: only 10.0f > 1e-45f is TRUE. Expected 1 match.
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(2, compIndices[0]);

        // 5. LessThanOrEqual: 0.0f <= 1e-45f is TRUE, -0.0f <= 1e-45f is TRUE. Expected 2 matches.
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);

        // 6. GreaterThanOrEqual: only 10.0f >= 1e-45f is TRUE. Expected 1 match.
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(2, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlock_BlockWithSubnormal_MatchesSubnormalThresholdBitExact()
    {
        // Block contains zero, an actual subnormal float exception, and normal float
        float[] values = [0.0f, 1e-45f, 10.0f];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);
        Assert.True(exCount > 0, "Subnormal float must be handled as an ALP exception");

        Span<int> compIndices = stackalloc int[values.Length];

        // Equal: only index 1 matches 1e-45f
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(1, compIndices[0]);

        // NotEqual: indices 0 and 2 match
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(2, compIndices[1]);

        // LessThan: only index 0 (0.0f < 1e-45f) matches
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(0, compIndices[0]);

        // GreaterThan: only index 2 (10.0f > 1e-45f) matches
        compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            1e-45f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(2, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_ZeroDouble_MustNotMatchSubnormalThresholdOnEqual()
    {
        // Block contains regular zero doubles and non-zero values
        double[] values = [0.0, -0.0, 10.0];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, compressed, out int exCount);

        Span<int> compIndices = stackalloc int[values.Length];

        // 1. Equal: 0.0 == 5e-324 is FALSE in IEEE 754. Expected 0 matches.
        int compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(0, compCount);

        // 2. NotEqual: 0.0 != 5e-324 is TRUE for all 3 values in IEEE 754. Expected 3 matches.
        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(3, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);
        Assert.Equal(2, compIndices[2]);

        // 3. LessThan: 0.0 < 5e-324 is TRUE, -0.0 < 5e-324 is TRUE, 10.0 < 5e-324 is FALSE. Expected 2 matches.
        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);

        // 4. GreaterThan: only 10.0 > 5e-324 is TRUE. Expected 1 match.
        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(2, compIndices[0]);

        // 5. LessThanOrEqual: 0.0 <= 5e-324 is TRUE, -0.0 <= 5e-324 is TRUE. Expected 2 matches.
        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(1, compIndices[1]);

        // 6. GreaterThanOrEqual: only 10.0 >= 5e-324 is TRUE. Expected 1 match.
        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(2, compIndices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_BlockWithSubnormal_MatchesSubnormalThresholdBitExact()
    {
        double[] values = [0.0, 5e-324, 10.0];
        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, compressed, out int exCount);
        Assert.True(exCount > 0, "Subnormal double must be handled as an ALP exception");

        Span<int> compIndices = stackalloc int[values.Length];

        // Equal: only index 1 matches 5e-324
        int compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(1, compCount);
        Assert.Equal(1, compIndices[0]);

        // NotEqual: indices 0 and 2 match
        compCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            5e-324,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            compIndices);
        Assert.Equal(2, compCount);
        Assert.Equal(0, compIndices[0]);
        Assert.Equal(2, compIndices[1]);
    }

    [Fact]
    public void EvaluateCompressedBlock_FloatThreshold_SelfComparisonBoundaries_MustConformWithIeee754()
    {
        // 15.2f has binary representation noise below exact 15.2 (15.1999998...)
        float[] valuesBelow = [10.0f, 15.2f, 20.0f];
        byte[] compBelow = new byte[AlpCompressor.GetMaxCompressedLength(valuesBelow.Length, sizeof(float))];
        int writtenBelow = AlpCompressor.CompressFloat(valuesBelow, compBelow, out _);

        Span<int> indices = stackalloc int[valuesBelow.Length];

        // GreaterThan: 15.2f > 15.2f is FALSE. Only 20.0f > 15.2f is TRUE.
        int count = FilterKernels.EvaluateCompressedBlock(
            compBelow.AsSpan(0, writtenBelow),
            15.2f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(2, indices[0]);

        // LessThanOrEqual: 10.0f <= 15.2f (T), 15.2f <= 15.2f (T - MUST match self), 20.0f <= 15.2f (F).
        count = FilterKernels.EvaluateCompressedBlock(
            compBelow.AsSpan(0, writtenBelow),
            15.2f,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(0, indices[0]);
        Assert.Equal(1, indices[1]);

        // GreaterThanOrEqual: 15.2f >= 15.2f (T - MUST match self), 20.0f >= 15.2f (T).
        count = FilterKernels.EvaluateCompressedBlock(
            compBelow.AsSpan(0, writtenBelow),
            15.2f,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(1, indices[0]);
        Assert.Equal(2, indices[1]);

        // LessThan: 10.0f < 15.2f (T), 15.2f < 15.2f (F - must NOT match self).
        count = FilterKernels.EvaluateCompressedBlock(
            compBelow.AsSpan(0, writtenBelow),
            15.2f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(0, indices[0]);

        // Equal: only 15.2f matches.
        count = FilterKernels.EvaluateCompressedBlock(
            compBelow.AsSpan(0, writtenBelow),
            15.2f,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(1, indices[0]);

        // NotEqual: 10.0f and 20.0f match.
        count = FilterKernels.EvaluateCompressedBlock(
            compBelow.AsSpan(0, writtenBelow),
            15.2f,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(0, indices[0]);
        Assert.Equal(2, indices[1]);

        // Symmetrically test 8.3f which has binary representation noise above exact 8.3 (8.30000019...)
        float[] valuesAbove = [5.0f, 8.3f, 12.0f];
        byte[] compAbove = new byte[AlpCompressor.GetMaxCompressedLength(valuesAbove.Length, sizeof(float))];
        int writtenAbove = AlpCompressor.CompressFloat(valuesAbove, compAbove, out _);

        // GreaterThan: only 12.0f matches
        count = FilterKernels.EvaluateCompressedBlock(
            compAbove.AsSpan(0, writtenAbove),
            8.3f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(2, indices[0]);

        // LessThanOrEqual: 5.0f and 8.3f match
        count = FilterKernels.EvaluateCompressedBlock(
            compAbove.AsSpan(0, writtenAbove),
            8.3f,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(0, indices[0]);
        Assert.Equal(1, indices[1]);

        // GreaterThanOrEqual: 8.3f and 12.0f match
        count = FilterKernels.EvaluateCompressedBlock(
            compAbove.AsSpan(0, writtenAbove),
            8.3f,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(1, indices[0]);
        Assert.Equal(2, indices[1]);

        // LessThan: only 5.0f matches
        count = FilterKernels.EvaluateCompressedBlock(
            compAbove.AsSpan(0, writtenAbove),
            8.3f,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(0, indices[0]);
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_SelfComparisonBoundaries_MustConformWithIeee754()
    {
        double[] values = [10.0, 15.2, 20.0];
        byte[] comp = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, comp, out _);

        Span<int> indices = stackalloc int[values.Length];

        // GreaterThan: only 20.0 matches (15.2 > 15.2 is FALSE)
        int count = FilterKernels.EvaluateCompressedBlockDouble(
            comp.AsSpan(0, written),
            15.2,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(2, indices[0]);

        // LessThanOrEqual: 10.0 and 15.2 match (15.2 <= 15.2 is TRUE)
        count = FilterKernels.EvaluateCompressedBlockDouble(
            comp.AsSpan(0, written),
            15.2,
            FilterOperation.LessThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(0, indices[0]);
        Assert.Equal(1, indices[1]);

        // GreaterThanOrEqual: 15.2 and 20.0 match (15.2 >= 15.2 is TRUE)
        count = FilterKernels.EvaluateCompressedBlockDouble(
            comp.AsSpan(0, written),
            15.2,
            FilterOperation.GreaterThanOrEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(1, indices[0]);
        Assert.Equal(2, indices[1]);

        // LessThan: only 10.0 matches (15.2 < 15.2 is FALSE)
        count = FilterKernels.EvaluateCompressedBlockDouble(
            comp.AsSpan(0, written),
            15.2,
            FilterOperation.LessThan,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(0, indices[0]);

        // Equal: only 15.2 matches
        count = FilterKernels.EvaluateCompressedBlockDouble(
            comp.AsSpan(0, written),
            15.2,
            FilterOperation.Equal,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(1, count);
        Assert.Equal(1, indices[0]);

        // NotEqual: 10.0 and 20.0 match
        count = FilterKernels.EvaluateCompressedBlockDouble(
            comp.AsSpan(0, written),
            15.2,
            FilterOperation.NotEqual,
            ReadOnlySpan<ulong>.Empty,
            indices);
        Assert.Equal(2, count);
        Assert.Equal(0, indices[0]);
        Assert.Equal(2, indices[1]);
    }

    [Fact]
    public void EvaluateCompressedBlock_SubnormalAndDecimalBoundaries_ZeroHeapAllocations_InSteadyState()
    {
        int count = 256;
        float[] values = new float[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (float)Math.Round(10.0 + (i * 0.1), 1);
        }
        values[0] = 0.0f;
        values[1] = 15.2f;
        values[2] = 8.3f;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        int wordCount = (count + 63) >> 6;
        Span<ulong> bitmask = stackalloc ulong[wordCount];
        Span<int> indices = stackalloc int[count];

        float[] testThresholds = [1e-45f, 15.2f, 8.3f];
        FilterOperation[] testOps =
        [
            FilterOperation.Equal,
            FilterOperation.NotEqual,
            FilterOperation.GreaterThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.GreaterThanOrEqual,
            FilterOperation.LessThan
        ];

        foreach (float thresh in testThresholds)
        {
            foreach (var op in testOps)
            {
                // Warmup JIT & steady-state caches
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                for (int i = 0; i < 200; i++)
                {
                    FilterKernels.EvaluateCompressedBlockToBitmap(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        bitmask);

                    FilterKernels.EvaluateCompressedBlock(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        indices);
                }

                // Measure heap allocations in steady state
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                long bytesBefore = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < 50; i++)
                {
                    FilterKernels.EvaluateCompressedBlockToBitmap(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        bitmask);

                    FilterKernels.EvaluateCompressedBlock(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        indices);
                }
                long bytesAfter = GC.GetAllocatedBytesForCurrentThread();

                long diff = bytesAfter - bytesBefore;
                Assert.True(
                    diff == 0,
                    $"Steady-state EvaluateCompressedBlock with thresh={thresh}, op={op} had {diff} heap allocations!");
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_SubnormalAndDecimalBoundaries_ZeroHeapAllocations_InSteadyState()
    {
        int count = 256;
        double[] values = new double[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = Math.Round(10.0 + (i * 0.1), 1);
        }
        values[0] = 0.0;
        values[1] = 15.2;
        values[2] = 8.3;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, compressed);

        Span<int> indices = stackalloc int[count];

        double[] testThresholds = [5e-324, 15.2, 8.3];
        FilterOperation[] testOps =
        [
            FilterOperation.Equal,
            FilterOperation.NotEqual,
            FilterOperation.GreaterThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.GreaterThanOrEqual,
            FilterOperation.LessThan
        ];

        foreach (double thresh in testThresholds)
        {
            foreach (var op in testOps)
            {
                // Warmup JIT
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                for (int i = 0; i < 200; i++)
                {
                    FilterKernels.EvaluateCompressedBlockDouble(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        indices);
                }

                // Measure steady-state allocations
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                long bytesBefore = GC.GetAllocatedBytesForCurrentThread();
                for (int i = 0; i < 50; i++)
                {
                    FilterKernels.EvaluateCompressedBlockDouble(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        indices);
                }
                long bytesAfter = GC.GetAllocatedBytesForCurrentThread();

                long diff = bytesAfter - bytesBefore;
                Assert.True(
                    diff == 0,
                    $"Steady-state EvaluateCompressedBlockDouble with thresh={thresh}, op={op} had {diff} heap allocations!");
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_ConstantAndAlpRdAndValidityMask_MatchesOracle()
    {
        // 1. Constant block + validity mask
        double[] constVals = new double[128];
        Array.Fill(constVals, 42.5);
        byte[] constComp = new byte[AlpCompressor.GetMaxCompressedLength(constVals.Length, sizeof(double))];
        int constWritten = AlpCompressor.CompressDouble(constVals, constComp, out _);
        AlpCompressor.ReadHeader(constComp.AsSpan(0, constWritten), out var constMode, out _, out _, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(AlpCompressionMode.Constant, constMode);

        ulong[] constMask = [0x0000_0000_0000_000FUL, 0x0000_0000_0000_0001UL];
        Span<int> constMatches = stackalloc int[constVals.Length];
        int constMatchCount = FilterKernels.EvaluateCompressedBlockDouble(
            constComp.AsSpan(0, constWritten),
            42.5,
            FilterOperation.Equal,
            constMask,
            constMatches);
        Assert.Equal(5, constMatchCount);
        Assert.Equal(0, constMatches[0]);
        Assert.Equal(1, constMatches[1]);
        Assert.Equal(2, constMatches[2]);
        Assert.Equal(3, constMatches[3]);
        Assert.Equal(64, constMatches[4]);

        // 2. AlpRdDouble block with large integer + decimal part + exceptions + validity mask
        const int count = 256;
        double[] rdVals = new double[count];
        for (int i = 0; i < count; i++)
        {
            // Large integer spread with small fractional steps makes AlpRdDouble (bwL+bwR=34) beat AlpDouble (bw=53)
            rdVals[i] = 200_000_000.0 + ((i % 7) * 100_000_000.0) + ((i % 10) * 0.0000001);
        }
        rdVals[5] = -0.0;
        rdVals[17] = double.PositiveInfinity;
        rdVals[99] = double.NaN;
        rdVals[150] = 5e-324;

        byte[] rdComp = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(double))];
        int rdWritten = AlpCompressor.CompressDouble(rdVals, rdComp, out _);
        AlpCompressor.ReadHeader(rdComp.AsSpan(0, rdWritten), out var rdMode, out _, out _, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(AlpCompressionMode.AlpRdDouble, rdMode);

        ulong[] rdMask = [0xAAAA_AAAA_AAAA_AAAAUL, 0x5555_5555_5555_5555UL, 0xFFFF_FFFF_FFFF_FFFFUL, 0x00FF_00FF_00FF_00FFUL];
        double[] thresholds = [500_000_000.0000005, 5e-324, double.NaN, double.PositiveInfinity];
        FilterOperation[] ops =
        [
            FilterOperation.Equal,
            FilterOperation.NotEqual,
            FilterOperation.LessThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.GreaterThan,
            FilterOperation.GreaterThanOrEqual
        ];

        Span<int> actual = stackalloc int[count];
        Span<int> expected = stackalloc int[count];
        foreach (double thresh in thresholds)
        {
            foreach (var op in ops)
            {
                int expCount = 0;
                for (int i = 0; i < count; i++)
                {
                    if ((rdMask[i >> 6] & (1UL << (i & 63))) == 0) continue;
                    double v = rdVals[i];
                    bool m = op switch
                    {
                        FilterOperation.Equal => v == thresh,
                        FilterOperation.NotEqual => v != thresh,
                        FilterOperation.LessThan => v < thresh,
                        FilterOperation.LessThanOrEqual => v <= thresh,
                        FilterOperation.GreaterThan => v > thresh,
                        FilterOperation.GreaterThanOrEqual => v >= thresh,
                        _ => false
                    };
                    if (m) expected[expCount++] = i;
                }

                int actCount = FilterKernels.EvaluateCompressedBlockDouble(
                    rdComp.AsSpan(0, rdWritten),
                    thresh,
                    op,
                    rdMask,
                    actual);

                Assert.Equal(expCount, actCount);
                for (int i = 0; i < expCount; i++)
                {
                    Assert.Equal(expected[i], actual[i]);
                }
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_AlpRdMode_ZeroHeapAllocations_InSteadyState()
    {
        const int count = 256;
        double[] values = new double[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = 200_000_000.0 + ((i % 7) * 100_000_000.0) + ((i % 10) * 0.0000001);
        }
        values[17] = double.PositiveInfinity;
        values[99] = 5e-324;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, compressed);
        AlpCompressor.ReadHeader(compressed.AsSpan(0, written), out var mode, out _, out _, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(AlpCompressionMode.AlpRdDouble, mode);

        Span<int> indices = stackalloc int[count];

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        for (int i = 0; i < 200; i++)
        {
            FilterKernels.EvaluateCompressedBlockDouble(
                compressed.AsSpan(0, written),
                500_000_000.0000005,
                FilterOperation.GreaterThanOrEqual,
                ReadOnlySpan<ulong>.Empty,
                indices);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long bytesBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 50; i++)
        {
            FilterKernels.EvaluateCompressedBlockDouble(
                compressed.AsSpan(0, written),
                500_000_000.0000005,
                FilterOperation.GreaterThanOrEqual,
                ReadOnlySpan<ulong>.Empty,
                indices);
        }
        long bytesAfter = GC.GetAllocatedBytesForCurrentThread();

        long diff = bytesAfter - bytesBefore;
        Assert.True(diff == 0, $"Steady-state AlpRdDouble EvaluateCompressedBlockDouble had {diff} heap allocations!");
    }

    private static bool Ieee754MatchesFloat(float v, float t, FilterOperation op) => op switch
    {
        FilterOperation.Equal => v == t,
        FilterOperation.NotEqual => v != t,
        FilterOperation.LessThan => v < t,
        FilterOperation.LessThanOrEqual => v <= t,
        FilterOperation.GreaterThan => v > t,
        FilterOperation.GreaterThanOrEqual => v >= t,
        _ => false
    };

    [Fact]
    public void EvaluateCompressedBlock_FloatBitWidth33To64_MatchesOracleAndZeroHeapAllocations()
    {
        FilterOperation[] ops =
        [
            FilterOperation.Equal,
            FilterOperation.NotEqual,
            FilterOperation.LessThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.GreaterThan,
            FilterOperation.GreaterThanOrEqual
        ];

        // 1. bitWidth == 33 ([0.0f, 5e9f, ...])
        float[] wide33 = new float[64];
        for (int i = 0; i < wide33.Length; i++) wide33[i] = (i & 1) == 0 ? 0.0f : 5e9f;
        byte[] comp33 = new byte[AlpCompressor.GetMaxCompressedLength(wide33.Length, sizeof(float))];
        int written33 = AlpCompressor.CompressFloat(wide33, comp33);
        AlpCompressor.ReadHeader(comp33.AsSpan(0, written33), out _, out _, out byte bw33, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(33, bw33);

        // 2. bitWidth == 63 ([-8.5e18f, 0.0f, ...])
        float[] wide63 = new float[64];
        for (int i = 0; i < wide63.Length; i++) wide63[i] = (i % 3 == 0) ? -8.5e18f : 0.0f;
        byte[] comp63 = new byte[AlpCompressor.GetMaxCompressedLength(wide63.Length, sizeof(float))];
        int written63 = AlpCompressor.CompressFloat(wide63, comp63);
        AlpCompressor.ReadHeader(comp63.AsSpan(0, written63), out _, out _, out byte bw63, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(63, bw63);

        // 3. bitWidth == 64 (valid AlpFloat block with full 64-bit range [-8.5e18f, 0.0f, +8.5e18f])
        const ushort count64 = 65;
        const int packedBytes64 = count64 * 8;
        byte[] comp64 = new byte[AlpCompressor.HeaderSize + packedBytes64];
        int written64 = comp64.Length;
        long minBase64 = (long)(-8.5e18f);
        long maxBase64 = (long)(8.5e18f);
        BinaryPrimitives.WriteUInt32LittleEndian(comp64.AsSpan(0x00, 4), AlpCompressor.Magic);
        comp64[0x04] = (byte)AlpCompressionMode.AlpFloat;
        comp64[0x05] = 0; // exp
        comp64[0x06] = 64; // bitWidth
        BinaryPrimitives.WriteUInt16LittleEndian(comp64.AsSpan(0x0A, 2), count64);
        BinaryPrimitives.WriteUInt16LittleEndian(comp64.AsSpan(0x0C, 2), 0); // exCount
        BinaryPrimitives.WriteInt64LittleEndian(comp64.AsSpan(0x10, 8), minBase64);
        BinaryPrimitives.WriteUInt64LittleEndian(comp64.AsSpan(0x20, 8), BitConverter.SingleToUInt32Bits(-8.5e18f));
        BinaryPrimitives.WriteUInt64LittleEndian(comp64.AsSpan(0x28, 8), BitConverter.SingleToUInt32Bits(8.5e18f));

        float[] wide64 = new float[count64];
        for (int i = 0; i < count64; i++)
        {
            long v = (i == 10) ? 0L : ((i % 2 == 0) ? minBase64 : maxBase64);
            ulong u = (ulong)(v - minBase64);
            BinaryPrimitives.WriteUInt64LittleEndian(comp64.AsSpan(AlpCompressor.HeaderSize + (i * 8), 8), u);
            wide64[i] = (float)v;
        }
        AlpCompressor.ReadHeader(comp64.AsSpan(0, written64), out _, out _, out byte bw64, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(64, bw64);

        Span<int> actual = stackalloc int[65];

        void VerifyBlock(float[] src, byte[] comp, int len, float[] thresholds)
        {
            float[] decomp = new float[src.Length];
            int[] actArr = new int[src.Length];
            int[] expArr = new int[src.Length];
            AlpCompressor.DecompressFloat(comp.AsSpan(0, len), decomp);
            foreach (float thresh in thresholds)
            {
                foreach (var op in ops)
                {
                    int expCount = 0;
                    for (int i = 0; i < src.Length; i++)
                    {
                        if (Ieee754MatchesFloat(decomp[i], thresh, op))
                            expArr[expCount++] = i;
                    }

                    int actCount = FilterKernels.EvaluateCompressedBlock(
                        comp.AsSpan(0, len), thresh, op, ReadOnlySpan<ulong>.Empty, actArr);

                    Assert.True(expCount == actCount, $"thresh={thresh}, op={op}: expected={expCount}, actual={actCount}");
                    for (int i = 0; i < expCount; i++)
                        Assert.Equal(expArr[i], actArr[i]);
                }
            }
        }

        VerifyBlock(wide33, comp33, written33, [0.0f, 1.0f, 5e9f, -1.0f, 6e9f]);
        VerifyBlock(wide63, comp63, written63, [-8.5e18f, -1.0f, 0.0f, 1.0f]);
        VerifyBlock(wide64, comp64, written64, [-8.5e18f, 0.0f, 8.5e18f, -9e18f, 9e18f]);

        // Verify zero steady-state heap allocations on bitWidth == 64 scalar path
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        for (int i = 0; i < 200; i++)
        {
            FilterKernels.EvaluateCompressedBlock(comp64.AsSpan(0, written64), 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, actual);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 50; i++)
        {
            FilterKernels.EvaluateCompressedBlock(comp64.AsSpan(0, written64), 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, actual);
        }
        long diff = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(diff == 0, $"Steady-state bitWidth=64 EvaluateCompressedBlock had {diff} heap allocations!");
    }

    [Fact]
    public void EvaluateCompressedBlock_FloatExtremeMinBaseInt128Overflow_MatchesOracle()
    {
        float[] ov = new float[64];
        Array.Fill(ov, -8.5e18f);
        ov[63] = 9.5e18f; // exception (> 9e18), leaving bitWidth == 0 with minBase ~ -8.5e18

        byte[] comp = new byte[AlpCompressor.GetMaxCompressedLength(ov.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(ov, comp, out int exCount);
        Assert.Equal(1, exCount);
        AlpCompressor.ReadHeader(comp.AsSpan(0, written), out _, out _, out byte bw, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.Equal(0, bw);

        float[] decomp = new float[ov.Length];
        AlpCompressor.DecompressFloat(comp.AsSpan(0, written), decomp);

        FilterOperation[] ops =
        [
            FilterOperation.Equal,
            FilterOperation.NotEqual,
            FilterOperation.LessThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.GreaterThan,
            FilterOperation.GreaterThanOrEqual
        ];
        float[] thresholds = [8.5e18f, -8.5e18f, 9.5e18f, 0.0f];
        Span<int> actual = stackalloc int[ov.Length];
        Span<int> expected = stackalloc int[ov.Length];

        foreach (float thresh in thresholds)
        {
            foreach (var op in ops)
            {
                int expCount = 0;
                for (int i = 0; i < ov.Length; i++)
                {
                    if (Ieee754MatchesFloat(decomp[i], thresh, op))
                        expected[expCount++] = i;
                }

                int actCount = FilterKernels.EvaluateCompressedBlock(
                    comp.AsSpan(0, written), thresh, op, ReadOnlySpan<ulong>.Empty, actual);

                Assert.Equal(expCount, actCount);
                for (int i = 0; i < expCount; i++)
                    Assert.Equal(expected[i], actual[i]);
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlock_DuplicateExceptionIndices_AllMatchAndSkip_LastWriteWins()
    {
        // Craft a valid 16-element AlpFloat block (regular values 10..25, bw=4, minBase=10)
        // with duplicate exception pairs overwriting row 3 (1 -> 20), row 4 (1 -> 20), row 5 (20 -> 1).
        const ushort count = 16;
        const ushort exCount = 6;
        const int bitWidth = 4;
        const int packedBytes = (count * bitWidth + 7) >> 3; // 8 bytes
        int totalLen = AlpCompressor.HeaderSize + packedBytes + (exCount * 2) + (exCount * 4);
        byte[] block = new byte[totalLen];

        BinaryPrimitives.WriteUInt32LittleEndian(block.AsSpan(0x00, 4), AlpCompressor.Magic);
        block[0x04] = (byte)AlpCompressionMode.AlpFloat;
        block[0x05] = 0; // exp
        block[0x06] = bitWidth;
        BinaryPrimitives.WriteUInt16LittleEndian(block.AsSpan(0x0A, 2), count);
        BinaryPrimitives.WriteUInt16LittleEndian(block.AsSpan(0x0C, 2), exCount);
        BinaryPrimitives.WriteInt64LittleEndian(block.AsSpan(0x10, 8), 10); // minBase
        BinaryPrimitives.WriteUInt64LittleEndian(block.AsSpan(0x20, 8), BitConverter.SingleToUInt32Bits(10.0f)); // zone min
        BinaryPrimitives.WriteUInt64LittleEndian(block.AsSpan(0x28, 8), BitConverter.SingleToUInt32Bits(25.0f)); // zone max

        // Pack 0..15 in 4-bit nibbles
        for (int i = 0; i < count; i += 2)
        {
            block[AlpCompressor.HeaderSize + (i >> 1)] = (byte)((i & 0xF) | (((i + 1) & 0xF) << 4));
        }

        ushort[] exRows = [3, 3, 4, 4, 5, 5];
        float[] exVals = [1.0f, 20.0f, 1.0f, 20.0f, 20.0f, 1.0f];
        int exOff = AlpCompressor.HeaderSize + packedBytes;
        int exValOff = exOff + (exCount * 2);
        for (int k = 0; k < exCount; k++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(block.AsSpan(exOff + (k * 2), 2), exRows[k]);
            BinaryPrimitives.WriteUInt32LittleEndian(block.AsSpan(exValOff + (k * 4), 4), BitConverter.SingleToUInt32Bits(exVals[k]));
        }

        float[] oracle = new float[count];
        AlpCompressor.DecompressFloat(block, oracle);

        FilterOperation[] ops =
        [
            FilterOperation.Equal,
            FilterOperation.NotEqual,
            FilterOperation.LessThan,
            FilterOperation.LessThanOrEqual,
            FilterOperation.GreaterThan,
            FilterOperation.GreaterThanOrEqual
        ];

        ulong[] validity = [0b1111_1111_1110_1111UL]; // mask out row 4
        Span<int> actual = stackalloc int[count];
        Span<int> expected = stackalloc int[count];

        foreach (float thresh in new[] { 5.0f, 15.0f, 30.0f })
        {
            foreach (var op in ops)
            {
                int expCount = 0;
                for (int i = 0; i < count; i++)
                {
                    if ((validity[0] & (1UL << i)) == 0) continue;
                    if (Ieee754MatchesFloat(oracle[i], thresh, op))
                        expected[expCount++] = i;
                }

                int actCount = FilterKernels.EvaluateCompressedBlock(block, thresh, op, validity, actual);
                Assert.Equal(expCount, actCount);
                for (int i = 0; i < expCount; i++)
                    Assert.Equal(expected[i], actual[i]);
            }
        }
    }

    [Fact]
    public void EvaluateCompressedBlock_AndDouble_CorruptAndTruncatedBuffers_ThrowInvalidDataExceptionParity()
    {
        Span<int> indices = stackalloc int[64];
        Span<ulong> bitmask = stackalloc ulong[1];

        // 0. Empty buffer returns 0 cleanly
        Assert.Equal(0, FilterKernels.EvaluateCompressedBlock(ReadOnlySpan<byte>.Empty, 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, indices));
        Assert.Equal(0, FilterKernels.EvaluateCompressedBlockToBitmap(ReadOnlySpan<byte>.Empty, 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, bitmask));
        Assert.Equal(0, FilterKernels.EvaluateCompressedBlockDouble(ReadOnlySpan<byte>.Empty, 0.0, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, indices));

        // 1. Non-empty < HeaderSize (1 and 47 bytes) throws InvalidDataException
        byte[] short1 = [0x31];
        byte[] short47 = new byte[47];
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(short1, 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, Span<int>.Empty));
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(short47, 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, Span<int>.Empty));
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockToBitmap(short47, 0.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new ulong[1]));
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(short1, 0.0, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, Span<int>.Empty));
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(short47, 0.0, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, Span<int>.Empty));

        // 2. Valid float & double blocks for truncation / corruption tests
        float[] fVals = [1.0f, 2.0f, 3.0f, float.NaN];
        byte[] fComp = new byte[AlpCompressor.GetMaxCompressedLength(fVals.Length, sizeof(float))];
        int fWritten = AlpCompressor.CompressFloat(fVals, fComp);

        double[] dVals = [1.0, 2.0, 3.0, double.NaN];
        byte[] dComp = new byte[AlpCompressor.GetMaxCompressedLength(dVals.Length, sizeof(double))];
        int dWritten = AlpCompressor.CompressDouble(dVals, dComp);

        // Truncated payload must throw even on NaN / Skip / AllMatch thresholds
        byte[] fTrunc = fComp.AsSpan(0, fWritten - 1).ToArray();
        byte[] dTrunc = dComp.AsSpan(0, dWritten - 1).ToArray();
        foreach (float thresh in new[] { 2.0f, float.NaN, 100.0f, -100.0f })
        {
            Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(fTrunc, thresh, FilterOperation.GreaterThan, ReadOnlySpan<ulong>.Empty, new int[4]));
            Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockToBitmap(fTrunc, thresh, FilterOperation.GreaterThan, ReadOnlySpan<ulong>.Empty, new ulong[1]));
        }
        foreach (double thresh in new[] { 2.0, double.NaN, 100.0, -100.0 })
        {
            Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(dTrunc, thresh, FilterOperation.GreaterThan, ReadOnlySpan<ulong>.Empty, new int[4]));
        }

        // Invalid mode (99)
        byte[] fBadMode = fComp.AsSpan(0, fWritten).ToArray();
        fBadMode[0x04] = 99;
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(fBadMode, 2.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new int[4]));

        byte[] dBadMode = dComp.AsSpan(0, dWritten).ToArray();
        dBadMode[0x04] = 99;
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(dBadMode, 2.0, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new int[4]));

        // Invalid exponent (255) & bitWidth (65)
        byte[] fBadExp = fComp.AsSpan(0, fWritten).ToArray();
        fBadExp[0x05] = 255;
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(fBadExp, 2.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new int[4]));

        byte[] fBadBw = fComp.AsSpan(0, fWritten).ToArray();
        fBadBw[0x06] = 65;
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(fBadBw, 2.0f, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new int[4]));

        byte[] dBadExp = dComp.AsSpan(0, dWritten).ToArray();
        dBadExp[0x05] = 255;
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(dBadExp, 2.0, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new int[4]));

        byte[] dBadBw = dComp.AsSpan(0, dWritten).ToArray();
        dBadBw[0x06] = 65;
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(dBadBw, 2.0, FilterOperation.Equal, ReadOnlySpan<ulong>.Empty, new int[4]));

        // Out-of-bounds exception rowIdx >= count must throw even under NaN / Skip / AllMatch thresholds
        AlpCompressor.ReadHeader(fComp.AsSpan(0, fWritten), out _, out _, out byte fBw, out _, out _, out ushort fCount, out _, out _, out _, out _, out _);
        byte[] fBadExIdx = fComp.AsSpan(0, fWritten).ToArray();
        int fExOff = AlpCompressor.HeaderSize + ((fCount * fBw + 7) >> 3);
        BinaryPrimitives.WriteUInt16LittleEndian(fBadExIdx.AsSpan(fExOff, 2), fCount); // rowIdx == count
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(fBadExIdx, float.NaN, FilterOperation.NotEqual, ReadOnlySpan<ulong>.Empty, new int[4]));
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlock(fBadExIdx, 100.0f, FilterOperation.GreaterThan, ReadOnlySpan<ulong>.Empty, new int[4]));

        AlpCompressor.ReadHeader(dComp.AsSpan(0, dWritten), out _, out _, out byte dBw, out _, out _, out ushort dCount, out _, out _, out _, out _, out _);
        byte[] dBadExIdx = dComp.AsSpan(0, dWritten).ToArray();
        int dExOff = AlpCompressor.HeaderSize + ((dCount * dBw + 7) >> 3);
        BinaryPrimitives.WriteUInt16LittleEndian(dBadExIdx.AsSpan(dExOff, 2), dCount); // rowIdx == count
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(dBadExIdx, double.NaN, FilterOperation.NotEqual, ReadOnlySpan<ulong>.Empty, new int[4]));
        Assert.Throws<InvalidDataException>(() => FilterKernels.EvaluateCompressedBlockDouble(dBadExIdx, 100.0, FilterOperation.GreaterThan, ReadOnlySpan<ulong>.Empty, new int[4]));
    }
}
