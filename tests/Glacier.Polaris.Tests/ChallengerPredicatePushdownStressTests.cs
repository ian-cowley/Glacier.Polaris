using System;
using System.Buffers;
using Glacier.Polaris.Compute;
using Glacier.Storage.Encoding;
using Xunit;

namespace Glacier.Polaris.Tests;

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
                for (int i = 0; i < 100; i++)
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
                // Warmup ArrayPool and JIT
                for (int i = 0; i < 100; i++)
                {
                    FilterKernels.EvaluateCompressedBlockDouble(
                        compressed.AsSpan(0, written),
                        thresh,
                        op,
                        ReadOnlySpan<ulong>.Empty,
                        indices);
                }

                // Measure steady-state allocations
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
}
