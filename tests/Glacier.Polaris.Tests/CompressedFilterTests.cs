using System;
using System.IO;
using Glacier.Polaris.Compute;
using Glacier.Storage.Encoding;
using Xunit;

namespace Glacier.Polaris.Tests;

public class CompressedFilterTests
{
    [Fact]
    public void PruneBlock_AllOperators_ProduceExactExpectedResults()
    {
        // Block range [10.0, 50.0]
        float min = 10.0f;
        float max = 50.0f;
        int count = 100;

        // GreaterThan
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 0, 55.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 50.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 5.0f, FilterOperation.GreaterThan));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 30.0f, FilterOperation.GreaterThan));

        // GreaterThanOrEqual
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 55.0f, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 10.0f, FilterOperation.GreaterThanOrEqual));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 50.0f, FilterOperation.GreaterThanOrEqual));

        // LessThan
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 5.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 10.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 55.0f, FilterOperation.LessThan));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 30.0f, FilterOperation.LessThan));

        // LessThanOrEqual
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 5.0f, FilterOperation.LessThanOrEqual));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 50.0f, FilterOperation.LessThanOrEqual));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 10.0f, FilterOperation.LessThanOrEqual));

        // Equal
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 5.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(min, max, count, 55.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 30.0f, FilterOperation.Equal));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(30.0f, 30.0f, count, 0, 30.0f, FilterOperation.Equal));

        // NotEqual
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 5.0f, FilterOperation.NotEqual));
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlock(min, max, count, 55.0f, FilterOperation.NotEqual));
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlock(30.0f, 30.0f, count, 0, 30.0f, FilterOperation.NotEqual));
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlock(min, max, count, 30.0f, FilterOperation.NotEqual));
    }

    [Fact]
    public void PruneBlockRange_ProducesExactExpectedResults()
    {
        float min = 10.0f;
        float max = 50.0f;
        int count = 100;

        // Disjoint above
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlockRange(min, max, count, 0, 60.0f, 70.0f));
        // Disjoint below
        Assert.Equal(BlockPruneResult.Skip, FilterKernels.PruneBlockRange(min, max, count, 0, 0.0f, 5.0f));
        // Strictly contained (all match)
        Assert.Equal(BlockPruneResult.AllMatch, FilterKernels.PruneBlockRange(min, max, count, 0, 5.0f, 60.0f));
        // Overlap (partial)
        Assert.Equal(BlockPruneResult.Partial, FilterKernels.PruneBlockRange(min, max, count, 0, 20.0f, 40.0f));
    }

    [Fact]
    public void EvaluateCompressedBlock_ParityWithUncompressedFilter()
    {
        // 1024 float values
        int count = 1024;
        float[] uncompressed = new float[count];
        for (int i = 0; i < count; i++)
        {
            uncompressed[i] = (float)Math.Round(10.0 + (i * 0.1), 1); // 10.0 to 112.3
        }

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(uncompressed, compressed);

        float threshold = 50.0f;

        // Run uncompressed filter
        var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(uncompressed, threshold, FilterOperation.GreaterThan);

        // Run compressed block filter
        Span<int> compIndices = stackalloc int[count];
        int compCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            threshold,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(uncompCount, compCount);
        for (int i = 0; i < uncompCount; i++)
        {
            Assert.Equal(uncompIndices[i], compIndices[i]);
        }

        System.Buffers.ArrayPool<int>.Shared.Return(uncompIndices);
    }

    [Fact]
    public void EvaluateCompressedBlock_NonExactThresholds_FloorCeilParity()
    {
        int count = 100;
        float[] values = new float[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (float)Math.Round(1.0 + (i * 0.2), 1); // 1.0, 1.2, 1.4, 1.6 ...
        }

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        // Non-exact threshold 1.25f
        float threshold = 1.25f;

        FilterOperation[] ops =
        [
            FilterOperation.GreaterThan,
            FilterOperation.GreaterThanOrEqual,
            FilterOperation.LessThan,
            FilterOperation.LessThanOrEqual
        ];

        Span<int> compIndices = stackalloc int[count];

        foreach (var op in ops)
        {
            var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(values, threshold, op);
            int compCount = FilterKernels.EvaluateCompressedBlock(
                compressed.AsSpan(0, written),
                threshold,
                op,
                ReadOnlySpan<ulong>.Empty,
                compIndices);

            Assert.Equal(uncompCount, compCount);
            for (int i = 0; i < uncompCount; i++)
            {
                Assert.Equal(uncompIndices[i], compIndices[i]);
            }

            System.Buffers.ArrayPool<int>.Shared.Return(uncompIndices);
        }
    }

    [Fact]
    public void EvaluateCompressedBlock_HandlesExceptionsAccordingToIeee754()
    {
        float[] values =
        [
            10.0f,
            float.NaN,
            20.0f,
            float.PositiveInfinity,
            30.0f,
            float.NegativeInfinity,
            40.0f,
            1.0f / 3.0f, // non-decimal exception
            50.0f
        ];

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(values.Length, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed, out int exCount);
        Assert.True(exCount > 0);

        Span<int> compIndices = stackalloc int[values.Length];

        // x > 25.0f
        // +Inf (idx 3), 30.0f (idx 4), 40.0f (idx 6), 50.0f (idx 8) -> 4 matches
        var (uncompIndices, uncompCount) = FilterKernels.Filter<float>(values, 25.0f, FilterOperation.GreaterThan);
        int matchCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            25.0f,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            compIndices);

        Assert.Equal(uncompCount, matchCount);
        Assert.Equal(4, matchCount);
        Assert.Equal(3, compIndices[0]);
        Assert.Equal(4, compIndices[1]);
        Assert.Equal(6, compIndices[2]);
        Assert.Equal(8, compIndices[3]);
        System.Buffers.ArrayPool<int>.Shared.Return(uncompIndices);
    }

    [Fact]
    public void EvaluateCompressedBlock_ValidityMaskNullHandling()
    {
        // 16 values: 10f, 20f, 30f, ... 160f
        int count = 16;
        float[] values = new float[count];
        for (int i = 0; i < count; i++) values[i] = (i + 1) * 10f;

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(float))];
        int written = AlpCompressor.CompressFloat(values, compressed);

        // Validity mask where every 2nd element is null (even indices valid: 0, 2, 4, ...)
        ulong validityWord = 0x5555; // 0101010101010101
        Span<ulong> validityMask = stackalloc ulong[1] { validityWord };

        // Filter: x >= 50f
        // Without null mask: indices 4..15 (12 elements)
        // With validity mask: only even indices >= 4 match: 4, 6, 8, 10, 12, 14 (6 elements)
        Span<int> compIndices = stackalloc int[count];
        int matchCount = FilterKernels.EvaluateCompressedBlock(
            compressed.AsSpan(0, written),
            50.0f,
            FilterOperation.GreaterThanOrEqual,
            validityMask,
            compIndices);

        Assert.Equal(6, matchCount);
        Assert.Equal(4, compIndices[0]);
        Assert.Equal(6, compIndices[1]);
        Assert.Equal(8, compIndices[2]);
        Assert.Equal(10, compIndices[3]);
        Assert.Equal(12, compIndices[4]);
        Assert.Equal(14, compIndices[5]);
    }

    [Fact]
    public void EvaluateCompressedBlockDouble_BitExactParity()
    {
        int count = 64;
        double[] values = new double[count];
        for (int i = 0; i < count; i++) values[i] = 100.0 + (i * 0.5);

        byte[] compressed = new byte[AlpCompressor.GetMaxCompressedLength(count, sizeof(double))];
        int written = AlpCompressor.CompressDouble(values, compressed);

        Span<int> matches = stackalloc int[count];
        int matchCount = FilterKernels.EvaluateCompressedBlockDouble(
            compressed.AsSpan(0, written),
            120.0,
            FilterOperation.GreaterThan,
            ReadOnlySpan<ulong>.Empty,
            matches);

        // Values > 120.0: i > 40 (i = 41..63 -> 23 values)
        Assert.Equal(23, matchCount);
        Assert.Equal(41, matches[0]);
    }

    [Fact]
    public void PredicatePushdownEngine_DefaultInstance_MatchesOracle()
    {
        IPredicatePushdownEngine engine = new PredicatePushdownEngine();

        var pruneSkip = engine.InspectZoneMap(10.0f, 50.0f, 60.0f, ">");
        Assert.Equal(PushdownPruneResult.AllRowsPruned, pruneSkip);

        var pruneAll = engine.InspectZoneMap(100.0f, 200.0f, 50.0f, ">");
        Assert.Equal(PushdownPruneResult.AllRowsMatch, pruneAll);

        var pruneEval = engine.InspectZoneMap(10.0f, 100.0f, 50.0f, ">");
        Assert.Equal(PushdownPruneResult.EvaluateBlock, pruneEval);

        float[] block = [10f, 25f, 55f, 12f, 90f, 44f, 62f, 5f];
        Span<int> matches = stackalloc int[block.Length];
        int count = engine.FilterBlock(block, 50.0f, ">", matches);

        Assert.Equal(3, count);
        Assert.Equal(2, matches[0]);
        Assert.Equal(4, matches[1]);
        Assert.Equal(6, matches[2]);
    }
}
