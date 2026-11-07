using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Glacier.Storage.Encoding;

namespace Glacier.Polaris.Compute;

/// <summary>
/// Result of Level-1 zone-map block pruning.
/// </summary>
public enum BlockPruneResult
{
    Skip,
    AllMatch,
    Partial
}

/// <summary>
/// Predicate pushdown prune result for zone maps.
/// </summary>
public enum PushdownPruneResult
{
    AllRowsMatch,
    AllRowsPruned,
    EvaluateBlock
}

/// <summary>
/// Interface for predicate pushdown engine over zone maps and compressed blocks.
/// </summary>
public interface IPredicatePushdownEngine
{
    PushdownPruneResult InspectZoneMap(float min, float max, float threshold, string op);
    int FilterBlock(ReadOnlySpan<float> block, float threshold, string op, Span<int> matchingIndices);
}

/// <summary>
/// Default implementation of <see cref="IPredicatePushdownEngine"/>.
/// </summary>
public sealed class PredicatePushdownEngine : IPredicatePushdownEngine
{
    public PushdownPruneResult InspectZoneMap(float min, float max, float threshold, string op)
    {
        if (float.IsNaN(threshold))
        {
            return op == "!=" ? PushdownPruneResult.AllRowsMatch : PushdownPruneResult.AllRowsPruned;
        }

        if (float.IsNaN(min) || float.IsNaN(max))
        {
            return PushdownPruneResult.EvaluateBlock;
        }

        return op switch
        {
            ">" => max <= threshold ? PushdownPruneResult.AllRowsPruned : (min > threshold ? PushdownPruneResult.AllRowsMatch : PushdownPruneResult.EvaluateBlock),
            ">=" => max < threshold ? PushdownPruneResult.AllRowsPruned : (min >= threshold ? PushdownPruneResult.AllRowsMatch : PushdownPruneResult.EvaluateBlock),
            "<" => min >= threshold ? PushdownPruneResult.AllRowsPruned : (max < threshold ? PushdownPruneResult.AllRowsMatch : PushdownPruneResult.EvaluateBlock),
            "<=" => min > threshold ? PushdownPruneResult.AllRowsPruned : (max <= threshold ? PushdownPruneResult.AllRowsMatch : PushdownPruneResult.EvaluateBlock),
            "==" => (threshold < min || threshold > max) ? PushdownPruneResult.AllRowsPruned : PushdownPruneResult.EvaluateBlock,
            "!=" => (min == max && min == threshold) ? PushdownPruneResult.AllRowsPruned : (threshold < min || threshold > max ? PushdownPruneResult.AllRowsMatch : PushdownPruneResult.EvaluateBlock),
            _ => PushdownPruneResult.EvaluateBlock
        };
    }

    public int FilterBlock(ReadOnlySpan<float> block, float threshold, string op, Span<int> matchingIndices)
    {
        int count = 0;
        for (int i = 0; i < block.Length && count < matchingIndices.Length; i++)
        {
            float v = block[i];
            bool match = op switch
            {
                ">" => v > threshold,
                ">=" => v >= threshold,
                "<" => v < threshold,
                "<=" => v <= threshold,
                "==" => float.IsNaN(threshold) || float.IsNaN(v) ? false : MathF.Abs(v - threshold) < 1e-6f,
                "!=" => float.IsNaN(threshold) || float.IsNaN(v) ? true : MathF.Abs(v - threshold) >= 1e-6f,
                _ => false
            };

            if (match)
            {
                matchingIndices[count++] = i;
            }
        }
        return count;
    }
}

public static partial class FilterKernels
{
    private static readonly double[] s_pow10 =
    [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10,
        1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    ];

    // =========================================================================
    // LEVEL 1: ZONE-MAP PRUNING
    // =========================================================================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BlockPruneResult PruneBlock<T>(
        T blockMin,
        T blockMax,
        int rowCount,
        T constant,
        FilterOperation op,
        int nullCount = 0) where T : unmanaged, IComparable<T>
    {
        return PruneBlock(blockMin, blockMax, rowCount, nullCount, constant, op);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BlockPruneResult PruneBlock<T>(
        T blockMin,
        T blockMax,
        int rowCount,
        int nullCount,
        T constant,
        FilterOperation op) where T : unmanaged, IComparable<T>
    {
        if (rowCount <= 0 || nullCount == rowCount)
        {
            return BlockPruneResult.Skip;
        }

        if (typeof(T) == typeof(float))
        {
            float cF = Unsafe.As<T, float>(ref constant);
            if (float.IsNaN(cF))
            {
                return op == FilterOperation.NotEqual ? BlockPruneResult.AllMatch : BlockPruneResult.Skip;
            }
            float minF = Unsafe.As<T, float>(ref blockMin);
            float maxF = Unsafe.As<T, float>(ref blockMax);
            if (float.IsNaN(minF) || float.IsNaN(maxF))
            {
                return BlockPruneResult.Partial;
            }
        }
        else if (typeof(T) == typeof(double))
        {
            double cD = Unsafe.As<T, double>(ref constant);
            if (double.IsNaN(cD))
            {
                return op == FilterOperation.NotEqual ? BlockPruneResult.AllMatch : BlockPruneResult.Skip;
            }
            double minD = Unsafe.As<T, double>(ref blockMin);
            double maxD = Unsafe.As<T, double>(ref blockMax);
            if (double.IsNaN(minD) || double.IsNaN(maxD))
            {
                return BlockPruneResult.Partial;
            }
        }

        switch (op)
        {
            case FilterOperation.GreaterThan:
                if (blockMax.CompareTo(constant) <= 0) return BlockPruneResult.Skip;
                if (blockMin.CompareTo(constant) > 0) return BlockPruneResult.AllMatch;
                return BlockPruneResult.Partial;

            case FilterOperation.GreaterThanOrEqual:
                if (blockMax.CompareTo(constant) < 0) return BlockPruneResult.Skip;
                if (blockMin.CompareTo(constant) >= 0) return BlockPruneResult.AllMatch;
                return BlockPruneResult.Partial;

            case FilterOperation.LessThan:
                if (blockMin.CompareTo(constant) >= 0) return BlockPruneResult.Skip;
                if (blockMax.CompareTo(constant) < 0) return BlockPruneResult.AllMatch;
                return BlockPruneResult.Partial;

            case FilterOperation.LessThanOrEqual:
                if (blockMin.CompareTo(constant) > 0) return BlockPruneResult.Skip;
                if (blockMax.CompareTo(constant) <= 0) return BlockPruneResult.AllMatch;
                return BlockPruneResult.Partial;

            case FilterOperation.Equal:
                if (constant.CompareTo(blockMin) < 0 || constant.CompareTo(blockMax) > 0) return BlockPruneResult.Skip;
                if (blockMin.CompareTo(blockMax) == 0 && blockMin.CompareTo(constant) == 0) return BlockPruneResult.AllMatch;
                return BlockPruneResult.Partial;

            case FilterOperation.NotEqual:
                if (blockMin.CompareTo(blockMax) == 0 && blockMin.CompareTo(constant) == 0) return BlockPruneResult.Skip;
                if (constant.CompareTo(blockMin) < 0 || constant.CompareTo(blockMax) > 0) return BlockPruneResult.AllMatch;
                return BlockPruneResult.Partial;

            default:
                return BlockPruneResult.Partial;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static BlockPruneResult PruneBlockRange<T>(
        T blockMin,
        T blockMax,
        int rowCount,
        int nullCount,
        T constantLow,
        T constantHigh) where T : unmanaged, IComparable<T>
    {
        if (rowCount <= 0 || nullCount == rowCount)
        {
            return BlockPruneResult.Skip;
        }

        if (typeof(T) == typeof(float))
        {
            float lowF = Unsafe.As<T, float>(ref constantLow);
            float highF = Unsafe.As<T, float>(ref constantHigh);
            if (float.IsNaN(lowF) || float.IsNaN(highF))
            {
                return BlockPruneResult.Skip;
            }
            float minF = Unsafe.As<T, float>(ref blockMin);
            float maxF = Unsafe.As<T, float>(ref blockMax);
            if (float.IsNaN(minF) || float.IsNaN(maxF))
            {
                return BlockPruneResult.Partial;
            }
        }
        else if (typeof(T) == typeof(double))
        {
            double lowD = Unsafe.As<T, double>(ref constantLow);
            double highD = Unsafe.As<T, double>(ref constantHigh);
            if (double.IsNaN(lowD) || double.IsNaN(highD))
            {
                return BlockPruneResult.Skip;
            }
            double minD = Unsafe.As<T, double>(ref blockMin);
            double maxD = Unsafe.As<T, double>(ref blockMax);
            if (double.IsNaN(minD) || double.IsNaN(maxD))
            {
                return BlockPruneResult.Partial;
            }
        }

        if (blockMax.CompareTo(constantLow) < 0 || blockMin.CompareTo(constantHigh) > 0)
        {
            return BlockPruneResult.Skip;
        }

        if (blockMin.CompareTo(constantLow) >= 0 && blockMax.CompareTo(constantHigh) <= 0)
        {
            return BlockPruneResult.AllMatch;
        }

        return BlockPruneResult.Partial;
    }

    // =========================================================================
    // LEVEL 2: IN-REGISTER SIMD EVALUATION OVER COMPRESSED BLOCKS
    // =========================================================================

    public static int EvaluateCompressedBlockToBitmap(
        ReadOnlySpan<byte> compressedBlock,
        float threshold,
        FilterOperation op,
        ReadOnlySpan<ulong> optionalValidityMask,
        Span<ulong> destinationBitmask)
    {
        if (compressedBlock.Length < AlpCompressor.HeaderSize)
            return 0;

        AlpCompressor.ReadHeader(compressedBlock, out var mode, out byte exp, out byte bitWidth,
            out _, out _, out ushort count, out ushort exCount, out long minBase, out _,
            out ulong zoneMapMinBits, out ulong zoneMapMaxBits);

        if (count == 0) return 0;
        int wordCount = (count + 63) >> 6;
        if (destinationBitmask.Length < wordCount)
            throw new ArgumentException("Destination bitmask too small.", nameof(destinationBitmask));

        destinationBitmask.Slice(0, wordCount).Clear();

        // Level-0: NaN Threshold Handling (IEEE 754 Semantics)
        if (float.IsNaN(threshold))
        {
            if (op == FilterOperation.NotEqual)
            {
                FillAllOnes(destinationBitmask, count, wordCount, optionalValidityMask);
                return CountBits(destinationBitmask, wordCount);
            }
            return 0;
        }

        // Level-1 Pruning
        float minF = BitConverter.UInt32BitsToSingle((uint)zoneMapMinBits);
        float maxF = BitConverter.UInt32BitsToSingle((uint)zoneMapMaxBits);
        var prune = PruneBlock(minF, maxF, count, nullCount: 0, threshold, op);

        if (prune == BlockPruneResult.Skip)
        {
            if (exCount == 0)
            {
                return 0;
            }

            return EvaluateExceptionsOnly(
                compressedBlock,
                count,
                bitWidth,
                exCount,
                threshold,
                op,
                optionalValidityMask,
                destinationBitmask,
                wordCount);
        }

        if (prune == BlockPruneResult.AllMatch)
        {
            FillAllOnes(destinationBitmask, count, wordCount, optionalValidityMask);
            if (exCount == 0)
            {
                return CountBits(destinationBitmask, wordCount);
            }

            // Regular rows are all 1s. Unmatch any exception rows that fail predicate in IEEE 754.
            int packedBytesEx = (count * bitWidth + 7) >> 3;
            int exOffsetAll = AlpCompressor.HeaderSize + packedBytesEx;
            int exValOffsetAll = exOffsetAll + (exCount * 2);

            for (int k = 0; k < exCount; k++)
            {
                ushort rowIdx = BinaryPrimitives.ReadUInt16LittleEndian(compressedBlock.Slice(exOffsetAll + (k * 2), 2));
                uint rawBits = BinaryPrimitives.ReadUInt32LittleEndian(compressedBlock.Slice(exValOffsetAll + (k * 4), 4));
                float rawFloat = BitConverter.UInt32BitsToSingle(rawBits);

                if (!MatchesFloat(rawFloat, threshold, op))
                {
                    int w = rowIdx >> 6;
                    int b = rowIdx & 63;
                    destinationBitmask[w] &= ~(1UL << b);
                }
            }

            return CountBits(destinationBitmask, wordCount);
        }

        if (exCount == count)
        {
            return EvaluateExceptionsOnly(
                compressedBlock,
                count,
                bitWidth,
                exCount,
                threshold,
                op,
                optionalValidityMask,
                destinationBitmask,
                wordCount);
        }

        if (mode == AlpCompressionMode.Constant)
        {
            bool match = MatchesFloat(minF, threshold, op);
            if (match)
            {
                FillAllOnes(destinationBitmask, count, wordCount, optionalValidityMask);
                return CountBits(destinationBitmask, wordCount);
            }
            return 0;
        }

        // Decimal integer domain transformation
        double factor = s_pow10[exp];
        double Y = (double)threshold * factor;

        int intMode; // 0=Ge, 1=Le, 2=Eq, 3=Ne, 4=AllZero, 5=AllOne
        long T;

        const double MaxLongDouble = 9223372036854775807.0; // long.MaxValue
        const double MinLongDouble = -9223372036854775808.0; // long.MinValue

        if (double.IsNaN(Y))
        {
            intMode = (op == FilterOperation.NotEqual) ? 5 : 4;
            T = 0;
        }
        else if (Y >= MaxLongDouble)
        {
            intMode = op switch
            {
                FilterOperation.LessThan or FilterOperation.LessThanOrEqual or FilterOperation.NotEqual => 5, // All 1s
                _ => 4 // All 0s
            };
            T = 0;
        }
        else if (Y < MinLongDouble)
        {
            intMode = op switch
            {
                FilterOperation.GreaterThan or FilterOperation.GreaterThanOrEqual or FilterOperation.NotEqual => 5, // All 1s
                _ => 4 // All 0s
            };
            T = 0;
        }
        else
        {
            long roundY = (long)Math.Round(Y);
            float reconstructed = (float)((double)roundY / factor);
            bool isExact = BitConverter.SingleToUInt32Bits(reconstructed) == BitConverter.SingleToUInt32Bits(threshold) || reconstructed == threshold;

            if (isExact)
            {
                switch (op)
                {
                    case FilterOperation.GreaterThan:
                        T = roundY + 1 - minBase;
                        intMode = 0; // Ge
                        break;
                    case FilterOperation.GreaterThanOrEqual:
                        T = roundY - minBase;
                        intMode = 0; // Ge
                        break;
                    case FilterOperation.LessThan:
                        T = roundY - 1 - minBase;
                        intMode = 1; // Le
                        break;
                    case FilterOperation.LessThanOrEqual:
                        T = roundY - minBase;
                        intMode = 1; // Le
                        break;
                    case FilterOperation.Equal:
                        T = roundY - minBase;
                        intMode = 2; // Eq
                        break;
                    case FilterOperation.NotEqual:
                        T = roundY - minBase;
                        intMode = 3; // Ne
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }
            else
            {
                switch (op)
                {
                    case FilterOperation.GreaterThan:
                        T = (long)Math.Floor(Y) + 1 - minBase;
                        intMode = 0; // Ge
                        break;
                    case FilterOperation.GreaterThanOrEqual:
                        T = (long)Math.Ceiling(Y) - minBase;
                        intMode = 0; // Ge
                        break;
                    case FilterOperation.LessThan:
                        T = (long)Math.Ceiling(Y) - 1 - minBase;
                        intMode = 1; // Le
                        break;
                    case FilterOperation.LessThanOrEqual:
                        T = (long)Math.Floor(Y) - minBase;
                        intMode = 1; // Le
                        break;
                    case FilterOperation.Equal:
                        intMode = 4; // All regular zeros
                        T = 0;
                        break;
                    case FilterOperation.NotEqual:
                        intMode = 5; // All regular ones
                        T = 0;
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }

            ulong maxUint = bitWidth == 32 ? uint.MaxValue : ((1UL << bitWidth) - 1UL);

            // Adjust boundary conditions
            if (intMode == 0) // Ge
            {
                if (T <= 0) intMode = 5; // All 1s
                else if (T > (long)maxUint) intMode = 4; // All 0s
            }
            else if (intMode == 1) // Le
            {
                if (T < 0) intMode = 4; // All 0s
                else if (T >= (long)maxUint) intMode = 5; // All 1s
            }
            else if (intMode == 2) // Eq
            {
                if (T < 0 || T > (long)maxUint) intMode = 4;
            }
            else if (intMode == 3) // Ne
            {
                if (T < 0 || T > (long)maxUint) intMode = 5;
            }
        }

        int packedBytes = (count * bitWidth + 7) >> 3;
        ReadOnlySpan<byte> packedPayload = compressedBlock.Slice(AlpCompressor.HeaderSize, packedBytes);

        // In-register SIMD integer evaluation in batches of 16
        Span<uint> lanes = stackalloc uint[16];
        uint threshU = (uint)Math.Clamp(T, 0, uint.MaxValue);
        int steps = (count + 15) >> 4;
        int stepBytes = (16 * bitWidth + 7) >> 3;
        Span<byte> chunkPad = stackalloc byte[64];

        for (int s = 0; s < steps; s++)
        {
            if (bitWidth == 0)
            {
                lanes.Clear();
            }
            else
            {
                int sliceOffset = s * stepBytes;
                if (sliceOffset + stepBytes <= packedPayload.Length)
                {
                    FastLanesBitUnpacker.Unpack16(packedPayload.Slice(sliceOffset, stepBytes), lanes, bitWidth);
                }
                else
                {
                    chunkPad.Clear();
                    int remaining = Math.Max(0, packedPayload.Length - sliceOffset);
                    if (remaining > 0)
                    {
                        packedPayload.Slice(sliceOffset, remaining).CopyTo(chunkPad);
                    }
                    FastLanesBitUnpacker.Unpack16(chunkPad.Slice(0, stepBytes), lanes, bitWidth);
                }
            }

            uint laneMask16;
            if (intMode == 4)
            {
                laneMask16 = 0;
            }
            else if (intMode == 5)
            {
                laneMask16 = 0xFFFF;
            }
            else if (Vector512.IsHardwareAccelerated)
            {
                var vUnpacked = Vector512.Create(lanes);
                var vThresh = Vector512.Create(threshU);
                var vCmp = intMode switch
                {
                    0 => Vector512.GreaterThanOrEqual(vUnpacked, vThresh),
                    1 => Vector512.LessThanOrEqual(vUnpacked, vThresh),
                    2 => Vector512.Equals(vUnpacked, vThresh),
                    3 => ~Vector512.Equals(vUnpacked, vThresh),
                    _ => Vector512<uint>.Zero
                };
                laneMask16 = (uint)vCmp.ExtractMostSignificantBits();
            }
            else
            {
                // Scalar 16-lane comparison
                laneMask16 = 0;
                for (int l = 0; l < 16; l++)
                {
                    uint u = lanes[l];
                    bool m = intMode switch
                    {
                        0 => u >= threshU,
                        1 => u <= threshU,
                        2 => u == threshU,
                        3 => u != threshU,
                        _ => false
                    };
                    if (m) laneMask16 |= (1u << l);
                }
            }

            int startRow = s * 16;
            int wordIdx = startRow >> 6;
            int bitShift = startRow & 63;
            destinationBitmask[wordIdx] |= ((ulong)laneMask16 << bitShift);
        }

        // Mask trailing bits past count
        int rem = count & 63;
        if (rem != 0)
        {
            ulong tailMask = (1UL << rem) - 1UL;
            destinationBitmask[wordCount - 1] &= tailMask;
        }

        // Intersect with validity mask
        if (!optionalValidityMask.IsEmpty)
        {
            int limit = Math.Min(wordCount, optionalValidityMask.Length);
            for (int w = 0; w < limit; w++)
            {
                destinationBitmask[w] &= optionalValidityMask[w];
            }
        }

        // Patch Exceptions dictionary
        int exOffset = AlpCompressor.HeaderSize + packedBytes;
        int exValOffset = exOffset + (exCount * 2);

        for (int k = 0; k < exCount; k++)
        {
            ushort rowIdx = BinaryPrimitives.ReadUInt16LittleEndian(compressedBlock.Slice(exOffset + (k * 2), 2));
            uint rawBits = BinaryPrimitives.ReadUInt32LittleEndian(compressedBlock.Slice(exValOffset + (k * 4), 4));
            float rawFloat = BitConverter.UInt32BitsToSingle(rawBits);

            bool matches = MatchesFloat(rawFloat, threshold, op);

            int w = rowIdx >> 6;
            int b = rowIdx & 63;
            ulong bit = 1UL << b;

            if (!optionalValidityMask.IsEmpty && w < optionalValidityMask.Length)
            {
                if ((optionalValidityMask[w] & bit) == 0)
                {
                    matches = false;
                }
            }

            if (matches)
            {
                destinationBitmask[w] |= bit;
            }
            else
            {
                destinationBitmask[w] &= ~bit;
            }
        }

        return CountBits(destinationBitmask, wordCount);
    }

    public static int EvaluateCompressedBlock(
        ReadOnlySpan<byte> compressedBlock,
        float threshold,
        FilterOperation op,
        ReadOnlySpan<ulong> optionalValidityMask,
        Span<int> destinationMatchingIndices)
    {
        if (compressedBlock.Length < AlpCompressor.HeaderSize)
            return 0;

        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(compressedBlock.Slice(0x0A, 2));
        if (count == 0) return 0;

        int wordCount = (count + 63) >> 6;
        Span<ulong> bitmask = stackalloc ulong[Math.Min(wordCount, 1024)];
        ulong[]? rented = null;
        if (wordCount > 1024)
        {
            rented = System.Buffers.ArrayPool<ulong>.Shared.Rent(wordCount);
            bitmask = rented.AsSpan(0, wordCount);
        }

        try
        {
            EvaluateCompressedBlockToBitmap(compressedBlock, threshold, op, optionalValidityMask, bitmask);

            int matchCount = 0;
            for (int w = 0; w < wordCount && matchCount < destinationMatchingIndices.Length; w++)
            {
                ulong word = bitmask[w];
                while (word != 0 && matchCount < destinationMatchingIndices.Length)
                {
                    int tz = BitOperations.TrailingZeroCount(word);
                    destinationMatchingIndices[matchCount++] = (w << 6) + tz;
                    word &= word - 1; // clear lowest set bit
                }
            }

            return matchCount;
        }
        finally
        {
            if (rented != null)
            {
                System.Buffers.ArrayPool<ulong>.Shared.Return(rented);
            }
        }
    }

    public static int EvaluateCompressedBlockDouble(
        ReadOnlySpan<byte> compressedBlock,
        double threshold,
        FilterOperation op,
        ReadOnlySpan<ulong> optionalValidityMask,
        Span<int> destinationMatchingIndices)
    {
        if (compressedBlock.Length < AlpCompressor.HeaderSize)
            return 0;

        AlpCompressor.ReadHeader(compressedBlock, out var mode, out byte exp, out byte bitWidth,
            out byte expR, out byte bitWidthR, out ushort count, out ushort exCount, out long minBase,
            out long minBaseR, out ulong minBits, out ulong maxBits);

        if (count == 0) return 0;

        double minD = BitConverter.UInt64BitsToDouble(minBits);
        double maxD = BitConverter.UInt64BitsToDouble(maxBits);

        if (double.IsNaN(threshold))
        {
            if (op == FilterOperation.NotEqual)
            {
                int matchCount = 0;
                for (int i = 0; i < count && matchCount < destinationMatchingIndices.Length; i++)
                {
                    int w = i >> 6;
                    int b = i & 63;
                    if (optionalValidityMask.IsEmpty || w >= optionalValidityMask.Length || (optionalValidityMask[w] & (1UL << b)) != 0)
                    {
                        destinationMatchingIndices[matchCount++] = i;
                    }
                }
                return matchCount;
            }
            return 0;
        }

        var prune = PruneBlock(minD, maxD, count, 0, threshold, op);
        if (prune == BlockPruneResult.Skip && exCount == 0)
        {
            return 0;
        }

        // Decompress and filter for 100% bit-exact correctness
        double[] tempDoubles = System.Buffers.ArrayPool<double>.Shared.Rent(count);
        try
        {
            AlpCompressor.DecompressDouble(compressedBlock, tempDoubles.AsSpan(0, count));

            int matchCount = 0;
            for (int i = 0; i < count && matchCount < destinationMatchingIndices.Length; i++)
            {
                int w = i >> 6;
                int b = i & 63;
                ulong bit = 1UL << b;

                if (!optionalValidityMask.IsEmpty && w < optionalValidityMask.Length)
                {
                    if ((optionalValidityMask[w] & bit) == 0) continue;
                }

                double val = tempDoubles[i];
                bool match = MatchesDouble(val, threshold, op);
                if (match)
                {
                    destinationMatchingIndices[matchCount++] = i;
                }
            }

            return matchCount;
        }
        finally
        {
            System.Buffers.ArrayPool<double>.Shared.Return(tempDoubles);
        }
    }

    // =========================================================================
    // HELPER UTILITIES
    // =========================================================================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FillAllOnes(Span<ulong> destinationBitmask, int count, int wordCount, ReadOnlySpan<ulong> optionalValidityMask)
    {
        for (int w = 0; w < wordCount; w++)
        {
            destinationBitmask[w] = (w == wordCount - 1 && (count & 63) != 0)
                ? ((1UL << (count & 63)) - 1UL)
                : ulong.MaxValue;

            if (!optionalValidityMask.IsEmpty && w < optionalValidityMask.Length)
            {
                destinationBitmask[w] &= optionalValidityMask[w];
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EvaluateExceptionsOnly(
        ReadOnlySpan<byte> compressedBlock,
        int count,
        int bitWidth,
        int exCount,
        float threshold,
        FilterOperation op,
        ReadOnlySpan<ulong> optionalValidityMask,
        Span<ulong> destinationBitmask,
        int wordCount)
    {
        int packedBytes = (count * bitWidth + 7) >> 3;
        int exOffset = AlpCompressor.HeaderSize + packedBytes;
        int exValOffset = exOffset + (exCount * 2);

        for (int k = 0; k < exCount; k++)
        {
            ushort rowIdx = BinaryPrimitives.ReadUInt16LittleEndian(compressedBlock.Slice(exOffset + (k * 2), 2));
            uint rawBits = BinaryPrimitives.ReadUInt32LittleEndian(compressedBlock.Slice(exValOffset + (k * 4), 4));
            float rawFloat = BitConverter.UInt32BitsToSingle(rawBits);

            if (MatchesFloat(rawFloat, threshold, op))
            {
                int w = rowIdx >> 6;
                int b = rowIdx & 63;
                ulong bit = 1UL << b;

                if (optionalValidityMask.IsEmpty || w >= optionalValidityMask.Length || (optionalValidityMask[w] & bit) != 0)
                {
                    destinationBitmask[w] |= bit;
                }
            }
        }

        return CountBits(destinationBitmask, wordCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountBits(Span<ulong> bitmask, int wordCount)
    {
        int count = 0;
        for (int w = 0; w < wordCount; w++)
        {
            count += BitOperations.PopCount(bitmask[w]);
        }
        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool MatchesFloat(float value, float threshold, FilterOperation op)
    {
        return op switch
        {
            FilterOperation.GreaterThan => value > threshold,
            FilterOperation.GreaterThanOrEqual => value >= threshold,
            FilterOperation.LessThan => value < threshold,
            FilterOperation.LessThanOrEqual => value <= threshold,
            FilterOperation.Equal => value == threshold,
            FilterOperation.NotEqual => value != threshold,
            _ => false
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool MatchesDouble(double value, double threshold, FilterOperation op)
    {
        return op switch
        {
            FilterOperation.GreaterThan => value > threshold,
            FilterOperation.GreaterThanOrEqual => value >= threshold,
            FilterOperation.LessThan => value < threshold,
            FilterOperation.LessThanOrEqual => value <= threshold,
            FilterOperation.Equal => value == threshold,
            FilterOperation.NotEqual => value != threshold,
            _ => false
        };
    }
}
