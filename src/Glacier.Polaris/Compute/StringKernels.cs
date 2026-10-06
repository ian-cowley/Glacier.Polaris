using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Implements SIMD-accelerated string kernels.
    /// </summary>
    public static partial class StringKernels
    {
        public enum PatternClass
        {
            FullRegex,
            Literal,
            StartsWith,
            EndsWith,
            Equals,
            PrefixAndSuffix,
            ContainsBothOrdered
        }

        public static (PatternClass Class, string Literal) ClassifyPattern(string pattern)
        {
            var (cls, lit, _) = ClassifyPatternFull(pattern);
            return (cls, lit);
        }

        private static bool HasRegexMetaChars(string s)
        {
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '\\' || c == '*' || c == '+' || c == '?' || c == '|' ||
                    c == '{' || c == '}' || c == '[' || c == ']' || c == '(' || c == ')' ||
                    c == '.' || c == '#' || c == '^' || c == '$')
                {
                    return true;
                }
            }
            return false;
        }

        public static (PatternClass Class, string Literal, string Suffix) ClassifyPatternFull(string pattern)
        {
            if (string.IsNullOrEmpty(pattern))
                return (PatternClass.Literal, string.Empty, string.Empty);

            bool startsWithAnchor = pattern.StartsWith('^');
            bool endsWithAnchor = pattern.EndsWith('$');

            if (startsWithAnchor && endsWithAnchor)
            {
                if (pattern.Length <= 2)
                    return (PatternClass.FullRegex, pattern, string.Empty);

                string inner = pattern.Substring(1, pattern.Length - 2);
                int dotStar = inner.IndexOf(".*", StringComparison.Ordinal);
                if (dotStar >= 0 && inner.IndexOf(".*", dotStar + 2, StringComparison.Ordinal) < 0)
                {
                    string p1 = inner.Substring(0, dotStar);
                    string p2 = inner.Substring(dotStar + 2);
                    if (!HasRegexMetaChars(p1) && !HasRegexMetaChars(p2))
                        return (PatternClass.PrefixAndSuffix, p1, p2);
                }

                if (!HasRegexMetaChars(inner))
                    return (PatternClass.Equals, inner, string.Empty);
            }
            else if (startsWithAnchor)
            {
                string inner = pattern.Substring(1);
                if (inner.EndsWith(".*") && !HasRegexMetaChars(inner.Substring(0, inner.Length - 2)))
                    return (PatternClass.StartsWith, inner.Substring(0, inner.Length - 2), string.Empty);
                if (!HasRegexMetaChars(inner))
                    return (PatternClass.StartsWith, inner, string.Empty);
            }
            else if (endsWithAnchor)
            {
                string inner = pattern.Substring(0, pattern.Length - 1);
                if (inner.StartsWith(".*") && !HasRegexMetaChars(inner.Substring(2)))
                    return (PatternClass.EndsWith, inner.Substring(2), string.Empty);
                if (!HasRegexMetaChars(inner))
                    return (PatternClass.EndsWith, inner, string.Empty);
            }
            else
            {
                if (pattern.StartsWith(".*") && pattern.EndsWith(".*") && pattern.Length >= 4)
                {
                    string inner = pattern.Substring(2, pattern.Length - 4);
                    if (!HasRegexMetaChars(inner))
                        return (PatternClass.Literal, inner, string.Empty);
                }

                int dotStar = pattern.IndexOf(".*", StringComparison.Ordinal);
                if (dotStar >= 0 && pattern.IndexOf(".*", dotStar + 2, StringComparison.Ordinal) < 0)
                {
                    string p1 = pattern.Substring(0, dotStar);
                    string p2 = pattern.Substring(dotStar + 2);
                    if (!HasRegexMetaChars(p1) && !HasRegexMetaChars(p2))
                    {
                        if (p1.Length == 0) return (PatternClass.Literal, p2, string.Empty);
                        if (p2.Length == 0) return (PatternClass.Literal, p1, string.Empty);
                        return (PatternClass.ContainsBothOrdered, p1, p2);
                    }
                }

                if (!HasRegexMetaChars(pattern))
                    return (PatternClass.Literal, pattern, string.Empty);
            }

            return (PatternClass.FullRegex, pattern, string.Empty);
        }

        /// <summary>
        /// Compares a column of UTF-8 strings against a literal for equality.
        /// Returns a mask of matching indices.
        /// Accelerated via AVX2 SIMD and dynamic thread chunking.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe void Equals(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, ReadOnlySpan<byte> targetBytes, Span<int> results)
        {
            results.Clear();
            int targetLen = targetBytes.Length;
            int rowCount = offsets.Length - 1;
            if (rowCount <= 0) return;

            // Fast path for empty target string
            if (targetLen == 0)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (offsets[i + 1] - offsets[i] == 0) results[i] = 1;
                }
                return;
            }

            fixed (byte* pData = dataBytes)
            fixed (byte* pTarget = targetBytes)
            fixed (int* pOffsets = offsets)
            fixed (int* pResults = results)
            {
                byte* pDataLocal = pData;
                byte* pTargetLocal = pTarget;
                int* pOffsetsLocal = pOffsets;
                int* pResultsLocal = pResults;
                int dataBytesLen = dataBytes.Length;

                int numThreads = Math.Min(Environment.ProcessorCount, (rowCount + 1023) / 1024);
                if (numThreads > 1 && rowCount >= 1024)
                {
                    int chunkSize = (rowCount + numThreads - 1) / numThreads;
                    System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                    {
                        int startIdx = t * chunkSize;
                        int endIdx = Math.Min(startIdx + chunkSize, rowCount);
                        if (startIdx >= endIdx) return;

                        ProcessEqualsRange(pDataLocal, pTargetLocal, pOffsetsLocal, pResultsLocal, targetLen, dataBytesLen, startIdx, endIdx);
                    });
                }
                else
                {
                    ProcessEqualsRange(pDataLocal, pTargetLocal, pOffsetsLocal, pResultsLocal, targetLen, dataBytesLen, 0, rowCount);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static unsafe void ProcessEqualsRange(
            byte* pData,
            byte* pTarget,
            int* pOffsets,
            int* pResults,
            int targetLen,
            int dataBytesLength,
            int startIdx,
            int endIdx)
        {
            if (Vector256.IsHardwareAccelerated && targetLen <= 32)
            {
                byte* paddedTarget = stackalloc byte[32];
                System.Runtime.CompilerServices.Unsafe.InitBlockUnaligned(paddedTarget, 0, 32);
                System.Runtime.CompilerServices.Unsafe.CopyBlockUnaligned(paddedTarget, pTarget, (uint)targetLen);

                Vector256<byte> vTarget = Vector256.Load(paddedTarget);
                byte* paddedData = stackalloc byte[32];

                uint validMask = (1u << targetLen) - 1;

                int i = startIdx;
                for (; i <= endIdx - 4; i += 4)
                {
                    int start0 = pOffsets[i]; int len0 = pOffsets[i + 1] - start0;
                    int start1 = pOffsets[i + 1]; int len1 = pOffsets[i + 2] - start1;
                    int start2 = pOffsets[i + 2]; int len2 = pOffsets[i + 3] - start2;
                    int start3 = pOffsets[i + 3]; int len3 = pOffsets[i + 4] - start3;

                    if (len0 == targetLen)
                    {
                        if (start0 + 32 <= dataBytesLength)
                        {
                            var vData = Vector256.Load(pData + start0);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i] = 1;
                        }
                        else
                        {
                            System.Runtime.CompilerServices.Unsafe.InitBlockUnaligned(paddedData, 0, 32);
                            System.Runtime.CompilerServices.Unsafe.CopyBlockUnaligned(paddedData, pData + start0, (uint)len0);
                            var vData = Vector256.Load(paddedData);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i] = 1;
                        }
                    }
                    if (len1 == targetLen)
                    {
                        if (start1 + 32 <= dataBytesLength)
                        {
                            var vData = Vector256.Load(pData + start1);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i + 1] = 1;
                        }
                        else
                        {
                            System.Runtime.CompilerServices.Unsafe.InitBlockUnaligned(paddedData, 0, 32);
                            System.Runtime.CompilerServices.Unsafe.CopyBlockUnaligned(paddedData, pData + start1, (uint)len1);
                            var vData = Vector256.Load(paddedData);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i + 1] = 1;
                        }
                    }
                    if (len2 == targetLen)
                    {
                        if (start2 + 32 <= dataBytesLength)
                        {
                            var vData = Vector256.Load(pData + start2);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i + 2] = 1;
                        }
                        else
                        {
                            System.Runtime.CompilerServices.Unsafe.InitBlockUnaligned(paddedData, 0, 32);
                            System.Runtime.CompilerServices.Unsafe.CopyBlockUnaligned(paddedData, pData + start2, (uint)len2);
                            var vData = Vector256.Load(paddedData);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i + 2] = 1;
                        }
                    }
                    if (len3 == targetLen)
                    {
                        if (start3 + 32 <= dataBytesLength)
                        {
                            var vData = Vector256.Load(pData + start3);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i + 3] = 1;
                        }
                        else
                        {
                            System.Runtime.CompilerServices.Unsafe.InitBlockUnaligned(paddedData, 0, 32);
                            System.Runtime.CompilerServices.Unsafe.CopyBlockUnaligned(paddedData, pData + start3, (uint)len3);
                            var vData = Vector256.Load(paddedData);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i + 3] = 1;
                        }
                    }
                }
                for (; i < endIdx; i++)
                {
                    int start = pOffsets[i];
                    int len = pOffsets[i + 1] - start;
                    if (len == targetLen)
                    {
                        if (start + 32 <= dataBytesLength)
                        {
                            var vData = Vector256.Load(pData + start);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i] = 1;
                        }
                        else
                        {
                            System.Runtime.CompilerServices.Unsafe.InitBlockUnaligned(paddedData, 0, 32);
                            System.Runtime.CompilerServices.Unsafe.CopyBlockUnaligned(paddedData, pData + start, (uint)len);
                            var vData = Vector256.Load(paddedData);
                            if ((Vector256.Equals(vTarget, vData).ExtractMostSignificantBits() & validMask) == validMask)
                                pResults[i] = 1;
                        }
                    }
                }
            }
            else
            {
                ReadOnlySpan<byte> targetSpan = new ReadOnlySpan<byte>(pTarget, targetLen);
                for (int i = startIdx; i < endIdx; i++)
                {
                    int start = pOffsets[i];
                    int len = pOffsets[i + 1] - start;
                    if (len == targetLen)
                    {
                        if (new ReadOnlySpan<byte>(pData + start, len).SequenceEqual(targetSpan))
                        {
                            pResults[i] = 1;
                        }
                    }
                }
            }
        }

        public static void Contains(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, string pattern, Span<int> results)
        {
            results.Clear();
            byte[] patternBytes = Encoding.UTF8.GetBytes(pattern);
            int rowCount = offsets.Length - 1;
            for (int i = 0; i < rowCount; i++)
            {
                int start = offsets[i];
                int end = offsets[i + 1];
                int length = end - start;
                if (length >= patternBytes.Length)
                {
                    var stringSpan = dataBytes.Slice(start, length);
                    if (stringSpan.IndexOf(patternBytes) >= 0)
                    {
                        results[i] = 1;
                    }
                }
            }
        }

        public static void StartsWith(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, string prefix, Span<int> results)
        {
            results.Clear();
            byte[] prefixBytes = Encoding.UTF8.GetBytes(prefix);
            int rowCount = offsets.Length - 1;
            for (int i = 0; i < rowCount; i++)
            {
                int start = offsets[i];
                int length = offsets[i + 1] - start;
                if (length >= prefixBytes.Length)
                {
                    if (dataBytes.Slice(start, prefixBytes.Length).SequenceEqual(prefixBytes))
                    {
                        results[i] = 1;
                    }
                }
            }
        }

        public static void EndsWith(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, string suffix, Span<int> results)
        {
            results.Clear();
            byte[] suffixBytes = Encoding.UTF8.GetBytes(suffix);
            int rowCount = offsets.Length - 1;
            for (int i = 0; i < rowCount; i++)
            {
                int start = offsets[i];
                int end = offsets[i + 1];
                int length = end - start;
                if (length >= suffixBytes.Length)
                {
                    if (dataBytes.Slice(end - suffixBytes.Length, suffixBytes.Length).SequenceEqual(suffixBytes))
                    {
                        results[i] = 1;
                    }
                }
            }
        }
    }
}
