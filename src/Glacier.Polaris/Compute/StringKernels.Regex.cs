using Lokad.Utf8Regex;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;

namespace Glacier.Polaris.Compute
{
    public static partial class StringKernels
    {
        private static readonly ConcurrentDictionary<string, (Regex netRegex, Utf8Regex? utf8Regex)> _regexCache =
            new ConcurrentDictionary<string, (Regex netRegex, Utf8Regex? utf8Regex)>(StringComparer.Ordinal);

        private static (Regex netRegex, Utf8Regex? utf8Regex) GetOrAddRegex(string pattern) =>
            _regexCache.GetOrAdd(pattern, static p =>
            {
                var netRegex = new Regex(p, RegexOptions.Compiled | RegexOptions.CultureInvariant);
                Utf8Regex? utf8Regex = null;
                try
                {
                    utf8Regex = new Utf8Regex(p, RegexOptions.Compiled | RegexOptions.CultureInvariant);
                }
                catch
                {
                    // Fall back to netRegex if Utf8Regex fails to compile this pattern
                }
                return (netRegex, utf8Regex);
            });

        /// <summary>
        /// Filters a Utf8StringSeries using a Regex pattern.
        /// Returns a bitmask of matching indices.
        /// Uses Lokad.Utf8Regex to match directly on the UTF-8 bytes to avoid transcoding and allocation,
        /// and falls back to SIMD-accelerated string/anchor matches where applicable.
        /// </summary>
        public static unsafe void RegexMatch(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, string pattern, Span<int> results)
        {
            results.Clear();
            int rowCount = offsets.Length - 1;
            if (rowCount <= 0) return;

            var (patternClass, literal, suffix) = ClassifyPatternFull(pattern);

            if (patternClass == PatternClass.Literal)
            {
                byte[] literalBytes = Encoding.UTF8.GetBytes(literal);
                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    if (rowCount >= 1024)
                    {
                        var options = new System.Threading.Tasks.ParallelOptions
                        {
                            MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1)
                        };
                        System.Threading.Tasks.Parallel.For(0, rowCount, options, i =>
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;
                            if (length >= literalBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, length);
                                if (span.IndexOf(literalBytes) >= 0)
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        });
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;
                            if (length >= literalBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, length);
                                if (span.IndexOf(literalBytes) >= 0)
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        }
                    }
                }
            }
            else if (patternClass == PatternClass.StartsWith)
            {
                byte[] prefixBytes = Encoding.UTF8.GetBytes(literal);
                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    if (rowCount >= 1024)
                    {
                        var options = new System.Threading.Tasks.ParallelOptions
                        {
                            MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1)
                        };
                        System.Threading.Tasks.Parallel.For(0, rowCount, options, i =>
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;
                            if (length >= prefixBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, prefixBytes.Length);
                                if (span.SequenceEqual(prefixBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        });
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;
                            if (length >= prefixBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, prefixBytes.Length);
                                if (span.SequenceEqual(prefixBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        }
                    }
                }
            }
            else if (patternClass == PatternClass.EndsWith)
            {
                byte[] suffixBytes = Encoding.UTF8.GetBytes(literal);
                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    if (rowCount >= 1024)
                    {
                        var options = new System.Threading.Tasks.ParallelOptions
                        {
                            MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1)
                        };
                        System.Threading.Tasks.Parallel.For(0, rowCount, options, i =>
                        {
                            int start = pOffsetsLocal[i];
                            int end = pOffsetsLocal[i + 1];
                            int length = end - start;
                            if (length >= suffixBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + end - suffixBytes.Length, suffixBytes.Length);
                                if (span.SequenceEqual(suffixBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        });
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int end = pOffsetsLocal[i + 1];
                            int length = end - start;
                            if (length >= suffixBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + end - suffixBytes.Length, suffixBytes.Length);
                                if (span.SequenceEqual(suffixBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        }
                    }
                }
            }
            else if (patternClass == PatternClass.Equals)
            {
                byte[] literalBytes = Encoding.UTF8.GetBytes(literal);
                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    if (rowCount >= 1024)
                    {
                        var options = new System.Threading.Tasks.ParallelOptions
                        {
                            MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount - 1)
                        };
                        System.Threading.Tasks.Parallel.For(0, rowCount, options, i =>
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;
                            if (length == literalBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, length);
                                if (span.SequenceEqual(literalBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        });
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;
                            if (length == literalBytes.Length)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, length);
                                if (span.SequenceEqual(literalBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        }
                    }
                }
            }
            else if (patternClass == PatternClass.PrefixAndSuffix)
            {
                byte[] prefixBytes = Encoding.UTF8.GetBytes(literal);
                byte[] suffixBytes = Encoding.UTF8.GetBytes(suffix);
                int minLen = prefixBytes.Length + suffixBytes.Length;

                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    int numThreads = Math.Min(Environment.ProcessorCount, (rowCount + 1023) / 1024);
                    if (numThreads > 1 && rowCount >= 1024)
                    {
                        int chunkSize = (rowCount + numThreads - 1) / numThreads;
                        System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                        {
                            int startIdx = t * chunkSize;
                            int endIdx = Math.Min(startIdx + chunkSize, rowCount);
                            if (startIdx >= endIdx) return;

                            for (int i = startIdx; i < endIdx; i++)
                            {
                                int start = pOffsetsLocal[i];
                                int len = pOffsetsLocal[i + 1] - start;
                                if (len >= minLen)
                                {
                                    var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, len);
                                    if (span.StartsWith(prefixBytes) && span.EndsWith(suffixBytes))
                                    {
                                        pResultsLocal[i] = 1;
                                    }
                                }
                            }
                        });
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int len = pOffsetsLocal[i + 1] - start;
                            if (len >= minLen)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, len);
                                if (span.StartsWith(prefixBytes) && span.EndsWith(suffixBytes))
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        }
                    }
                }
            }
            else if (patternClass == PatternClass.ContainsBothOrdered)
            {
                byte[] p1Bytes = Encoding.UTF8.GetBytes(literal);
                byte[] p2Bytes = Encoding.UTF8.GetBytes(suffix);
                int minLen = p1Bytes.Length + p2Bytes.Length;

                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    int numThreads = Math.Min(Environment.ProcessorCount, (rowCount + 1023) / 1024);
                    if (numThreads > 1 && rowCount >= 1024)
                    {
                        int chunkSize = (rowCount + numThreads - 1) / numThreads;
                        System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                        {
                            int startIdx = t * chunkSize;
                            int endIdx = Math.Min(startIdx + chunkSize, rowCount);
                            if (startIdx >= endIdx) return;

                            for (int i = startIdx; i < endIdx; i++)
                            {
                                int start = pOffsetsLocal[i];
                                int len = pOffsetsLocal[i + 1] - start;
                                if (len >= minLen)
                                {
                                    var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, len);
                                    int idx1 = span.IndexOf(p1Bytes);
                                    if (idx1 >= 0 && span.Slice(idx1 + p1Bytes.Length).IndexOf(p2Bytes) >= 0)
                                    {
                                        pResultsLocal[i] = 1;
                                    }
                                }
                            }
                        });
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int len = pOffsetsLocal[i + 1] - start;
                            if (len >= minLen)
                            {
                                var span = new ReadOnlySpan<byte>(pDataBytesLocal + start, len);
                                int idx1 = span.IndexOf(p1Bytes);
                                if (idx1 >= 0 && span.Slice(idx1 + p1Bytes.Length).IndexOf(p2Bytes) >= 0)
                                {
                                    pResultsLocal[i] = 1;
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                var (netRegex, _) = GetOrAddRegex(pattern);

                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    byte* pDataBytesLocal = pDataBytes;
                    int* pOffsetsLocal = pOffsets;
                    int* pResultsLocal = pResults;

                    int numThreads = Math.Min(Environment.ProcessorCount, (rowCount + 1023) / 1024);
                    if (numThreads > 1 && rowCount >= 1024)
                    {
                        int chunkSize = (rowCount + numThreads - 1) / numThreads;
                        System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                        {
                            int startIdx = t * chunkSize;
                            int endIdx = Math.Min(startIdx + chunkSize, rowCount);
                            if (startIdx >= endIdx) return;

                            char[] buffer = new char[256];
                            for (int i = startIdx; i < endIdx; i++)
                            {
                                int start = pOffsetsLocal[i];
                                int length = pOffsetsLocal[i + 1] - start;

                                if (length == 0)
                                {
                                    if (netRegex.IsMatch(ReadOnlySpan<char>.Empty))
                                        pResultsLocal[i] = 1;
                                    continue;
                                }

                                var byteSpan = new ReadOnlySpan<byte>(pDataBytesLocal + start, length);
                                if (length > buffer.Length)
                                    buffer = new char[length * 2];

                                int charCount;
                                if (System.Text.Ascii.IsValid(byteSpan))
                                {
                                    System.Text.Ascii.ToUtf16(byteSpan, buffer, out charCount);
                                }
                                else
                                {
                                    charCount = Encoding.UTF8.GetChars(byteSpan, buffer);
                                }

                                if (netRegex.IsMatch(new ReadOnlySpan<char>(buffer, 0, charCount)))
                                    pResultsLocal[i] = 1;
                            }
                        });
                    }
                    else
                    {
                        char[] buffer = new char[256];
                        for (int i = 0; i < rowCount; i++)
                        {
                            int start = pOffsetsLocal[i];
                            int length = pOffsetsLocal[i + 1] - start;

                            if (length == 0)
                            {
                                if (netRegex.IsMatch(ReadOnlySpan<char>.Empty))
                                    pResultsLocal[i] = 1;
                                continue;
                            }

                            var byteSpan = new ReadOnlySpan<byte>(pDataBytesLocal + start, length);
                            if (length > buffer.Length)
                                buffer = new char[length * 2];

                            int charCount;
                            if (System.Text.Ascii.IsValid(byteSpan))
                            {
                                System.Text.Ascii.ToUtf16(byteSpan, buffer, out charCount);
                            }
                            else
                            {
                                charCount = Encoding.UTF8.GetChars(byteSpan, buffer);
                            }

                            if (netRegex.IsMatch(new ReadOnlySpan<char>(buffer, 0, charCount)))
                                pResultsLocal[i] = 1;
                        }
                    }
                }
            }
        }
    }
}
