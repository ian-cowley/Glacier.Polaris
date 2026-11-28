using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Glacier.Polaris.Compute
{
    public static partial class StringKernels
    {
        internal const int RegexCacheCapacity = 256;
        private const RegexOptions DefaultRegexOptions = RegexOptions.Compiled | RegexOptions.CultureInvariant;
        private static readonly TimeSpan DefaultRegexTimeout = TimeSpan.FromSeconds(5);

        private readonly struct RegexCacheKey : IEquatable<RegexCacheKey>
        {
            public string Pattern { get; }
            public RegexOptions Options { get; }
            public TimeSpan MatchTimeout { get; }

            public RegexCacheKey(string pattern, RegexOptions options, TimeSpan matchTimeout)
            {
                Pattern = pattern;
                Options = options;
                MatchTimeout = matchTimeout;
            }

            public bool Equals(RegexCacheKey other) =>
                Options == other.Options &&
                MatchTimeout == other.MatchTimeout &&
                StringComparer.Ordinal.Equals(Pattern, other.Pattern);

            public override bool Equals(object? obj) => obj is RegexCacheKey other && Equals(other);

            public override int GetHashCode() =>
                HashCode.Combine(StringComparer.Ordinal.GetHashCode(Pattern), Options, MatchTimeout);
        }

        private sealed class RegexCacheEntry
        {
            public readonly RegexCacheKey Key;
            public readonly Regex Value;

            public RegexCacheEntry(RegexCacheKey key, Regex value)
            {
                Key = key;
                Value = value;
            }
        }

        private static readonly Lock _regexCacheLock = new();
        private static readonly Dictionary<RegexCacheKey, LinkedListNode<RegexCacheEntry>> _regexCacheMap = new(RegexCacheCapacity);
        private static readonly LinkedList<RegexCacheEntry> _regexCacheLru = new();

        private static Regex GetOrAddRegex(string pattern) =>
            GetOrAddRegex(pattern, DefaultRegexOptions, DefaultRegexTimeout);

        internal static Regex GetOrAddRegex(string pattern, RegexOptions options, TimeSpan matchTimeout)
        {
            ArgumentNullException.ThrowIfNull(pattern);
            var key = new RegexCacheKey(pattern, options, matchTimeout);

            lock (_regexCacheLock)
            {
                if (_regexCacheMap.TryGetValue(key, out var existingNode))
                {
                    _regexCacheLru.Remove(existingNode);
                    _regexCacheLru.AddLast(existingNode);
                    return existingNode.Value.Value;
                }
            }

            // Compile outside the lock so invalid patterns throw cleanly without blocking or poisoning the cache.
            var compiled = new Regex(pattern, options, matchTimeout);

            lock (_regexCacheLock)
            {
                if (_regexCacheMap.TryGetValue(key, out var existingNode))
                {
                    _regexCacheLru.Remove(existingNode);
                    _regexCacheLru.AddLast(existingNode);
                    return existingNode.Value.Value;
                }

                while (_regexCacheMap.Count >= RegexCacheCapacity && _regexCacheLru.First is { } oldest)
                {
                    _regexCacheLru.RemoveFirst();
                    _regexCacheMap.Remove(oldest.Value.Key);
                }

                var newNode = new LinkedListNode<RegexCacheEntry>(new RegexCacheEntry(key, compiled));
                _regexCacheLru.AddLast(newNode);
                _regexCacheMap[key] = newNode;
                return compiled;
            }
        }

        internal static int RegexCacheCount
        {
            get
            {
                lock (_regexCacheLock)
                {
                    return _regexCacheMap.Count;
                }
            }
        }

        internal static void ClearRegexCacheForTests()
        {
            lock (_regexCacheLock)
            {
                _regexCacheMap.Clear();
                _regexCacheLru.Clear();
            }
        }

        /// <summary>
        /// Filters a Utf8StringSeries using a Regex pattern.
        /// Returns a bitmask of matching indices.
        /// Uses SIMD-accelerated UTF-8 byte scans for literal/anchor patterns and compiled span-based Regex fallback.
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
                var netRegex = GetOrAddRegex(pattern);

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
