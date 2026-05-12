using System;
using System.Collections.Concurrent;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Text.RegularExpressions;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Implements SIMD-accelerated string kernels.
    /// </summary>
    public static class StringKernels
    {
        private static readonly ConcurrentDictionary<string, Regex> _regexCache =
            new ConcurrentDictionary<string, Regex>(StringComparer.Ordinal);

        private static Regex GetOrAddRegex(string pattern) =>
            _regexCache.GetOrAdd(pattern, static p =>
                new Regex(p, RegexOptions.Compiled | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(10)));
        /// <summary>
        /// Compares a column of UTF-8 strings against a literal for equality.
        /// Returns a mask of matching indices.
        /// </summary>
        public static unsafe void Equals(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, ReadOnlySpan<byte> literal, Span<int> results)
        {
            int rowCount = offsets.Length - 1;
            for (int i = 0; i < rowCount; i++)
            {
                int start = offsets[i];
                int end = offsets[i + 1];
                int length = end - start;

                if (length == literal.Length)
                {
                    var stringSpan = dataBytes.Slice(start, length);
                    if (stringSpan.SequenceEqual(literal))
                    {
                        results[i] = 1;
                    }
                }
            }
        }
        /// <summary>
        /// Filters a Utf8StringSeries using a Regex pattern.
        /// Returns a bitmask of matching indices.
        /// Uses a thread-local char buffer to transcode UTF-8 → UTF-16 without heap allocation,
        /// then matches against ReadOnlySpan&lt;char&gt; so no string is ever interned.
        /// </summary>
        public static unsafe void RegexMatch(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, string pattern, Span<int> results)
        {
            var regex = GetOrAddRegex(pattern);
            int rowCount = offsets.Length - 1;

            int numThreads = Math.Max(1, Environment.ProcessorCount);
            if (rowCount < numThreads * 2)
            {
                fixed (byte* pDataBytes = dataBytes)
                fixed (int* pOffsets = offsets)
                fixed (int* pResults = results)
                {
                    char[] buffer = new char[256];
                    for (int i = 0; i < rowCount; i++)
                    {
                        int start = pOffsets[i];
                        int end = pOffsets[i + 1];
                        int length = end - start;

                        if (length == 0)
                        {
                            if (regex.IsMatch(ReadOnlySpan<char>.Empty))
                                pResults[i] = 1;
                            continue;
                        }

                        int maxChars = Encoding.UTF8.GetMaxCharCount(length);
                        if (maxChars > buffer.Length)
                            buffer = new char[maxChars * 2];

                        int charCount = Encoding.UTF8.GetChars(
                            new ReadOnlySpan<byte>(pDataBytes + start, length), buffer);

                        if (regex.IsMatch(new ReadOnlySpan<char>(buffer, 0, charCount)))
                            pResults[i] = 1;
                    }
                }
                return;
            }

            fixed (byte* pDataBytes = dataBytes)
            fixed (int* pOffsets = offsets)
            fixed (int* pResults = results)
            {
                byte* pDataBytesLocal = pDataBytes;
                int* pOffsetsLocal = pOffsets;
                int* pResultsLocal = pResults;

                System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                {
                    int startRow = (int)((long)t * rowCount / numThreads);
                    int endRow = (int)((long)(t + 1) * rowCount / numThreads);
                    char[] buffer = new char[256];

                    for (int i = startRow; i < endRow; i++)
                    {
                        int start = pOffsetsLocal[i];
                        int end = pOffsetsLocal[i + 1];
                        int length = end - start;

                        if (length == 0)
                        {
                            if (regex.IsMatch(ReadOnlySpan<char>.Empty))
                                pResultsLocal[i] = 1;
                            continue;
                        }

                        int maxChars = Encoding.UTF8.GetMaxCharCount(length);
                        if (maxChars > buffer.Length)
                            buffer = new char[maxChars * 2];

                        int charCount = Encoding.UTF8.GetChars(
                            new ReadOnlySpan<byte>(pDataBytesLocal + start, length), buffer);

                        if (regex.IsMatch(new ReadOnlySpan<char>(buffer, 0, charCount)))
                            pResultsLocal[i] = 1;
                    }
                });
            }
        }        /// <summary>
                 /// Materializes a new Utf8StringSeries based on a set of chosen row indices.
                 /// </summary>
        public static Data.Utf8StringSeries Take(Data.Utf8StringSeries source, ReadOnlySpan<int> indices)
        {
            if (indices.Length == 0)
            {
                return new Data.Utf8StringSeries(source.Name, 0, 0);
            }

            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = 0;

            for (int i = 0; i < indices.Length; i++)
            {
                int idx = indices[i];
                totalBytes += srcOffsets[idx + 1] - srcOffsets[idx];
            }

            var result = new Data.Utf8StringSeries(source.Name, indices.Length, totalBytes);
            var destOffsets = result.Offsets.Span;
            var destData = result.DataBytes.Span;

            int currentOffset = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                destOffsets[i] = currentOffset;
                int idx = indices[i];
                int start = srcOffsets[idx];
                int length = srcOffsets[idx + 1] - start;

                if (length > 0)
                {
                    srcData.Slice(start, length).CopyTo(destData.Slice(currentOffset));
                    currentOffset += length;
                }
            }
            destOffsets[indices.Length] = currentOffset;

            return result;
        }

        public static Data.Utf8StringSeries TakeWithNulls(Data.Utf8StringSeries source, ReadOnlySpan<int> indices)
        {
            if (indices.Length == 0) return new Data.Utf8StringSeries(source.Name, 0, 0);

            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = 0;

            for (int i = 0; i < indices.Length; i++)
            {
                int idx = indices[i];
                if (idx != -1) totalBytes += srcOffsets[idx + 1] - srcOffsets[idx];
            }

            var result = new Data.Utf8StringSeries(source.Name, indices.Length, totalBytes);
            var destOffsets = result.Offsets.Span;
            var destData = result.DataBytes.Span;

            int currentOffset = 0;
            for (int i = 0; i < indices.Length; i++)
            {
                destOffsets[i] = currentOffset;
                int idx = indices[i];
                if (idx == -1)
                {
                    result.ValidityMask.SetNull(i);
                }
                else
                {
                    int start = srcOffsets[idx];
                    int length = srcOffsets[idx + 1] - start;

                    if (length > 0)
                    {
                        srcData.Slice(start, length).CopyTo(destData.Slice(currentOffset));
                        currentOffset += length;
                    }
                }
            }
            destOffsets[indices.Length] = currentOffset;

            return result;
        }
        public static unsafe void Lengths(ReadOnlySpan<int> offsets, Span<int> result)
        {
            int rowCount = offsets.Length - 1;
            if (Vector256.IsHardwareAccelerated && rowCount >= 8)
            {
                fixed (int* pOffsets = offsets)
                fixed (int* pResult = result)
                {
                    int i = 0;
                    for (; i <= rowCount - 8; i += 8)
                    {
                        var o1 = Vector256.Load(pOffsets + i);
                        var o2 = Vector256.Load(pOffsets + i + 1);
                        var lengths = Vector256.Subtract(o2, o1);
                        lengths.Store(pResult + i);
                    }
                    for (; i < rowCount; i++)
                    {
                        result[i] = offsets[i + 1] - offsets[i];
                    }
                }
            }
            else
            {
                for (int i = 0; i < rowCount; i++)
                {
                    result[i] = offsets[i + 1] - offsets[i];
                }
            }
        }

        public static void Contains(ReadOnlySpan<byte> dataBytes, ReadOnlySpan<int> offsets, string pattern, Span<int> results)
        {
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

        /// <summary>Optimized ASCII-uppercase: operates directly on UTF-8 bytes.</summary>
        public static unsafe Data.Utf8StringSeries ToUppercase(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = source.DataBytes.Length;

            var result = new Data.Utf8StringSeries(source.Name, rowCount, totalBytes);
            var destOffsets = result.Offsets.Span;
            var destData = result.DataBytes.Span;

            fixed (byte* pSrc = srcData)
            fixed (byte* pDst = destData)
            fixed (int* pOff = srcOffsets)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    int start = pOff[i];
                    int end = pOff[i + 1];
                    int len = end - start;
                    destOffsets[i] = start;

                    byte* src = pSrc + start;
                    byte* dst = pDst + start;
                    for (int j = 0; j < len; j++)
                    {
                        byte b = src[j];
                        // ASCII lowercase 'a'-'z' -> uppercase
                        dst[j] = (byte)(b - (b >= 97 && b <= 122 ? 32u : 0u));
                    }
                }
                destOffsets[rowCount] = srcOffsets[rowCount];
            }
            return result;
        }

        /// <summary>Optimized ASCII-lowercase: operates directly on UTF-8 bytes.</summary>
        public static unsafe Data.Utf8StringSeries ToLowercase(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = source.DataBytes.Length;

            var result = new Data.Utf8StringSeries(source.Name, rowCount, totalBytes);
            var destOffsets = result.Offsets.Span;
            var destData = result.DataBytes.Span;

            fixed (byte* pSrc = srcData)
            fixed (byte* pDst = destData)
            fixed (int* pOff = srcOffsets)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    int start = pOff[i];
                    int end = pOff[i + 1];
                    int len = end - start;
                    destOffsets[i] = start;

                    byte* src = pSrc + start;
                    byte* dst = pDst + start;
                    for (int j = 0; j < len; j++)
                    {
                        byte b = src[j];
                        dst[j] = (byte)(b + (b >= 65 && b <= 90 ? 32u : 0u));
                    }
                }
                destOffsets[rowCount] = srcOffsets[rowCount];
            }
            return result;
        }
        /// <summary>Replace first occurrence of a pattern in each string.</summary>
        public static Data.Utf8StringSeries Replace(Data.Utf8StringSeries source, string oldValue, string newValue)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i))
                {
                    strings[i] = null!;
                    continue;
                }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                int idx = s.IndexOf(oldValue, StringComparison.Ordinal);
                strings[i] = idx >= 0 ? s.Substring(0, idx) + newValue + s.Substring(idx + oldValue.Length) : s;
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Replace all occurrences of a pattern in each string.</summary>
        public static Data.Utf8StringSeries ReplaceAll(Data.Utf8StringSeries source, string oldValue, string newValue)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i))
                {
                    strings[i] = null!;
                    continue;
                }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Replace(oldValue, newValue);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Strip whitespace from both ends of each string.</summary>
        public static Data.Utf8StringSeries Strip(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                strings[i] = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i)).Trim();
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Strip whitespace from the start of each string.</summary>
        public static Data.Utf8StringSeries LStrip(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                strings[i] = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i)).TrimStart();
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Strip whitespace from the end of each string.</summary>
        public static Data.Utf8StringSeries RStrip(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                strings[i] = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i)).TrimEnd();
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Split each string by a separator, returning a ListSeries.</summary>
        public static Data.ListSeries Split(Data.Utf8StringSeries source, string separator, int? maxSplits = null)
        {
            int rowCount = source.Length;
            // First pass: calculate total number of elements
            int totalElements = 0;
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) continue;
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (maxSplits.HasValue)
                {
                    var parts = s.Split(separator, maxSplits.Value, StringSplitOptions.None);
                    totalElements += parts.Length;
                }
                else
                {
                    var parts = s.Split(new[] { separator }, StringSplitOptions.None);
                    totalElements += parts.Length;
                }
            }

            var values = new string[totalElements];
            var offsets = new int[rowCount + 1];
            int idx = 0;
            for (int i = 0; i < rowCount; i++)
            {
                offsets[i] = idx;
                if (source.ValidityMask.IsNull(i)) continue;
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                string[] parts;
                if (maxSplits.HasValue)
                    parts = s.Split(separator, maxSplits.Value, StringSplitOptions.None);
                else
                    parts = s.Split(new[] { separator }, StringSplitOptions.None);
                foreach (var part in parts) values[idx++] = part;
            }
            offsets[rowCount] = idx;

            var valueSeries = Data.Utf8StringSeries.FromStrings(source.Name + "_split", values);
            var offsetSeries = new Data.Int32Series(source.Name + "_offsets", offsets);
            return new Data.ListSeries(source.Name, offsetSeries, valueSeries);
        }

        /// <summary>Slice each string: start position and optional length.</summary>
        public static Data.Utf8StringSeries Slice(Data.Utf8StringSeries source, int start, int? length = null)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (length.HasValue)
                    strings[i] = start >= 0 && start < s.Length ? s.Substring(start, Math.Min(length.Value, s.Length - start)) : string.Empty;
                else
                    strings[i] = start >= 0 && start < s.Length ? s.Substring(start) : string.Empty;
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Parse strings to DateSeries using optional format.</summary>
        public static Data.DateSeries ParseDate(Data.Utf8StringSeries source, string? format = null)
        {
            int rowCount = source.Length;
            var result = new Data.DateSeries(source.Name + "_date", rowCount);
            var span = result.Memory.Span;
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (DateTime.TryParseExact(s, format ?? "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.None, out var dt))
                {
                    span[i] = (int)(dt.Date - epoch).TotalDays;
                }
                else if (format == null && DateTime.TryParse(s, out dt))
                {
                    span[i] = (int)(dt.Date - epoch).TotalDays;
                }
                else
                {
                    result.ValidityMask.SetNull(i);
                }
            }
            return result;
        }

        /// <summary>Parse strings to DatetimeSeries using optional format.</summary>
        public static Data.DatetimeSeries ParseDatetime(Data.Utf8StringSeries source, string? format = null)
        {
            int rowCount = source.Length;
            var result = new Data.DatetimeSeries(source.Name + "_datetime", rowCount);
            var span = result.Memory.Span;
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (DateTime.TryParseExact(s, format ?? "yyyy-MM-dd HH:mm:ss", null, System.Globalization.DateTimeStyles.AssumeUniversal, out var dt))
                {
                    span[i] = (dt.Ticks - epoch.Ticks) * 100; // Convert to nanoseconds
                }
                else if (format == null && DateTime.TryParse(s, out dt))
                {
                    span[i] = (dt.ToUniversalTime().Ticks - epoch.Ticks) * 100;
                }
                else
                {
                    result.ValidityMask.SetNull(i);
                }
            }
            return result;
        }    /// <summary>Extract first n characters from each string.</summary>
        public static Data.Utf8StringSeries Head(Data.Utf8StringSeries source, int n)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length <= n ? s : s.Substring(0, n);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Extract last n characters from each string.</summary>
        public static Data.Utf8StringSeries Tail(Data.Utf8StringSeries source, int n)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length <= n ? s : s.Substring(s.Length - n);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Pad each string on the left to the specified width.</summary>
        public static Data.Utf8StringSeries PadStart(Data.Utf8StringSeries source, int width, char fillChar = ' ')
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length >= width ? s : s.PadLeft(width, fillChar);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Pad each string on the right to the specified width.</summary>
        public static Data.Utf8StringSeries PadEnd(Data.Utf8StringSeries source, int width, char fillChar = ' ')
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length >= width ? s : s.PadRight(width, fillChar);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Convert each string to title case.</summary>
        public static Data.Utf8StringSeries ToTitlecase(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                var textInfo = System.Globalization.CultureInfo.InvariantCulture.TextInfo;
                strings[i] = textInfo.ToTitleCase(s);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }
        /// <summary>Extract the first match of a regex pattern from each string.</summary>
        public static Data.Utf8StringSeries Extract(Data.Utf8StringSeries source, string pattern)
        {
            var regex = GetOrAddRegex(pattern);
            int rowCount = source.Length;
            var strings = new string[rowCount];

            int numThreads = Math.Max(1, Environment.ProcessorCount);
            if (rowCount < numThreads * 2)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                    var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                    var match = regex.Match(s);
                    strings[i] = match.Success ? match.Value : string.Empty;
                }
            }
            else
            {
                System.Threading.Tasks.Parallel.For(0, numThreads, t =>
                {
                    int startRow = (int)((long)t * rowCount / numThreads);
                    int endRow = (int)((long)(t + 1) * rowCount / numThreads);
                    for (int i = startRow; i < endRow; i++)
                    {
                        if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                        var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                        var match = regex.Match(s);
                        strings[i] = match.Success ? match.Value : string.Empty;
                    }
                });
            }

            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }
        /// <summary>Reverse each string.</summary>
        public static Data.Utf8StringSeries Reverse(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                var arr = s.ToCharArray();
                Array.Reverse(arr);
                strings[i] = new string(arr);
            }
            return Data.Utf8StringSeries.FromStrings(source.Name, strings);
        }/// <summary>Extract all matches of a regex pattern from each string, returning a ListSeries.</summary>
        public static Data.ListSeries ExtractAll(Data.Utf8StringSeries source, string pattern)
        {
            int rowCount = source.Length;
            var regex = new System.Text.RegularExpressions.Regex(pattern, System.Text.RegularExpressions.RegexOptions.Compiled);

            // First pass: count total matches and collect strings
            var allMatches = new System.Collections.Generic.List<string>();
            var offsets = new int[rowCount + 1];
            int total = 0;
            for (int i = 0; i < rowCount; i++)
            {
                offsets[i] = total;
                if (!source.ValidityMask.IsNull(i))
                {
                    var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                    var matches = regex.Matches(s);
                    foreach (System.Text.RegularExpressions.Match m in matches)
                    {
                        allMatches.Add(m.Value);
                        total++;
                    }
                }
            }
            offsets[rowCount] = total;

            var valueSeries = Data.Utf8StringSeries.FromStrings(source.Name + "_extract_all", allMatches.ToArray());
            var offsetSeries = new Data.Int32Series(source.Name + "_offsets", offsets);
            return new Data.ListSeries(source.Name, offsetSeries, valueSeries);
        }

        /// <summary>Decode JSON strings into a StructSeries. Each string must be a JSON object.</summary>
        public static Data.StructSeries JsonDecode(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var options = new System.Text.Json.JsonDocumentOptions { AllowTrailingCommas = true };

            // Discover all property names from non-null rows
            var propertyNames = new System.Collections.Generic.HashSet<string>(System.StringComparer.Ordinal);
            var parsedDocs = new System.Text.Json.JsonDocument[rowCount];

            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) continue;
                var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (string.IsNullOrWhiteSpace(s)) continue;
                try
                {
                    parsedDocs[i] = System.Text.Json.JsonDocument.Parse(s, options);
                    foreach (var prop in parsedDocs[i].RootElement.EnumerateObject())
                    {
                        propertyNames.Add(prop.Name);
                    }
                }
                catch
                {
                    // Ignore parse failures
                }
            }

            // Build field series
            var names = propertyNames.ToArray();
            var fields = new System.Collections.Generic.List<ISeries>();

            foreach (var propName in names)
            {
                // Infer type from first non-null occurrence
                System.Type inferredType = typeof(string);
                for (int i = 0; i < rowCount; i++)
                {
                    if (parsedDocs[i] == null) continue;
                    if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                    {
                        inferredType = prop.ValueKind switch
                        {
                            System.Text.Json.JsonValueKind.Number => typeof(double),
                            System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False => typeof(bool),
                            System.Text.Json.JsonValueKind.String => typeof(string),
                            _ => typeof(string)
                        };
                        break;
                    }
                }

                if (inferredType == typeof(double))
                {
                    var field = new Data.Float64Series(propName, rowCount);
                    var span = field.Memory.Span;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (parsedDocs[i] == null)
                        {
                            field.ValidityMask.SetNull(i);
                        }
                        else if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                        {
                            if (prop.ValueKind == System.Text.Json.JsonValueKind.Number)
                                span[i] = prop.GetDouble();
                            else if (prop.ValueKind == System.Text.Json.JsonValueKind.String && double.TryParse(prop.GetString(), out var d))
                                span[i] = d;
                            else
                                field.ValidityMask.SetNull(i);
                        }
                        else
                        {
                            field.ValidityMask.SetNull(i);
                        }
                    }
                    fields.Add(field);
                }
                else if (inferredType == typeof(bool))
                {
                    var field = new Data.BooleanSeries(propName, rowCount);
                    var span = field.Memory.Span;
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (parsedDocs[i] == null)
                        {
                            field.ValidityMask.SetNull(i);
                        }
                        else if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                        {
                            if (prop.ValueKind == System.Text.Json.JsonValueKind.True || prop.ValueKind == System.Text.Json.JsonValueKind.False)
                                span[i] = prop.GetBoolean();
                            else
                                field.ValidityMask.SetNull(i);
                        }
                        else
                        {
                            field.ValidityMask.SetNull(i);
                        }
                    }
                    fields.Add(field);
                }
                else
                {
                    // String type
                    var strings = new string[rowCount];
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (parsedDocs[i] == null)
                        {
                            strings[i] = null!;
                        }
                        else if (parsedDocs[i].RootElement.TryGetProperty(propName, out var prop))
                        {
                            strings[i] = prop.ValueKind == System.Text.Json.JsonValueKind.String
                                ? prop.GetString()
                                : prop.GetRawText();
                        }
                        else
                        {
                            strings[i] = null!;
                        }
                    }
                    var field = Data.Utf8StringSeries.FromStrings(propName, strings);
                    fields.Add(field);
                }
            }

            // Cleanup
            for (int i = 0; i < rowCount; i++)
            {
                parsedDocs[i]?.Dispose();
            }

            var result = new Data.StructSeries(source.Name + "_decoded", fields.ToArray());
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i))
                    result.ValidityMask.SetNull(i);
            }
            return result;
        }

        /// <summary>Encode each string as a JSON string value (wraps in quotes with proper escaping).</summary>
        public static Data.Utf8StringSeries JsonEncode(Data.Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            int totalBytes = 0;
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i))
                {
                    strings[i] = "null";
                    totalBytes += 4;
                }
                else
                {
                    var s = System.Text.Encoding.UTF8.GetString(source.GetStringSpan(i));
                    strings[i] = System.Text.Json.JsonSerializer.Serialize(s);
                    totalBytes += System.Text.Encoding.UTF8.GetByteCount(strings[i]);
                }
            }

            var result = new Data.Utf8StringSeries(source.Name + "_json", rowCount, totalBytes);
            int offset = 0;
            for (int i = 0; i < rowCount; i++)
            {
                result.Offsets.Span[i] = offset;
                var bytes = System.Text.Encoding.UTF8.GetBytes(strings[i]);
                bytes.CopyTo(result.DataBytes.Span.Slice(offset));
                offset += bytes.Length;
                if (source.ValidityMask.IsNull(i))
                    result.ValidityMask.SetNull(i);
            }
            result.Offsets.Span[rowCount] = offset;
            return result;
        }
    }
}
