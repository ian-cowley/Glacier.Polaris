using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static partial class StringKernels
    {
        /// <summary>
        /// Materializes a new Utf8StringSeries based on a set of chosen row indices.
        /// </summary>
        public static Utf8StringSeries Take(Utf8StringSeries source, ReadOnlySpan<int> indices)
        {
            if (indices.Length == 0)
            {
                return new Utf8StringSeries(source.Name, 0, 0);
            }

            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = 0;

            for (int i = 0; i < indices.Length; i++)
            {
                int idx = indices[i];
                totalBytes += srcOffsets[idx + 1] - srcOffsets[idx];
            }

            var result = new Utf8StringSeries(source.Name, indices.Length, totalBytes);
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

        public static Utf8StringSeries TakeWithNulls(Utf8StringSeries source, ReadOnlySpan<int> indices)
        {
            if (indices.Length == 0) return new Utf8StringSeries(source.Name, 0, 0);

            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = 0;

            for (int i = 0; i < indices.Length; i++)
            {
                int idx = indices[i];
                if (idx != -1) totalBytes += srcOffsets[idx + 1] - srcOffsets[idx];
            }

            var result = new Utf8StringSeries(source.Name, indices.Length, totalBytes);
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

        /// <summary>Optimized ASCII-uppercase: operates directly on UTF-8 bytes.</summary>
        public static unsafe Utf8StringSeries ToUppercase(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = source.DataBytes.Length;

            var result = new Utf8StringSeries(source.Name, rowCount, totalBytes);
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
                        dst[j] = (byte)(b - (b >= 97 && b <= 122 ? 32u : 0u));
                    }
                }
                destOffsets[rowCount] = srcOffsets[rowCount];
            }
            return result;
        }

        /// <summary>Optimized ASCII-lowercase: operates directly on UTF-8 bytes.</summary>
        public static unsafe Utf8StringSeries ToLowercase(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var srcOffsets = source.Offsets.Span;
            var srcData = source.DataBytes.Span;
            int totalBytes = source.DataBytes.Length;

            var result = new Utf8StringSeries(source.Name, rowCount, totalBytes);
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

        /// <summary>Convert each string to title case.</summary>
        public static Utf8StringSeries ToTitlecase(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                var textInfo = System.Globalization.CultureInfo.InvariantCulture.TextInfo;
                strings[i] = textInfo.ToTitleCase(s);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Replace first occurrence of a pattern in each string.</summary>
        public static Utf8StringSeries Replace(Utf8StringSeries source, string oldValue, string newValue)
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
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                int idx = s.IndexOf(oldValue, StringComparison.Ordinal);
                strings[i] = idx >= 0 ? s.Substring(0, idx) + newValue + s.Substring(idx + oldValue.Length) : s;
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Replace all occurrences of a pattern in each string.</summary>
        public static Utf8StringSeries ReplaceAll(Utf8StringSeries source, string oldValue, string newValue)
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
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Replace(oldValue, newValue);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Strip whitespace from both ends of each string.</summary>
        public static Utf8StringSeries Strip(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                strings[i] = Encoding.UTF8.GetString(source.GetStringSpan(i)).Trim();
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Strip whitespace from the start of each string.</summary>
        public static Utf8StringSeries LStrip(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                strings[i] = Encoding.UTF8.GetString(source.GetStringSpan(i)).TrimStart();
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Strip whitespace from the end of each string.</summary>
        public static Utf8StringSeries RStrip(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                strings[i] = Encoding.UTF8.GetString(source.GetStringSpan(i)).TrimEnd();
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Slice each string: start position and optional length.</summary>
        public static Utf8StringSeries Slice(Utf8StringSeries source, int start, int? length = null)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                if (length.HasValue)
                    strings[i] = start >= 0 && start < s.Length ? s.Substring(start, Math.Min(length.Value, s.Length - start)) : string.Empty;
                else
                    strings[i] = start >= 0 && start < s.Length ? s.Substring(start) : string.Empty;
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Extract first n characters from each string.</summary>
        public static Utf8StringSeries Head(Utf8StringSeries source, int n)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length <= n ? s : s.Substring(0, n);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Extract last n characters from each string.</summary>
        public static Utf8StringSeries Tail(Utf8StringSeries source, int n)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length <= n ? s : s.Substring(s.Length - n);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Pad each string on the left to the specified width.</summary>
        public static Utf8StringSeries PadStart(Utf8StringSeries source, int width, char fillChar = ' ')
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length >= width ? s : s.PadLeft(width, fillChar);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Pad each string on the right to the specified width.</summary>
        public static Utf8StringSeries PadEnd(Utf8StringSeries source, int width, char fillChar = ' ')
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                strings[i] = s.Length >= width ? s : s.PadRight(width, fillChar);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }

        /// <summary>Reverse each string.</summary>
        public static Utf8StringSeries Reverse(Utf8StringSeries source)
        {
            int rowCount = source.Length;
            var strings = new string[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                if (source.ValidityMask.IsNull(i)) { strings[i] = null!; continue; }
                var s = Encoding.UTF8.GetString(source.GetStringSpan(i));
                var arr = s.ToCharArray();
                Array.Reverse(arr);
                strings[i] = new string(arr);
            }
            return Utf8StringSeries.FromStrings(source.Name, strings);
        }
    }
}
