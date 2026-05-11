using System;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class BinaryKernels
    {
        public static BooleanSeries StartsWith(BinarySeries series, byte[] prefix)
        {
            var result = new BooleanSeries(series.Name + "_startswith", series.Length);
            var resSpan = result.Memory.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }
                var span = series.GetSpan(i);
                resSpan[i] = span.Length >= prefix.Length && span.Slice(0, prefix.Length).SequenceEqual(prefix);
                result.ValidityMask.SetValid(i);
            }
            return result;
        }

        public static BooleanSeries EndsWith(BinarySeries series, byte[] suffix)
        {
            var result = new BooleanSeries(series.Name + "_endswith", series.Length);
            var resSpan = result.Memory.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }
                var span = series.GetSpan(i);
                resSpan[i] = span.Length >= suffix.Length && span.Slice(span.Length - suffix.Length).SequenceEqual(suffix);
                result.ValidityMask.SetValid(i);
            }
            return result;
        }

        public static BooleanSeries Contains(BinarySeries series, byte[] pattern)
        {
            var result = new BooleanSeries(series.Name + "_contains", series.Length);
            var resSpan = result.Memory.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }
                var span = series.GetSpan(i);
                resSpan[i] = span.IndexOf(pattern) >= 0;
                result.ValidityMask.SetValid(i);
            }
            return result;
        }

        public static Int32Series Lengths(BinarySeries series)
        {
            var result = new Int32Series(series.Name + "_len", series.Length);
            var resSpan = result.Memory.Span;
            var offsets = series.Offsets.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }
                resSpan[i] = offsets[i + 1] - offsets[i];
                result.ValidityMask.SetValid(i);
            }
            return result;
        }

        public static Utf8StringSeries Encode(BinarySeries series, string encoding)
        {
            // For now, only support hex and base64
            var result = new Utf8StringSeries(series.Name + "_encoded", series.Length, series.Length * 16); // heuristic size
            // Note: Real implementation would pre-calculate size. 
            // For now, let's use a simpler approach that might be slower but correct.
            var strings = new string[series.Length];
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i)) continue;
                var bytes = series.GetSpan(i).ToArray();
                if (encoding.Equals("hex", StringComparison.OrdinalIgnoreCase))
                    strings[i] = Convert.ToHexString(bytes).ToLowerInvariant();
                else if (encoding.Equals("base64", StringComparison.OrdinalIgnoreCase))
                    strings[i] = Convert.ToBase64String(bytes);
                else
                    throw new NotSupportedException($"Encoding {encoding} not supported.");
            }
            return new Utf8StringSeries(series.Name + "_encoded", strings);
        }
        /// <summary>Decodes Utf8StringSeries using hex or base64 encoding. For hex, normalizes to uppercase before Convert.FromHexString.</summary>
        public static BinarySeries Decode(Utf8StringSeries series, string encoding)
        {
            var data = new byte[series.Length][];
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i)) continue;
                var s = System.Text.Encoding.UTF8.GetString(series.GetStringSpan(i));
                if (encoding.Equals("hex", StringComparison.OrdinalIgnoreCase))
                {
                    // Convert.FromHexString only accepts uppercase A-F hex chars; normalize input
                    var normalized = s.ToUpperInvariant();
                    data[i] = Convert.FromHexString(normalized);
                }
                else if (encoding.Equals("base64", StringComparison.OrdinalIgnoreCase))
                    data[i] = Convert.FromBase64String(s);
                else
                    throw new NotSupportedException($"Encoding {encoding} not supported.");
            }
            return new BinarySeries(series.Name + "_decoded", data);
        }
        public static BinarySeries EncodeUtf8(Utf8StringSeries series)
        {
            var data = new byte[series.Length][];
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i)) continue;
                data[i] = series.GetStringSpan(i).ToArray();
            }
            return new BinarySeries(series.Name + "_encoded", data);
        }

        public static Utf8StringSeries DecodeUtf8(BinarySeries series)
        {
            var strings = new string[series.Length];
            for (int i = 0; i < series.Length; i++)
            {
                if (series.ValidityMask.IsNull(i)) continue;
                strings[i] = System.Text.Encoding.UTF8.GetString(series.GetSpan(i));
            }
            return new Utf8StringSeries(series.Name + "_decoded", strings);
        }
    }
}
