using System;
using System.Globalization;

namespace Glacier.Polaris
{
    /// <summary>
    /// Parses Polars duration strings (e.g., "1h30m", "2d", "500ms") into nanoseconds.
    /// Used by GroupByDynamic and GroupByRolling builders.
    /// </summary>
    public static class Duration
    {
        public static double Parse(string durationStr, Type indexType)
        {
            if (string.IsNullOrEmpty(durationStr)) return 0;

            ReadOnlySpan<char> span = durationStr.AsSpan().Trim();
            if (span.IsEmpty)
            {
                throw new ArgumentException($"Invalid Polars duration format: '{durationStr}'", nameof(durationStr));
            }

            // Try to parse as direct numeric first (invariant culture, finite only)
            const NumberStyles numStyles = NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint | NumberStyles.AllowExponent;
            if (double.TryParse(span, numStyles, CultureInfo.InvariantCulture, out double numericVal))
            {
                if (!double.IsFinite(numericVal))
                {
                    throw new ArgumentException($"Invalid non-finite duration value: '{durationStr}'", nameof(durationStr));
                }
                return numericVal;
            }

            double sign = 1.0;
            if (span[0] == '+')
            {
                span = span.Slice(1);
            }
            else if (span[0] == '-')
            {
                sign = -1.0;
                span = span.Slice(1);
            }

            span = span.TrimStart();
            if (span.IsEmpty)
            {
                throw new ArgumentException($"Invalid Polars duration format: '{durationStr}'", nameof(durationStr));
            }

            double total = 0.0;
            int pos = 0;

            while (pos < span.Length)
            {
                while (pos < span.Length && char.IsWhiteSpace(span[pos]))
                {
                    pos++;
                }
                if (pos >= span.Length) break;

                int numStart = pos;
                int intDigits = 0;
                while (pos < span.Length && char.IsAsciiDigit(span[pos]))
                {
                    intDigits++;
                    pos++;
                }

                int fracDigits = 0;
                if (pos < span.Length && span[pos] == '.')
                {
                    pos++;
                    while (pos < span.Length && char.IsAsciiDigit(span[pos]))
                    {
                        fracDigits++;
                        pos++;
                    }
                    if (fracDigits == 0)
                    {
                        throw new ArgumentException($"Invalid Polars duration format: '{durationStr}'", nameof(durationStr));
                    }
                }

                if (intDigits == 0 && fracDigits == 0)
                {
                    throw new ArgumentException($"Invalid Polars duration format: '{durationStr}'", nameof(durationStr));
                }

                // Optional scientific exponent only when followed by optional sign + digits (so "1ms" or "1em" doesn't match 'e')
                if (pos < span.Length && (span[pos] == 'e' || span[pos] == 'E'))
                {
                    int expPos = pos + 1;
                    if (expPos < span.Length && (span[expPos] == '+' || span[expPos] == '-'))
                    {
                        expPos++;
                    }
                    int expDigits = 0;
                    while (expPos < span.Length && char.IsAsciiDigit(span[expPos]))
                    {
                        expDigits++;
                        expPos++;
                    }
                    if (expDigits > 0)
                    {
                        pos = expPos;
                    }
                }

                ReadOnlySpan<char> numSlice = span.Slice(numStart, pos - numStart);
                if (!double.TryParse(numSlice, NumberStyles.AllowDecimalPoint | NumberStyles.AllowExponent, CultureInfo.InvariantCulture, out double value) ||
                    !double.IsFinite(value))
                {
                    throw new OverflowException($"Duration '{durationStr}' overflows double range.");
                }

                while (pos < span.Length && char.IsWhiteSpace(span[pos]))
                {
                    pos++;
                }

                int unitStart = pos;
                while (pos < span.Length && (char.IsAsciiLetter(span[pos]) || span[pos] == '\u00B5' || span[pos] == '\u03BC'))
                {
                    pos++;
                }

                if (pos == unitStart)
                {
                    throw new ArgumentException($"Invalid Polars duration format (missing unit): '{durationStr}'", nameof(durationStr));
                }

                string unit = span.Slice(unitStart, pos - unitStart).ToString().ToLowerInvariant();
                double scale = unit switch
                {
                    "ns" => 1.0,
                    "us" or "\u00B5s" or "\u03BCs" => 1_000.0,
                    "ms" => 1_000_000.0,
                    "s" => 1_000_000_000.0,
                    "m" => 60.0 * 1_000_000_000.0,
                    "h" => 3_600.0 * 1_000_000_000.0,
                    "d" => 86_400.0 * 1_000_000_000.0,
                    "w" => 7.0 * 86_400.0 * 1_000_000_000.0,
                    "mo" => 30.0 * 86_400.0 * 1_000_000_000.0,
                    "y" => 365.0 * 86_400.0 * 1_000_000_000.0,
                    _ => throw new ArgumentException($"Unsupported duration unit: '{unit}' in '{durationStr}'", nameof(durationStr))
                };

                double term = value * scale;
                total += term;
                if (!double.IsFinite(term) || !double.IsFinite(total))
                {
                    throw new OverflowException($"Duration '{durationStr}' overflows double range.");
                }
            }

            return sign * total;
        }
    }
}
