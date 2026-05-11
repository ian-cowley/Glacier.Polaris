using System;
using System.Text.RegularExpressions;

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

            durationStr = durationStr.Trim();

            // Try to parse as direct numeric first
            if (double.TryParse(durationStr, out double numericVal))
            {
                return numericVal;
            }

            // Regex to parse prefix number and suffix unit
            var match = Regex.Match(durationStr, @"^([+-]?\d+(?:\.\d+)?)\s*([a-zA-Z]+)$");
            if (!match.Success)
            {
                throw new ArgumentException($"Invalid Polars duration format: '{durationStr}'");
            }

            double value = double.Parse(match.Groups[1].Value);
            string unit = match.Groups[2].Value.ToLower();

            switch (unit)
            {
                case "ns":
                    return value;
                case "us":
                    return value * 1000.0;
                case "ms":
                    return value * 1_000_000.0;
                case "s":
                    return value * 1_000_000_000.0;
                case "m":
                    return value * 60.0 * 1_000_000_000.0;
                case "h":
                    return value * 3600.0 * 1_000_000_000.0;
                case "d":
                    return value * 86400.0 * 1_000_000_000.0;
                case "w":
                    return value * 7.0 * 86400.0 * 1_000_000_000.0;
                case "mo":
                    return value * 30.0 * 86400.0 * 1_000_000_000.0;
                case "y":
                    return value * 365.0 * 86400.0 * 1_000_000_000.0;
                default:
                    throw new ArgumentException($"Unsupported duration unit: '{unit}' in '{durationStr}'");
            }
        }
    }
}
