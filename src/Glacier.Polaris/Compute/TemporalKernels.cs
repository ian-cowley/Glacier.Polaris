using System;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Compute
{
    public static class TemporalKernels
    {
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>Convert a DatetimeSeries value (stored as nanoseconds since epoch) to DateTime.</summary>
        private static DateTime NanosToDateTime(long nanos)
        {
            long ticks = nanos / 100; // 100ns per tick
            if (ticks < 0 || ticks > (DateTime.MaxValue.Ticks - Epoch.Ticks))
            {
                // Clamp or fallback for extreme values
                if (nanos < 0) return DateTime.MinValue;
                return DateTime.MaxValue;
            }
            return Epoch.AddTicks(ticks);
        }

        public static Int32Series ExtractYear(ISeries series)
        {
            if (series is DateSeries ds)
            {
                var result = new Int32Series(series.Name + "_year", ds.Length);
                var src = ds.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < ds.Length; i++) dst[i] = Epoch.AddDays(src[i]).Year;
                result.ValidityMask.CopyFrom(ds.ValidityMask);
                return result;
            }
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_year", dts.Length);
                var src = dts.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < dts.Length; i++) dst[i] = NanosToDateTime(src[i]).Year;
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            throw new NotSupportedException("ExtractYear only supported for Date/Datetime.");
        }

        public static Int32Series ExtractMonth(ISeries series)
        {
            if (series is DateSeries ds)
            {
                var result = new Int32Series(series.Name + "_month", ds.Length);
                var src = ds.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < ds.Length; i++) dst[i] = Epoch.AddDays(src[i]).Month;
                result.ValidityMask.CopyFrom(ds.ValidityMask);
                return result;
            }
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_month", dts.Length);
                var src = dts.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < dts.Length; i++) dst[i] = NanosToDateTime(src[i]).Month;
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            throw new NotSupportedException("ExtractMonth only supported for Date/Datetime.");
        }

        public static Int32Series ExtractDay(ISeries series)
        {
            if (series is DateSeries ds)
            {
                var result = new Int32Series(series.Name + "_day", ds.Length);
                var src = ds.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < ds.Length; i++) dst[i] = Epoch.AddDays(src[i]).Day;
                result.ValidityMask.CopyFrom(ds.ValidityMask);
                return result;
            }
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_day", dts.Length);
                var src = dts.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < dts.Length; i++) dst[i] = NanosToDateTime(src[i]).Day;
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            throw new NotSupportedException("ExtractDay only supported for Date/Datetime.");
        }

        public static Int32Series ExtractHour(ISeries series)
        {
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_hour", dts.Length);
                var src = dts.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < dts.Length; i++) dst[i] = NanosToDateTime(src[i]).Hour;
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            if (series is TimeSeries ts)
            {
                var result = new Int32Series(series.Name + "_hour", ts.Length);
                var src = ts.Memory.Span;
                var dst = result.Memory.Span;
                for (int i = 0; i < ts.Length; i++) dst[i] = (int)(src[i] / 3_600_000_000_000L);
                result.ValidityMask.CopyFrom(ts.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static Int32Series ExtractMinute(ISeries series)
        {
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_minute", dts.Length);
                for (int i = 0; i < dts.Length; i++) result.Memory.Span[i] = NanosToDateTime(dts.Memory.Span[i]).Minute;
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            if (series is TimeSeries ts)
            {
                var result = new Int32Series(series.Name + "_minute", ts.Length);
                for (int i = 0; i < ts.Length; i++) result.Memory.Span[i] = (int)((ts.Memory.Span[i] / 60_000_000_000L) % 60);
                result.ValidityMask.CopyFrom(ts.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static Int32Series ExtractSecond(ISeries series)
        {
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_second", dts.Length);
                for (int i = 0; i < dts.Length; i++) result.Memory.Span[i] = NanosToDateTime(dts.Memory.Span[i]).Second;
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            if (series is TimeSeries ts)
            {
                var result = new Int32Series(series.Name + "_second", ts.Length);
                for (int i = 0; i < ts.Length; i++) result.Memory.Span[i] = (int)((ts.Memory.Span[i] / 1_000_000_000L) % 60);
                result.ValidityMask.CopyFrom(ts.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static Int32Series ExtractNanosecond(ISeries series)
        {
            if (series is DatetimeSeries dts)
            {
                var result = new Int32Series(series.Name + "_nanosecond", dts.Length);
                for (int i = 0; i < dts.Length; i++) result.Memory.Span[i] = (int)(dts.Memory.Span[i] % 1_000_000_000L);
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            if (series is TimeSeries ts)
            {
                var result = new Int32Series(series.Name + "_nanosecond", ts.Length);
                for (int i = 0; i < ts.Length; i++) result.Memory.Span[i] = (int)(ts.Memory.Span[i] % 1_000_000_000L);
                result.ValidityMask.CopyFrom(ts.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }


        public static ISeries AddDuration(ISeries temporal, ISeries duration)
        {
            if (temporal is DatetimeSeries dts && duration is DurationSeries ds)
            {
                var result = new DatetimeSeries(temporal.Name, dts.Length);
                for (int i = 0; i < dts.Length; i++) result.Memory.Span[i] = dts.Memory.Span[i] + ds.Memory.Span[i];
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static ISeries SubtractDuration(ISeries temporal, ISeries duration)
        {
            if (temporal is DatetimeSeries dts && duration is DurationSeries ds)
            {
                var result = new DatetimeSeries(temporal.Name, dts.Length);
                for (int i = 0; i < dts.Length; i++) result.Memory.Span[i] = dts.Memory.Span[i] - ds.Memory.Span[i];
                result.ValidityMask.CopyFrom(dts.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static DurationSeries Subtract(ISeries left, ISeries right)
        {
            if (left is DatetimeSeries l && right is DatetimeSeries r)
            {
                var result = new DurationSeries(left.Name + "_diff", l.Length);
                for (int i = 0; i < l.Length; i++) result.Memory.Span[i] = l.Memory.Span[i] - r.Memory.Span[i];
                result.ValidityMask.CopyFrom(l.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static Float64Series ExtractTotalDays(ISeries series)
        {
            if (series is DurationSeries ds)
            {
                var result = new Float64Series(series.Name + "_total_days", ds.Length);
                for (int i = 0; i < ds.Length; i++)
                {
                    // DurationSeries stores nanoseconds; convert to TimeSpan (100ns per tick)
                    var ts = new TimeSpan(ds.Memory.Span[i] / 100);
                    result.Memory.Span[i] = ts.TotalDays;
                }
                result.ValidityMask.CopyFrom(ds.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static Float64Series ExtractTotalHours(ISeries series)
        {
            if (series is DurationSeries ds)
            {
                var result = new Float64Series(series.Name + "_total_hours", ds.Length);
                for (int i = 0; i < ds.Length; i++)
                {
                    var ts = new TimeSpan(ds.Memory.Span[i] / 100);
                    result.Memory.Span[i] = ts.TotalHours;
                }
                result.ValidityMask.CopyFrom(ds.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }

        public static Float64Series ExtractTotalSeconds(ISeries series)
        {
            if (series is DurationSeries ds)
            {
                var result = new Float64Series(series.Name + "_total_seconds", ds.Length);
                for (int i = 0; i < ds.Length; i++)
                {
                    var ts = new TimeSpan(ds.Memory.Span[i] / 100);
                    result.Memory.Span[i] = ts.TotalSeconds;
                }
                result.ValidityMask.CopyFrom(ds.ValidityMask);
                return result;
            }
            throw new NotSupportedException();
        }
        /// <summary>Extracts the weekday (1=Monday, 7=Sunday) from DateSeries or DatetimeSeries.</summary>
        public static Data.Int32Series ExtractWeekday(ISeries series)
        {
            int len = series.Length;
            var result = new Data.Int32Series(series.Name + "_weekday", len);
            if (series is Data.DateSeries dateSeries)
            {
                var src = dateSeries.Memory.Span;
                var res = result.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (dateSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = new DateTime(1970, 1, 1).AddDays(src[i]);
                    res[i] = ((int)dt.DayOfWeek == 0) ? 7 : (int)dt.DayOfWeek;
                }
            }
            else if (series is Data.DatetimeSeries datetimeSeries)
            {
                var src = datetimeSeries.Memory.Span;
                var res = result.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (datetimeSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = new DateTime(1970, 1, 1).AddTicks(src[i] / 100);
                    res[i] = ((int)dt.DayOfWeek == 0) ? 7 : (int)dt.DayOfWeek;
                }
            }
            else
            {
                // Generic fallback for other temporal types (e.g., TimeSeries doesn't have weekday)
                for (int i = 0; i < len; i++) result.ValidityMask.SetNull(i);
            }
            return result;
        }

        /// <summary>Extracts the quarter (1-4) from DateSeries or DatetimeSeries.</summary>
        public static Data.Int32Series ExtractQuarter(ISeries series)
        {
            int len = series.Length;
            var result = new Data.Int32Series(series.Name + "_quarter", len);
            if (series is Data.DateSeries dateSeries)
            {
                var src = dateSeries.Memory.Span;
                var res = result.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (dateSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = new DateTime(1970, 1, 1).AddDays(src[i]);
                    res[i] = (dt.Month - 1) / 3 + 1;
                }
            }
            else if (series is Data.DatetimeSeries datetimeSeries)
            {
                var src = datetimeSeries.Memory.Span;
                var res = result.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (datetimeSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = new DateTime(1970, 1, 1).AddTicks(src[i] / 100);
                    res[i] = (dt.Month - 1) / 3 + 1;
                }
            }
            else
            {
                for (int i = 0; i < len; i++) result.ValidityMask.SetNull(i);
            }
            return result;
        }

        /// <summary>Parses a duration string (e.g., "1d2h30m") and returns the offset in nanoseconds.</summary>
        private static long ParseDurationNs(string duration)
        {
            long totalNs = 0;
            var numBuf = new System.Text.StringBuilder();
            foreach (char c in duration)
            {
                if (char.IsDigit(c)) { numBuf.Append(c); }
                else
                {
                    if (numBuf.Length == 0) continue;
                    long val = long.Parse(numBuf.ToString());
                    numBuf.Clear();
                    totalNs += c switch
                    {
                        'd' or 'D' => val * 24L * 3600L * 1_000_000_000L,
                        'h' or 'H' => val * 3600L * 1_000_000_000L,
                        'm' => val * 60L * 1_000_000_000L,
                        's' or 'S' => val * 1_000_000_000L,
                        'M' => val * 30L * 24L * 3600L * 1_000_000_000L, // month ~30 days
                        'y' or 'Y' => val * 365L * 24L * 3600L * 1_000_000_000L, // year ~365 days
                        _ => 0
                    };
                }
            }
            return totalNs;
        }

        /// <summary>Extracts the Unix epoch from a DatetimeSeries as Int64 nanoseconds, or returns the specified unit.</summary>
        public static Data.Int64Series ExtractEpoch(ISeries series, string unit = "s")
        {
            if (series is not Data.DatetimeSeries dts)
                throw new NotSupportedException("ExtractEpoch requires DatetimeSeries.");
            var src = dts.Memory.Span;
            var result = new Data.Int64Series(series.Name + "_epoch", series.Length);
            var res = result.Memory.Span;
            long divisor = unit switch
            {
                "s" => 1_000_000_000L,
                "ms" => 1_000_000L,
                "us" => 1_000L,
                "ns" => 1L,
                _ => throw new ArgumentException($"Unsupported epoch unit: {unit}. Use s, ms, us, or ns.")
            };
            for (int i = 0; i < series.Length; i++)
            {
                if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                res[i] = src[i] / divisor;
            }
            return result;
        }

        /// <summary>Offsets a DatetimeSeries by a duration string (e.g., "1d2h").</summary>
        public static Data.DatetimeSeries OffsetBy(ISeries series, string duration)
        {
            if (series is not Data.DatetimeSeries dts)
                throw new NotSupportedException("OffsetBy requires DatetimeSeries.");
            long offsetNs = ParseDurationNs(duration);
            var src = dts.Memory.Span;
            var result = new Data.DatetimeSeries(series.Name, series.Length);
            var res = result.Memory.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                res[i] = src[i] + offsetNs;
            }
            return result;
        }

        /// <summary>Rounds a DatetimeSeries to the nearest unit specified by a duration string (e.g., "1h", "1d").</summary>
        public static Data.DatetimeSeries Round(ISeries series, string every)
        {
            if (series is not Data.DatetimeSeries dts)
                throw new NotSupportedException("Round requires DatetimeSeries.");
            long intervalNs = ParseDurationNs(every);
            if (intervalNs <= 0) throw new ArgumentException($"Invalid duration for rounding: {every}");
            var src = dts.Memory.Span;
            var result = new Data.DatetimeSeries(series.Name, series.Length);
            var res = result.Memory.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                // Round to nearest interval
                long halfInterval = intervalNs / 2;
                res[i] = ((src[i] + halfInterval) / intervalNs) * intervalNs;
            }
            return result;
        }
        /// <summary>Truncates a DatetimeSeries to the specified unit (e.g., "1h", "1d"). Always rounds down (like floor division).</summary>
        public static Data.DatetimeSeries Truncate(ISeries series, string every)
        {
            if (series is not Data.DatetimeSeries dts)
                throw new NotSupportedException("Truncate requires DatetimeSeries.");
            long intervalNs = ParseDurationNs(every);
            if (intervalNs <= 0) throw new ArgumentException($"Invalid duration for truncation: {every}");
            var src = dts.Memory.Span;
            var result = new Data.DatetimeSeries(series.Name, series.Length);
            var res = result.Memory.Span;
            for (int i = 0; i < series.Length; i++)
            {
                if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                // Truncate (floor) to interval: always round down
                res[i] = (src[i] / intervalNs) * intervalNs;
            }
            return result;
        }
        /// <summary>Extracts the ordinal day (1..366) from DateSeries or DatetimeSeries.</summary>
        public static Data.Int32Series ExtractOrdinalDay(ISeries series)
        {
            int len = series.Length;
            var result = new Data.Int32Series(series.Name + "_ordinal_day", len);
            if (series is Data.DateSeries dateSeries)
            {
                var src = dateSeries.Memory.Span;
                var res = result.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (dateSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    res[i] = Epoch.AddDays(src[i]).DayOfYear;
                }
            }
            else if (series is Data.DatetimeSeries datetimeSeries)
            {
                var src = datetimeSeries.Memory.Span;
                var res = result.Memory.Span;
                for (int i = 0; i < len; i++)
                {
                    if (datetimeSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    res[i] = NanosToDateTime(src[i]).DayOfYear;
                }
            }
            else
            {
                for (int i = 0; i < len; i++) result.ValidityMask.SetNull(i);
            }
            return result;
        }

        /// <summary>Returns the raw Int64 timestamp of a temporal series in the specified time unit (ns, us, ms, s).</summary>
        public static Data.Int64Series ExtractTimestamp(ISeries series, string unit = "ns")
        {
            long divisor = unit switch
            {
                "ns" => 1L,
                "us" => 1_000L,
                "ms" => 1_000_000L,
                "s" => 1_000_000_000L,
                _ => throw new ArgumentException($"Unsupported timestamp unit: {unit}. Use ns, us, ms, or s.")
            };

            if (series is Data.DatetimeSeries dts)
            {
                var src = dts.Memory.Span;
                var result = new Data.Int64Series(series.Name + "_timestamp", series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    res[i] = src[i] / divisor;
                }
                return result;
            }
            if (series is Data.DateSeries dateSeries)
            {
                var src = dateSeries.Memory.Span;
                var result = new Data.Int64Series(series.Name + "_timestamp", series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (dateSeries.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    long nanos = src[i] * 86_400_000_000_000L;
                    res[i] = nanos / divisor;
                }
                return result;
            }
            throw new NotSupportedException("ExtractTimestamp only supported for Date/Datetime.");
        }

        /// <summary>Returns a new series with the time unit reinterpreted (changes metadata without scaling). Since we always store in ns, this is a pass-through.</summary>
        public static ISeries WithTimeUnit(ISeries series, string unit)
        {
            if (series is Data.DatetimeSeries dts)
            {
                // Our internal storage is always nanoseconds; with_time_unit just changes
                // the logical interpretation, so we return the series as-is.
                return dts;
            }
            throw new NotSupportedException("WithTimeUnit only supported for DatetimeSeries.");
        }

        /// <summary>Returns a new series with the values scaled to the target time unit (ns, us, ms, s).</summary>
        public static ISeries CastTimeUnit(ISeries series, string unit)
        {
            if (series is Data.DatetimeSeries dts)
            {
                long divisor = unit switch
                {
                    "ns" => 1L,
                    "us" => 1_000L,
                    "ms" => 1_000_000L,
                    "s" => 1_000_000_000L,
                    _ => throw new ArgumentException($"Unsupported time unit: {unit}. Use ns, us, ms, or s.")
                };
                var src = dts.Memory.Span;
                var result = new Data.DatetimeSeries(series.Name, series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    res[i] = (src[i] / divisor) * divisor;
                }
                return result;
            }
            throw new NotSupportedException("CastTimeUnit only supported for DatetimeSeries.");
        }

        /// <summary>Rolls dates/times to the first day of the month. Preserves input type (Date or Datetime).</summary>
        public static ISeries MonthStart(ISeries series)
        {
            if (series is Data.DateSeries ds)
            {
                var src = ds.Memory.Span;
                var result = new Data.DateSeries(series.Name, series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (ds.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = Epoch.AddDays(src[i]);
                    var monthStart = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, DateTimeKind.Utc);
                    res[i] = (int)((monthStart - Epoch).TotalDays);
                }
                return result;
            }
            if (series is Data.DatetimeSeries dts)
            {
                var src = dts.Memory.Span;
                var result = new Data.DatetimeSeries(series.Name, series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = NanosToDateTime(src[i]);
                    var monthStart = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, DateTimeKind.Utc);
                    res[i] = (monthStart - Epoch).Ticks * 100;
                }
                return result;
            }
            throw new NotSupportedException("MonthStart only supported for Date/Datetime.");
        }

        /// <summary>Rolls dates/times to the last day of the month. Preserves input type (Date or Datetime).</summary>
        public static ISeries MonthEnd(ISeries series)
        {
            if (series is Data.DateSeries ds)
            {
                var src = ds.Memory.Span;
                var result = new Data.DateSeries(series.Name, series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (ds.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = Epoch.AddDays(src[i]);
                    var monthEnd = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1).AddDays(-1);
                    res[i] = (int)((monthEnd - Epoch).TotalDays);
                }
                return result;
            }
            if (series is Data.DatetimeSeries dts)
            {
                var src = dts.Memory.Span;
                var result = new Data.DatetimeSeries(series.Name, series.Length);
                var res = result.Memory.Span;
                for (int i = 0; i < series.Length; i++)
                {
                    if (dts.ValidityMask.IsNull(i)) { result.ValidityMask.SetNull(i); continue; }
                    var dt = NanosToDateTime(src[i]);
                    var monthEnd = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1).AddDays(-1);
                    // Set to end of day (23:59:59.9999999)
                    monthEnd = monthEnd.AddHours(23).AddMinutes(59).AddSeconds(59).AddTicks(9999999);
                    res[i] = (monthEnd - Epoch).Ticks * 100;
                }
                return result;
            }
            throw new NotSupportedException("MonthEnd only supported for Date/Datetime.");
        }

        /// <summary>Converts the datetime values to a target timezone from a source timezone (defaults to UTC).</summary>
        public static ISeries ConvertTimeZone(ISeries series, string targetTimeZoneId, string sourceTimeZoneId)
        {
            if (series is not Data.DatetimeSeries dts)
                throw new NotSupportedException("ConvertTimeZone only supported for DatetimeSeries.");

            TimeZoneInfo sourceTz = TimeZoneInfo.FindSystemTimeZoneById(sourceTimeZoneId);
            TimeZoneInfo targetTz = TimeZoneInfo.FindSystemTimeZoneById(targetTimeZoneId);

            var src = dts.Memory.Span;
            var result = new Data.DatetimeSeries(series.Name, series.Length);
            var res = result.Memory.Span;

            for (int i = 0; i < series.Length; i++)
            {
                if (dts.ValidityMask.IsNull(i))
                {
                    result.ValidityMask.SetNull(i);
                    continue;
                }

                DateTime dt = NanosToDateTime(src[i]);
                DateTime unspecifiedDt = DateTime.SpecifyKind(dt, DateTimeKind.Unspecified);
                DateTime targetDt = TimeZoneInfo.ConvertTime(unspecifiedDt, sourceTz, targetTz);
                DateTime targetDtUtc = DateTime.SpecifyKind(targetDt, DateTimeKind.Utc);
                long ticks = (targetDtUtc - Epoch).Ticks;
                res[i] = ticks * 100;
            }
            result.ValidityMask.CopyFrom(dts.ValidityMask);
            return result;
        }
    }
}
