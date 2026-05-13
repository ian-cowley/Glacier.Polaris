using System;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class TemporalTests
    {
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private static long ToNanos(DateTime dt) =>
            (dt.ToUniversalTime() - Epoch).Ticks * 100;

        #region Extract: Year / Month / Day

        [Fact]
        public async Task TestExtractYearMonthDay_DateSeries()
        {
            // DateSeries stores days since 1970-01-01
            var date = new DateSeries("date", 3);
            date.Memory.Span[0] = 0;       // 1970-01-01
            date.Memory.Span[1] = 365;     // 1971-01-01
            date.Memory.Span[2] = 365 * 5; // ~1975-01-01
            date.ValidityMask.SetNull(2);  // null for row 2

            var df = new DataFrame(new ISeries[] { date });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("date").Year().Alias("y"),
                    Expr.Col("date").Month().Alias("m"),
                    Expr.Col("date").Day().Alias("d")
                )
                .Collect();

            var y = result.GetColumn("y") as Int32Series;
            var m = result.GetColumn("m") as Int32Series;
            var d = result.GetColumn("d") as Int32Series;

            Assert.Equal(1970, y.Memory.Span[0]);
            Assert.Equal(1, m.Memory.Span[0]);
            Assert.Equal(1, d.Memory.Span[0]);

            Assert.Equal(1971, y.Memory.Span[1]);
            Assert.Equal(1, m.Memory.Span[1]);
            Assert.Equal(1, d.Memory.Span[1]);

            Assert.True(y.ValidityMask.IsNull(2));
            Assert.True(m.ValidityMask.IsNull(2));
            Assert.True(d.ValidityMask.IsNull(2));
        }

        [Fact]
        public async Task TestExtractYearMonthDay_DatetimeSeries()
        {
            // 2026-05-02 10:30:45 UTC
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 30, 45, DateTimeKind.Utc));
            var dt = new DatetimeSeries("dt", 1);
            dt.Memory.Span[0] = dtVal;

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Year().Alias("y"),
                    Expr.Col("dt").Month().Alias("m"),
                    Expr.Col("dt").Day().Alias("d")
                )
                .Collect();

            Assert.Equal(2026, (result.GetColumn("y") as Int32Series).Memory.Span[0]);
            Assert.Equal(5, (result.GetColumn("m") as Int32Series).Memory.Span[0]);
            Assert.Equal(2, (result.GetColumn("d") as Int32Series).Memory.Span[0]);
        }

        #endregion

        #region Extract: Hour / Minute / Second

        [Fact]
        public async Task TestExtractHourMinuteSecond_DatetimeSeries()
        {
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 14, 35, 20, DateTimeKind.Utc));
            var dt = new DatetimeSeries("dt", 1);
            dt.Memory.Span[0] = dtVal;

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().Hour().Alias("h"),
                    Expr.Col("dt").Dt().Minute().Alias("min"),
                    Expr.Col("dt").Dt().Second().Alias("s")
                )
                .Collect();

            Assert.Equal(14, (result.GetColumn("h") as Int32Series).Memory.Span[0]);
            Assert.Equal(35, (result.GetColumn("min") as Int32Series).Memory.Span[0]);
            Assert.Equal(20, (result.GetColumn("s") as Int32Series).Memory.Span[0]);
        }

        [Fact]
        public async Task TestExtractHourMinuteSecond_TimeSeries()
        {
            // TimeSeries stores nanoseconds since midnight
            // 14:35:20 = 14*3600*1e9 + 35*60*1e9 + 20*1e9
            long nanos = 14L * 3600 * 1_000_000_000L +
                          35L * 60 * 1_000_000_000L +
                          20L * 1_000_000_000L;
            var ts = new TimeSeries("time", 1);
            ts.Memory.Span[0] = nanos;

            var df = new DataFrame(new ISeries[] { ts });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("time").Dt().Hour().Alias("h"),
                    Expr.Col("time").Dt().Minute().Alias("min"),
                    Expr.Col("time").Dt().Second().Alias("s")
                )
                .Collect();

            Assert.Equal(14, (result.GetColumn("h") as Int32Series).Memory.Span[0]);
            Assert.Equal(35, (result.GetColumn("min") as Int32Series).Memory.Span[0]);
            Assert.Equal(20, (result.GetColumn("s") as Int32Series).Memory.Span[0]);
        }

        #endregion

        #region Extract: Nanosecond

        [Fact]
        public async Task TestExtractNanosecond_DatetimeSeries()
        {
            // 123456789 nanoseconds are just the fractional second part
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc)) + 123_456_789L;
            var dt = new DatetimeSeries("dt", 1);
            dt.Memory.Span[0] = dtVal;

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(Expr.Col("dt").Dt().Nanosecond().Alias("ns"))
                .Collect();

            Assert.Equal(123_456_789, (result.GetColumn("ns") as Int32Series).Memory.Span[0]);
        }

        [Fact]
        public async Task TestExtractNanosecond_TimeSeries()
        {
            var ts = new TimeSeries("time", 2);
            ts.Memory.Span[0] = 987_654_321L;    // 987654321 ns since midnight
            ts.Memory.Span[1] = 0L;
            ts.ValidityMask.SetNull(1);

            var df = new DataFrame(new ISeries[] { ts });

            var result = await df.Lazy()
                .Select(Expr.Col("time").Dt().Nanosecond().Alias("ns"))
                .Collect();

            var ns = result.GetColumn("ns") as Int32Series;
            Assert.Equal(987_654_321, ns.Memory.Span[0]);
            Assert.True(ns.ValidityMask.IsNull(1));
        }

        #endregion

        #region Extract: Weekday / Quarter

        [Fact]
        public async Task TestExtractWeekday()
        {
            // 1970-01-01 was a Thursday (DayOfWeek=4 -> Polars weekday=4)
            var date = new DateSeries("date", 2);
            date.Memory.Span[0] = 0;    // 1970-01-01 Thursday
            date.Memory.Span[1] = 2;    // 1970-01-03 Saturday

            var dt = new DatetimeSeries("dt", 2);
            dt.Memory.Span[0] = 0; // 1970-01-01 00:00:00 UTC Thursday
            dt.Memory.Span[1] = ToNanos(new DateTime(2026, 5, 3, 0, 0, 0, DateTimeKind.Utc)); // Sunday
            dt.ValidityMask.SetNull(1);

            var df = new DataFrame(new ISeries[] { date, dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("date").Dt().Weekday().Alias("dow"),
                    Expr.Col("dt").Dt().Weekday().Alias("dow_dt")
                )
                .Collect();

            var dow = result.GetColumn("dow") as Int32Series;
            // Polars: 1=Monday..7=Sunday; Thu=4, Sat=6
            Assert.Equal(4, dow.Memory.Span[0]); // 1970-01-01 Thursday
            Assert.Equal(6, dow.Memory.Span[1]); // 1970-01-03 Saturday

            var dowDt = result.GetColumn("dow_dt") as Int32Series;
            Assert.Equal(4, dowDt.Memory.Span[0]); // 1970-01-01 Thursday
            Assert.True(dowDt.ValidityMask.IsNull(1)); // null propagated
        }

        [Fact]
        public async Task TestExtractQuarter()
        {
            var date = new DateSeries("date", 4);
            date.Memory.Span[0] = 0;          // 1970-01-01 Q1
            date.Memory.Span[1] = 90;         // 1970-04-01 Q2
            date.Memory.Span[2] = 181;        // 1970-07-01 Q3
            date.Memory.Span[3] = 273;        // 1970-10-01 Q4

            var df = new DataFrame(new ISeries[] { date });

            var result = await df.Lazy()
                .Select(Expr.Col("date").Dt().Quarter().Alias("q"))
                .Collect();

            var q = result.GetColumn("q") as Int32Series;
            Assert.Equal(1, q.Memory.Span[0]);
            Assert.Equal(2, q.Memory.Span[1]);
            Assert.Equal(3, q.Memory.Span[2]);
            Assert.Equal(4, q.Memory.Span[3]);
        }

        #endregion

        #region OffsetBy / Round / Epoch

        [Fact]
        public async Task TestOffsetBy()
        {
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            var dt = new DatetimeSeries("dt", 2);
            dt.Memory.Span[0] = dtVal;
            dt.Memory.Span[1] = dtVal;
            dt.ValidityMask.SetNull(1); // null propagation

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().OffsetBy("1d").Alias("plus1d"),
                    Expr.Col("dt").Dt().OffsetBy("2h30m").Alias("plus2h30m")
                )
                .Collect();

            // plus1d
            var plus1d = result.GetColumn("plus1d") as DatetimeSeries;
            var expected1d = new DateTime(2026, 5, 3, 10, 0, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expected1d), plus1d.Memory.Span[0]);
            Assert.True(plus1d.ValidityMask.IsNull(1));

            // plus2h30m
            var plus2h30m = result.GetColumn("plus2h30m") as DatetimeSeries;
            var expected2h30m = new DateTime(2026, 5, 2, 12, 30, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expected2h30m), plus2h30m.Memory.Span[0]);
        }

        [Fact]
        public async Task TestRound()
        {
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 17, 0, DateTimeKind.Utc));
            var dt = new DatetimeSeries("dt", 2);
            dt.Memory.Span[0] = dtVal;
            dt.Memory.Span[1] = dtVal;

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().Round("1h").Alias("round_hour"),
                    Expr.Col("dt").Dt().Round("1d").Alias("round_day")
                )
                .Collect();

            // Round to nearest hour: 10:17 -> 10:00
            var roundHour = result.GetColumn("round_hour") as DatetimeSeries;
            var expectedHour = new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expectedHour), roundHour.Memory.Span[0]);

            // Round to nearest day: 10:17 -> 1970-01-01 ... actually 2026-05-02 -> 2026-05-02 (before noon)
            var roundDay = result.GetColumn("round_day") as DatetimeSeries;
            var expectedDay = new DateTime(2026, 5, 2, 0, 0, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expectedDay), roundDay.Memory.Span[0]);
        }

        [Fact]
        public async Task TestEpoch()
        {
            // 2026-05-02 10:00:00 UTC
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            var dt = new DatetimeSeries("dt", 2);
            dt.Memory.Span[0] = dtVal;
            dt.ValidityMask.SetNull(1); // null

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().Epoch("s").Alias("epoch_s"),
                    Expr.Col("dt").Dt().Epoch("ms").Alias("epoch_ms"),
                    Expr.Col("dt").Dt().Epoch("ns").Alias("epoch_ns")
                )
                .Collect();

            long expectedSeconds = dtVal / 1_000_000_000L;
            Assert.Equal(expectedSeconds, (result.GetColumn("epoch_s") as Int64Series).Memory.Span[0]);

            long expectedMs = dtVal / 1_000_000L;
            Assert.Equal(expectedMs, (result.GetColumn("epoch_ms") as Int64Series).Memory.Span[0]);

            Assert.Equal(dtVal, (result.GetColumn("epoch_ns") as Int64Series).Memory.Span[0]);

            // Null propagation
            Assert.True((result.GetColumn("epoch_s") as Int64Series).ValidityMask.IsNull(1));
        }

        #endregion

        #region AddDuration / SubtractDuration / Subtract

        [Fact]
        public async Task TestAddDuration()
        {
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            long durVal = 3600L * 1_000_000_000L; // 1 hour

            var dt = new DatetimeSeries("dt", 1);
            dt.Memory.Span[0] = dtVal;
            var dur = new DurationSeries("dur", 1);
            dur.Memory.Span[0] = durVal;

            var df = new DataFrame(new ISeries[] { dt, dur });

            // Use LazyFrame binary expression: dt + dur
            // The execution engine handles DatetimeSeries + DurationSeries -> AddDuration
            var result = await df.Lazy()
                .Select(
                    (Expr.Col("dt") + Expr.Col("dur")).Alias("plus1h")
                )
                .Collect();

            var plus1h = result.GetColumn("plus1h") as DatetimeSeries;
            var expected = new DateTime(2026, 5, 2, 11, 0, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expected), plus1h.Memory.Span[0]);
        }

        [Fact]
        public async Task TestSubtractDuration()
        {
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            long durVal = 2 * 3600L * 1_000_000_000L; // 2 hours

            var dt = new DatetimeSeries("dt", 2);
            dt.Memory.Span[0] = dtVal;
            dt.Memory.Span[1] = dtVal;
            dt.ValidityMask.SetNull(1);
            var dur = new DurationSeries("dur", 2);
            dur.Memory.Span[0] = durVal;
            dur.Memory.Span[1] = durVal;

            var df = new DataFrame(new ISeries[] { dt, dur });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().SubtractDuration(Expr.Col("dur")).Alias("minus2h")
                )
                .WithColumns(
                    Expr.Col("minus2h").Dt().Hour().Alias("hour")
                )
                .Collect();

            var minus2h = result.GetColumn("minus2h") as DatetimeSeries;
            var expected = new DateTime(2026, 5, 2, 8, 0, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expected), minus2h.Memory.Span[0]);
            Assert.True(minus2h.ValidityMask.IsNull(1));

            var hour = result.GetColumn("hour") as Int32Series;
            Assert.Equal(8, hour.Memory.Span[0]);
        }

        [Fact]
        public async Task TestSubtractDatetime()
        {
            long dt1Val = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            long dt2Val = ToNanos(new DateTime(2026, 5, 1, 10, 0, 0, DateTimeKind.Utc));

            var dt1 = new DatetimeSeries("dt1", 1);
            dt1.Memory.Span[0] = dt1Val;
            var dt2 = new DatetimeSeries("dt2", 1);
            dt2.Memory.Span[0] = dt2Val;

            var df = new DataFrame(new ISeries[] { dt1, dt2 });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt1").Dt().Subtract(Expr.Col("dt2")).Alias("diff")
                )
                .WithColumns(
                    Expr.Col("diff").TotalDays().Alias("days"),
                    Expr.Col("diff").TotalHours().Alias("hours"),
                    Expr.Col("diff").TotalSeconds().Alias("seconds")
                )
                .Collect();

            var days = result.GetColumn("days") as Float64Series;
            var hours = result.GetColumn("hours") as Float64Series;
            var seconds = result.GetColumn("seconds") as Float64Series;

            Assert.Equal(1.0, days.Memory.Span[0], 5);
            Assert.Equal(24.0, hours.Memory.Span[0], 5);
            Assert.Equal(86400.0, seconds.Memory.Span[0], 5);
        }

        #endregion

        #region Duration Accessors

        [Fact]
        public async Task TestDurationAccessors()
        {
            // 1 hour 30 minutes in nanoseconds
            long durVal = 90L * 60 * 1_000_000_000L;
            var dur = new DurationSeries("dur", 2);
            dur.Memory.Span[0] = durVal;
            dur.ValidityMask.SetNull(1); // null

            var df = new DataFrame(new ISeries[] { dur });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dur").TotalDays().Alias("days"),
                    Expr.Col("dur").TotalHours().Alias("hours"),
                    Expr.Col("dur").TotalSeconds().Alias("seconds")
                )
                .Collect();

            var days = result.GetColumn("days") as Float64Series;
            var hours = result.GetColumn("hours") as Float64Series;
            var seconds = result.GetColumn("seconds") as Float64Series;

            Assert.Equal(1.5 / 24, days.Memory.Span[0], 5);
            Assert.Equal(1.5, hours.Memory.Span[0], 5);
            Assert.Equal(5400.0, seconds.Memory.Span[0], 5);

            Assert.True(days.ValidityMask.IsNull(1));
            Assert.True(hours.ValidityMask.IsNull(1));
            Assert.True(seconds.ValidityMask.IsNull(1));
        }

        [Fact]
        public async Task TestDurationOp()
        {
            var dur = new DurationSeries("dur", 1);
            dur.Memory.Span[0] = 3600L * 1_000_000_000L; // 1 hour

            var df = new DataFrame(new ISeries[] { dur });

            // DurationOp is essentially a no-op identity for DurationSeries
            var result = await df.Lazy()
                .Select(Expr.Col("dur").Duration().Alias("identity"))
                .Collect();

            var identity = result.GetColumn("identity") as DurationSeries;
            Assert.Equal(3600L * 1_000_000_000L, identity.Memory.Span[0]);
        }

        #endregion

        #region Null Propagation (all temporal ops)

        [Fact]
        public async Task TestTemporalNullPropagation()
        {
            var dt = new DatetimeSeries("dt", 3);
            dt.Memory.Span[0] = ToNanos(new DateTime(2026, 5, 2, 10, 30, 45, DateTimeKind.Utc));
            dt.ValidityMask.SetNull(1); // null row 1
            dt.Memory.Span[2] = ToNanos(new DateTime(2026, 5, 3, 12, 0, 0, DateTimeKind.Utc));

            var dur = new DurationSeries("dur", 3);
            dur.Memory.Span[0] = 3600L * 1_000_000_000L;
            dur.Memory.Span[2] = 7200L * 1_000_000_000L;

            var df = new DataFrame(new ISeries[] { dt, dur });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().Year().Alias("year"),
                    Expr.Col("dt").Dt().Hour().Alias("hour"),
                    Expr.Col("dt").Dt().OffsetBy("1d").Alias("offset"),
                    Expr.Col("dt").Dt().Epoch("s").Alias("epoch"),
                    (Expr.Col("dt") + Expr.Col("dur")).Alias("add_dur")
                )
                .Collect();

            // Row 0: all valid
            Assert.True(result.GetColumn("year").ValidityMask.IsValid(0));
            Assert.True(result.GetColumn("hour").ValidityMask.IsValid(0));
            Assert.True(result.GetColumn("offset").ValidityMask.IsValid(0));
            Assert.True(result.GetColumn("epoch").ValidityMask.IsValid(0));
            Assert.True(result.GetColumn("add_dur").ValidityMask.IsValid(0));

            // Row 1: all null (null DatetimeSeries input)
            Assert.True(result.GetColumn("year").ValidityMask.IsNull(1));
            Assert.True(result.GetColumn("hour").ValidityMask.IsNull(1));
            Assert.True(result.GetColumn("offset").ValidityMask.IsNull(1));
            Assert.True(result.GetColumn("epoch").ValidityMask.IsNull(1));

            // Row 2: all valid
            Assert.True(result.GetColumn("year").ValidityMask.IsValid(2));
            Assert.True(result.GetColumn("hour").ValidityMask.IsValid(2));
            Assert.True(result.GetColumn("offset").ValidityMask.IsValid(2));
            Assert.True(result.GetColumn("epoch").ValidityMask.IsValid(2));
        }

        #endregion

        #region Timezone Localization and Conversion

        [Fact]
        public async Task TestConvertTimeZone_UTCtoEastern()
        {
            // 2026-05-13T12:00:00 UTC (which is DST on, so EDT is UTC-4 -> 08:00:00 Eastern)
            DateTime utcDateTime = new DateTime(2026, 5, 13, 12, 0, 0, DateTimeKind.Utc);
            long dtVal = ToNanos(utcDateTime);
            
            var dt = new DatetimeSeries("dt", 2);
            dt.Memory.Span[0] = dtVal;
            dt.ValidityMask.SetNull(1); // Null value to verify null propagation

            var df = new DataFrame(new ISeries[] { dt });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().ConvertTimeZone("Eastern Standard Time", "UTC").Alias("est"),
                    Expr.Col("dt").Dt().ConvertTimeZone("Eastern Standard Time", "UTC").Dt().Hour().Alias("hour")
                )
                .Collect();

            var est = result.GetColumn("est") as DatetimeSeries;
            var hour = result.GetColumn("hour") as Int32Series;

            // Hour should be converted to Eastern Daylight Time (EDT, which is UTC-4 in May -> 8)
            Assert.Equal(8, hour.Memory.Span[0]);
            Assert.True(hour.ValidityMask.IsNull(1));

            // Validate est nanosecond representation
            DateTime expectedLocalEst = new DateTime(2026, 5, 13, 8, 0, 0, DateTimeKind.Utc);
            Assert.Equal(ToNanos(expectedLocalEst), est.Memory.Span[0]);
            Assert.True(est.ValidityMask.IsNull(1));
        }

        #endregion
    }
}
