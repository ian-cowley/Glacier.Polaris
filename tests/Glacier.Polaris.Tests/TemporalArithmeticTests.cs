using System;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class TemporalArithmeticTests
    {
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>Convert a DateTime to nanoseconds since 1970-01-01 UTC.</summary>
        private static long ToNanos(DateTime dt)
        {
            return (dt.ToUniversalTime() - Epoch).Ticks * 100;
        }

        [Fact]
        public async Task TestTemporalSubtractionAndDuration()
        {
            // 2026-05-02 10:00:00 UTC
            long dt1Val = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            // 2026-05-01 10:00:00 UTC
            long dt2Val = ToNanos(new DateTime(2026, 5, 1, 10, 0, 0, DateTimeKind.Utc));

            var dt1 = new DatetimeSeries("dt1", 1);
            dt1.Memory.Span[0] = dt1Val;
            var dt2 = new DatetimeSeries("dt2", 1);
            dt2.Memory.Span[0] = dt2Val;
            var df = new DataFrame(new ISeries[] { dt1, dt2 });

            // dt1 - dt2 should be 24 hours (86400 seconds)
            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt1").Dt().Subtract(Expr.Col("dt2")).Alias("diff")
                )
                .WithColumns(
                    Expr.Col("diff").Duration().TotalDays().Alias("days"),
                    Expr.Col("diff").Duration().TotalHours().Alias("hours"),
                    Expr.Col("diff").Duration().TotalSeconds().Alias("seconds")
                )
                .Collect();

            var days = result.GetColumn("days") as Float64Series;
            var hours = result.GetColumn("hours") as Float64Series;
            var seconds = result.GetColumn("seconds") as Float64Series;

            Assert.Equal(1.0, days.Memory.Span[0], 5);
            Assert.Equal(24.0, hours.Memory.Span[0], 5);
            Assert.Equal(86400.0, seconds.Memory.Span[0], 5);
        }

        [Fact]
        public async Task TestSubtractDuration()
        {
            // 2026-05-02 10:00:00 UTC (nanoseconds)
            long dtVal = ToNanos(new DateTime(2026, 5, 2, 10, 0, 0, DateTimeKind.Utc));
            // 1 hour in nanoseconds
            long durVal = 3600L * 1000000000L;

            var dt = new DatetimeSeries("dt", 1);
            dt.Memory.Span[0] = dtVal;
            var dur = new DurationSeries("dur", 1);
            dur.Memory.Span[0] = durVal;
            var df = new DataFrame(new ISeries[] { dt, dur });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("dt").Dt().SubtractDuration(Expr.Col("dur")).Alias("prev_hour")
                )
                .WithColumns(
                    Expr.Col("prev_hour").Dt().Hour().Alias("hour")
                )
                .Collect();

            var hour = result.GetColumn("hour") as Int32Series;
            Assert.Equal(9, hour.Memory.Span[0]);
        }
    }
}
