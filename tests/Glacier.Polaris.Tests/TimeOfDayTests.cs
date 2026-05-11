using System;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class TimeOfDayTests
    {
        private static readonly TimeSpan T1 = new TimeSpan(14, 30, 45); // 14:30:45.000000500
        private static readonly TimeSpan T2 = new TimeSpan(0, 8, 0, 0); // 08:00:00
        private static readonly TimeSpan T3 = new TimeSpan(0, 23, 59, 59); // 23:59:59

        [Fact]
        public void TimeSeries_ConstructFromTimeSpan_StoresNanoseconds()
        {
            var series = new TimeSeries("t", new[] { T1 });
            long expectedNs = T1.Ticks * 100L;
            Assert.Equal(expectedNs, (long)series.Get(0)!);
        }

        [Fact]
        public void TimeSeries_GetHour_ReturnsCorrectValue()
        {
            var series = new TimeSeries("t", new[] { T1, T2, T3 });
            Assert.Equal(14, series.GetHour(0));
            Assert.Equal(8, series.GetHour(1));
            Assert.Equal(23, series.GetHour(2));
        }

        [Fact]
        public void TimeSeries_GetMinute_ReturnsCorrectValue()
        {
            var series = new TimeSeries("t", new[] { T1, T2, T3 });
            Assert.Equal(30, series.GetMinute(0));
            Assert.Equal(0, series.GetMinute(1));
            Assert.Equal(59, series.GetMinute(2));
        }

        [Fact]
        public void TimeSeries_GetSecond_ReturnsCorrectValue()
        {
            var series = new TimeSeries("t", new[] { T1, T2, T3 });
            Assert.Equal(45, series.GetSecond(0));
            Assert.Equal(0, series.GetSecond(1));
            Assert.Equal(59, series.GetSecond(2));
        }

        [Fact]
        public async Task DtHour_OnTimeSeries_ExtractsHour()
        {
            var df = new DataFrame(new ISeries[]
            {
                new TimeSeries("t", new[] { T1, T2, T3 })
            });

            var result = await df.Lazy()
                .Select(Expr.Col("t").Dt().Hour().Alias("hour"))
                .Collect();

            Assert.Equal(14, result.GetColumn("hour").Get(0));
            Assert.Equal(8,  result.GetColumn("hour").Get(1));
            Assert.Equal(23, result.GetColumn("hour").Get(2));
        }

        [Fact]
        public async Task DtMinute_OnTimeSeries_ExtractsMinute()
        {
            var df = new DataFrame(new ISeries[]
            {
                new TimeSeries("t", new[] { T1, T2, T3 })
            });

            var result = await df.Lazy()
                .Select(Expr.Col("t").Dt().Minute().Alias("minute"))
                .Collect();

            Assert.Equal(30, result.GetColumn("minute").Get(0));
            Assert.Equal(0,  result.GetColumn("minute").Get(1));
            Assert.Equal(59, result.GetColumn("minute").Get(2));
        }

        [Fact]
        public async Task DtSecond_OnTimeSeries_ExtractsSecond()
        {
            var df = new DataFrame(new ISeries[]
            {
                new TimeSeries("t", new[] { T1, T2, T3 })
            });

            var result = await df.Lazy()
                .Select(Expr.Col("t").Dt().Second().Alias("second"))
                .Collect();

            Assert.Equal(45, result.GetColumn("second").Get(0));
            Assert.Equal(0,  result.GetColumn("second").Get(1));
            Assert.Equal(59, result.GetColumn("second").Get(2));
        }

        [Fact]
        public async Task DtNanosecond_OnTimeSeries_ExtractsSubSecondNs()
        {
            // 14:30:45 has 0 nanoseconds within the second
            var df = new DataFrame(new ISeries[]
            {
                new TimeSeries("t", new[] { T1 })
            });

            var result = await df.Lazy()
                .Select(Expr.Col("t").Dt().Nanosecond().Alias("ns"))
                .Collect();

            Assert.Equal(0, result.GetColumn("ns").Get(0));

        }

        [Fact]
        public async Task TimeSeries_ArrowRoundTrip_PreservesValues()
        {
            var original = new DataFrame(new ISeries[]
            {
                new TimeSeries("t", new[] { T1, T2, T3 })
            });

            var batch = original.ToArrow();
            var restored = DataFrame.FromArrow(batch);

            var orig = (TimeSeries)original.GetColumn("t");
            var rest = (TimeSeries)restored.GetColumn("t");

            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(orig.GetHour(i), rest.GetHour(i));
                Assert.Equal(orig.GetMinute(i), rest.GetMinute(i));
                Assert.Equal(orig.GetSecond(i), rest.GetSecond(i));
            }
        }

        [Fact]
        public async Task TimeSeries_NullHandling_IsNull()
        {
            var series = new TimeSeries("t", 2);
            series.Memory.Span[0] = T1.Ticks * 100L;
            series.ValidityMask.SetValid(0);
            series.ValidityMask.SetNull(1);

            var df = new DataFrame(new ISeries[] { series });
            var result = await df.Lazy()
                .Select(Expr.Col("t").Dt().Hour().Alias("hour"))
                .Collect();

            Assert.Equal(14, result.GetColumn("hour").Get(0));
            Assert.Null(result.GetColumn("hour").Get(1));
        }
    }
}
