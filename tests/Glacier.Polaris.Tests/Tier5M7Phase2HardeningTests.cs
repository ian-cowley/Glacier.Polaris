using System;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests;

[CollectionDefinition("StringCache", DisableParallelization = true)]
public sealed class StringCacheCollectionDefinition
{
}

[Collection("StringCache")]
public class Tier5M7Phase2HardeningTests
{
    private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TemporalKernels_Pre1970AndExtremeDatetimeBounds_PreservesOrNullsSafelyWithoutCrash()
    {
        // -1ns, -1s, -1d, 1678-01-01 (fits in Int64 nanos), long.MinValue (1677-09-21), long.MaxValue (2262-04-11), null
        long dt1678Nanos = (new DateTime(1678, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks - Epoch.Ticks) * 100L;
        var dt = DatetimeSeries.FromValues("dt", new long?[]
        {
            -1L,
            -1_000_000_000L,
            -86_400_000_000_000L,
            dt1678Nanos,
            long.MinValue,
            long.MaxValue,
            null
        });

        using var years = TemporalKernels.ExtractYear(dt);
        using var months = TemporalKernels.ExtractMonth(dt);
        using var days = TemporalKernels.ExtractDay(dt);
        using var hours = TemporalKernels.ExtractHour(dt);
        using var mins = TemporalKernels.ExtractMinute(dt);
        using var secs = TemporalKernels.ExtractSecond(dt);
        using var ord = TemporalKernels.ExtractOrdinalDay(dt);
        using var wd = TemporalKernels.ExtractWeekday(dt);
        using var qtr = TemporalKernels.ExtractQuarter(dt);
        using var ms = (DatetimeSeries)TemporalKernels.MonthStart(dt);
        using var me = (DatetimeSeries)TemporalKernels.MonthEnd(dt);
        using var tz = (DatetimeSeries)TemporalKernels.ConvertTimeZone(dt, "UTC", "UTC");

        // -1ns -> 1969-12-31 23:59:59 UTC (pre-1970 must NOT be null)
        Assert.False(years.ValidityMask.IsNull(0));
        Assert.Equal(1969, years.Memory.Span[0]);
        Assert.Equal(12, months.Memory.Span[0]);
        Assert.Equal(31, days.Memory.Span[0]);
        Assert.Equal(23, hours.Memory.Span[0]);
        Assert.Equal(59, mins.Memory.Span[0]);
        Assert.Equal(59, secs.Memory.Span[0]);
        Assert.Equal(365, ord.Memory.Span[0]);
        Assert.Equal(4, qtr.Memory.Span[0]);
        Assert.False(ms.ValidityMask.IsNull(0));
        Assert.False(me.ValidityMask.IsNull(0));
        Assert.False(tz.ValidityMask.IsNull(0));

        // -1d -> 1969-12-31 00:00:00
        Assert.Equal(1969, years.Memory.Span[2]);
        Assert.Equal(12, months.Memory.Span[2]);
        Assert.Equal(31, days.Memory.Span[2]);

        // 1678-01-01 preserved accurately in Int64 nanosecond precision
        Assert.False(years.ValidityMask.IsNull(3));
        Assert.Equal(1678, years.Memory.Span[3]);
        Assert.Equal(1, months.Memory.Span[3]);
        Assert.Equal(1, days.Memory.Span[3]);

        // Extreme Int64 bounds (1677 / 2262) handled safely without throwing, and true null stays null
        Assert.False(years.ValidityMask.IsNull(4));
        Assert.Equal(1677, years.Memory.Span[4]);
        Assert.False(years.ValidityMask.IsNull(5));
        Assert.Equal(2262, years.Memory.Span[5]);
        Assert.True(years.ValidityMask.IsNull(6));
        Assert.True(ms.ValidityMask.IsNull(6));
        Assert.True(me.ValidityMask.IsNull(6));
        Assert.True(tz.ValidityMask.IsNull(6));
    }

    [Fact]
    public void TemporalKernels_DateSeriesOutOfBounds_MonthEnd9999_And_ExtractTimestampOverflow()
    {
        int d1600 = (int)(new DateTime(1600, 1, 1, 0, 0, 0, DateTimeKind.Utc) - Epoch).TotalDays;
        int d2500 = (int)(new DateTime(2500, 1, 1, 0, 0, 0, DateTimeKind.Utc) - Epoch).TotalDays;
        int d9999 = (int)(new DateTime(9999, 12, 15, 0, 0, 0, DateTimeKind.Utc) - Epoch).TotalDays;

        var ds = new DateSeries("d", 7);
        ds.Memory.Span[0] = int.MinValue;
        ds.ValidityMask.SetNull(0);
        ds.Memory.Span[1] = -719_164; // below 0001-01-01
        ds.Memory.Span[2] = 2_932_898; // above 9999-12-31
        ds.Memory.Span[3] = d1600;
        ds.Memory.Span[4] = d2500;
        ds.Memory.Span[5] = d9999;
        ds.Memory.Span[6] = int.MaxValue;

        using var y = TemporalKernels.ExtractYear(ds);
        using var m = TemporalKernels.ExtractMonth(ds);
        using var d = TemporalKernels.ExtractDay(ds);
        using var me = (DateSeries)TemporalKernels.MonthEnd(ds);
        using var tsSec = TemporalKernels.ExtractTimestamp(ds, "s");

        Assert.True(y.ValidityMask.IsNull(0));
        Assert.True(m.ValidityMask.IsNull(0));
        Assert.True(d.ValidityMask.IsNull(0));
        Assert.Equal(1, y.Memory.Span[1]);    // out-of-range < MinValue clamps cleanly to 0001
        Assert.Equal(9999, y.Memory.Span[2]); // out-of-range > MaxValue clamps cleanly to 9999
        Assert.Equal(9999, y.Memory.Span[6]);

        // 9999-12-15 MonthEnd must clamp cleanly to 9999-12-31 without ArgumentOutOfRangeException
        Assert.False(me.ValidityMask.IsNull(5));
        int d9999End = (int)(new DateTime(9999, 12, 31, 0, 0, 0, DateTimeKind.Utc) - Epoch).TotalDays;
        Assert.Equal(d9999End, me.Memory.Span[5]);

        // Pre-1677 & post-2262 ExtractTimestamp("s") must not overflow 64-bit nanos
        Assert.False(tsSec.ValidityMask.IsNull(3));
        Assert.Equal(-11_676_096_000L, tsSec.Memory.Span[3]);
        Assert.False(tsSec.ValidityMask.IsNull(4));
        Assert.Equal((long)(new DateTime(2500, 1, 1, 0, 0, 0, DateTimeKind.Utc) - Epoch).TotalSeconds, tsSec.Memory.Span[4]);
    }

    [Fact]
    public void TemporalKernels_TruncateAndRound_Pre1970FloorAndExtremeBounds()
    {
        var dt = DatetimeSeries.FromValues("dt", new long?[]
        {
            -1L,
            -1_000_000_000L,
            long.MinValue,
            long.MaxValue,
            null
        });

        using var trunc = TemporalKernels.Truncate(dt, "1s");
        using var round = TemporalKernels.Round(dt, "1s");

        // Floor division on -1ns -> -1_000_000_000L (-1s), NOT 0!
        Assert.Equal(-1_000_000_000L, trunc.Memory.Span[0]);
        Assert.Equal(-1_000_000_000L, trunc.Memory.Span[1]);
        // -1ns rounds to 0, -1s rounds to -1s
        Assert.Equal(0L, round.Memory.Span[0]);
        Assert.Equal(-1_000_000_000L, round.Memory.Span[1]);

        Assert.Equal(long.MinValue, trunc.Memory.Span[2]);
        Assert.Equal(long.MinValue, round.Memory.Span[2]);
        Assert.True(trunc.ValidityMask.IsNull(4));
        Assert.True(round.ValidityMask.IsNull(4));
    }

    [Fact]
    public void StringCache_HighContentionConcurrencyAndToggle_RemainsConsistent()
    {
        bool wasEnabled = StringCache.IsEnabled;
        try
        {
            StringCache.Enable();
            int threadCount = 16;
            int keysPerThread = 500;
            var exceptions = new System.Collections.Concurrent.ConcurrentQueue<Exception>();

            Parallel.For(0, threadCount, t =>
            {
                try
                {
                    for (int i = 0; i < keysPerThread; i++)
                    {
                        string key = $"tier5_sc_{t % 4}_{i}";
                        uint code = StringCache.GetOrCreate(key);
                        string? roundTrip = StringCache.GetString(code);
                        if (!string.Equals(roundTrip, key, StringComparison.Ordinal))
                        {
                            throw new InvalidOperationException($"Code {code} resolved to '{roundTrip}' instead of '{key}'.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            });

            Assert.Empty(exceptions);

            string[] sampleStrings = Enumerable.Range(0, 200).Select(i => $"tier5_cat_{i % 25}").ToArray();
            using var cat = CategoricalSeries.FromStrings("cat", sampleStrings);
            Assert.Equal(200, cat.Length);
            Assert.NotNull(StringCache.GetRevMap());

            // Concurrent Disable / Enable lifecycle must not throw IndexOutOfRangeException
            Parallel.Invoke(
                () =>
                {
                    for (int i = 0; i < 200; i++)
                    {
                        StringCache.Disable();
                        StringCache.Enable();
                    }
                },
                () =>
                {
                    for (int i = 0; i < 400; i++)
                    {
                        uint c = StringCache.GetOrCreate($"toggle_key_{i % 30}");
                        _ = StringCache.GetString(c);
                    }
                });
        }
        finally
        {
            StringCache.Disable();
            if (wasEnabled)
            {
                StringCache.Enable();
            }
        }
    }

    [Fact]
    public async Task ExecutionEngine_CastFloat32AndMultiWordValidityBoundaries_WorksCleanly()
    {
        const int count = 150;
        var dbl = new Float64Series("f64", count);
        var strs = new string?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 15 == 0)
            {
                dbl.Memory.Span[i] = double.NaN;
                dbl.ValidityMask.SetNull(i);
                strs[i] = null;
            }
            else if (i == 1)
            {
                dbl.Memory.Span[i] = double.PositiveInfinity;
                strs[i] = "  3.5 ";
            }
            else if (i == 2)
            {
                dbl.Memory.Span[i] = double.NegativeInfinity;
                strs[i] = "not-a-float";
            }
            else if (i == 3)
            {
                dbl.Memory.Span[i] = 1e40; // overflows float -> +Infinity
                strs[i] = "999999999999999999999999999999999999999";
            }
            else
            {
                dbl.Memory.Span[i] = i * 1.25;
                strs[i] = (i * 0.5f).ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
        }

        using var sCol = Utf8StringSeries.FromStrings("s", strs);
        using var df = new DataFrame(new ISeries[] { dbl, sCol });
        using var res = await df.Lazy()
            .Select(
                Expr.Col("f64").Cast(typeof(float)).Alias("f32_from_f64"),
                Expr.Col("s").Cast(typeof(float)).Alias("f32_from_str"),
                Expr.Col("f64").Cast(typeof(double)).Alias("f64_copy"))
            .CollectAsync()
            .FirstAsync();

        var f32FromDbl = Assert.IsType<Float32Series>(res.GetColumn("f32_from_f64"));
        var f32FromStr = Assert.IsType<Float32Series>(res.GetColumn("f32_from_str"));

        Assert.Equal(count, f32FromDbl.Length);
        Assert.True(float.IsPositiveInfinity(f32FromDbl.Memory.Span[1]));
        Assert.True(float.IsNegativeInfinity(f32FromDbl.Memory.Span[2]));
        Assert.True(float.IsPositiveInfinity(f32FromDbl.Memory.Span[3]));

        Assert.Equal(3.5f, f32FromStr.Memory.Span[1]);
        Assert.True(f32FromStr.ValidityMask.IsNull(2)); // unparseable string becomes null safely
        for (int i = 0; i < count; i += 15)
        {
            Assert.True(f32FromDbl.ValidityMask.IsNull(i));
            Assert.True(f32FromStr.ValidityMask.IsNull(i));
        }
    }

    [Fact]
    public async Task TemporalKernels_ExtractNanosecond_ConvertTimeZoneClamped_And_DatetimeStringCastFloorDivision()
    {
        var dt = DatetimeSeries.FromValues("dt", new long?[]
        {
            -1L,
            -1_000_000_000L,
            long.MinValue,
            long.MaxValue,
            null
        });

        using var nanos = TemporalKernels.ExtractNanosecond(dt);
        Assert.False(nanos.ValidityMask.IsNull(0));
        Assert.Equal(999_999_999, nanos.Memory.Span[0]);
        Assert.Equal(0, nanos.Memory.Span[1]);
        Assert.True(nanos.ValidityMask.IsNull(4));

        // Extreme Min/Max DateTime values across non-UTC timezones must not throw ArgumentOutOfRangeException
        long dtMinNanos = long.MinValue;
        long dtMaxNanos = long.MaxValue;
        var dtBounds = DatetimeSeries.FromValues("bounds", new long?[] { dtMinNanos, dtMaxNanos, null });
        using var tzNy = (DatetimeSeries)TemporalKernels.ConvertTimeZone(dtBounds, "America/New_York", "UTC");
        using var tzTokyo = (DatetimeSeries)TemporalKernels.ConvertTimeZone(dtBounds, "Asia/Tokyo", "UTC");
        Assert.False(tzNy.ValidityMask.IsNull(0));
        Assert.False(tzTokyo.ValidityMask.IsNull(1));
        Assert.True(tzNy.ValidityMask.IsNull(2));

        // ExecutionEngine Cast(typeof(string)) on -1ns must use floor division (1969-12-31 23:59:59.9999999), not 1970-01-01
        using var df = new DataFrame(new ISeries[] { dt });
        using var castRes = await df.Lazy()
            .Select(Expr.Col("dt").Cast(typeof(string)).Alias("dt_str"))
            .CollectAsync()
            .FirstAsync();
        var strCol = Assert.IsType<Utf8StringSeries>(castRes.GetColumn("dt_str"));
        Assert.StartsWith("1969-12-31 23:59:59.", strCol.Get(0)?.ToString(), StringComparison.Ordinal);
        Assert.True(strCol.ValidityMask.IsNull(4));
    }
}
