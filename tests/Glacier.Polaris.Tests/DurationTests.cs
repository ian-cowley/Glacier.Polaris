using System;
using System.Collections.Generic;
using System.Globalization;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests;

public class DurationTests
{
    private const double NsPerUs = 1_000.0;
    private const double NsPerMs = 1_000_000.0;
    private const double NsPerSec = 1_000_000_000.0;
    private const double NsPerMin = 60.0 * NsPerSec;
    private const double NsPerHour = 3_600.0 * NsPerSec;
    private const double NsPerDay = 86_400.0 * NsPerSec;

    [Theory]
    [InlineData("fr-FR")]
    [InlineData("de-DE")]
    [InlineData("tr-TR")]
    public void Duration_Parse_UsesInvariantCultureAcrossLocales(string cultureName)
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(cultureName);

            Assert.Equal(1.5, Duration.Parse("1.5", typeof(long)));
            Assert.Equal(1.5 * NsPerHour, Duration.Parse("1.5h", typeof(long)));
            Assert.Equal(2.25 * NsPerMin, Duration.Parse("2.25m", typeof(long)));
            Assert.Equal(1.5 * NsPerHour + 30.0 * NsPerMin, Duration.Parse("1.5h30m", typeof(long)));
            Assert.Equal(10.0 * NsPerHour, Duration.Parse("10H", typeof(long)));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Theory]
    [InlineData("", 0.0)]
    [InlineData("1h30m", 1.5 * NsPerHour)]
    [InlineData("1d2h30m15s", NsPerDay + 2.0 * NsPerHour + 30.0 * NsPerMin + 15.0 * NsPerSec)]
    [InlineData("  2h  15m  ", 2.0 * NsPerHour + 15.0 * NsPerMin)]
    [InlineData("500ms250us", 500.0 * NsPerMs + 250.0 * NsPerUs)]
    [InlineData("-1h30m", -(1.5 * NsPerHour))]
    [InlineData("-500ms", -500.0 * NsPerMs)]
    [InlineData("+2h30m", 2.5 * NsPerHour)]
    [InlineData("1H30M", 1.5 * NsPerHour)]
    [InlineData("42ns", 42.0)]
    [InlineData("500\u00B5s", 500.0 * NsPerUs)]
    [InlineData("500\u03BCs", 500.0 * NsPerUs)]
    [InlineData("2w", 14.0 * NsPerDay)]
    [InlineData("1mo", 30.0 * NsPerDay)]
    [InlineData("1y", 365.0 * NsPerDay)]
    public void Duration_Parse_CompoundAndAllUnits_ReturnsExpectedNanoseconds(string input, double expectedNs)
    {
        Assert.Equal(expectedNs, Duration.Parse(input, typeof(long)));
    }

    [Theory]
    [InlineData("   ")]
    [InlineData("NaN")]
    [InlineData("Infinity")]
    [InlineData("-Infinity")]
    [InlineData("1h30")]
    [InlineData("1h-30m")]
    [InlineData("1x")]
    [InlineData("--1h")]
    [InlineData("1.h")]
    [InlineData("+")]
    [InlineData("-")]
    public void Duration_Parse_InvalidInputs_ThrowsArgumentException(string input)
    {
        Assert.Throws<ArgumentException>(() => Duration.Parse(input, typeof(long)));
    }

    [Theory]
    [InlineData("1e305y")]
    [InlineData("1e305s")]
    [InlineData("1e309s")]
    public void Duration_Parse_Overflow_ThrowsOverflowException(string input)
    {
        Assert.Throws<OverflowException>(() => Duration.Parse(input, typeof(long)));
    }

    [Fact]
    public void GroupByDynamicAndRolling_WithCompoundDuration_WorksEndToEnd()
    {
        var ts = DatetimeSeries.FromValues("ts", new long?[]
        {
            0L,
            (long)(60 * NsPerMin),
            (long)(90 * NsPerMin),
            (long)(180 * NsPerMin)
        });
        var vals = new Int32Series("val", [1, 2, 3, 4]);
        using var df = new DataFrame(new List<ISeries> { ts, vals });

        using var dyn = new GroupByDynamicBuilder(df, "ts", "1h30m").Agg(("val", "sum"));
        Assert.Equal(3, dyn.RowCount);

        using var roll = new GroupByRollingBuilder(df, "ts", "1h30m").Agg(("val", "sum"));
        Assert.Equal(4, roll.RowCount);
    }
}
