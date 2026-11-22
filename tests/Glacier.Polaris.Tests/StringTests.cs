using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class StringTests
    {
        private DataFrame CreateStringDF(string name, string[] values)
        {
            int totalBytes = values.Sum(s => Encoding.UTF8.GetByteCount(s));
            var series = new Utf8StringSeries(name, values.Length, totalBytes);
            var offsets = series.Offsets.Span;
            var data = series.DataBytes.Span;
            int current = 0;
            for (int i = 0; i < values.Length; i++)
            {
                offsets[i] = current;
                var bytes = Encoding.UTF8.GetBytes(values[i]);
                bytes.CopyTo(data.Slice(current));
                current += bytes.Length;
            }
            offsets[values.Length] = current;
            return new DataFrame(new[] { series });
        }

        [Fact]
        public async Task TestStringPredicates()
        {
            var names = new[] { "Alice", "Bob", "Charlie", "David" };
            var df = CreateStringDF("name", names);

            var result = await df.Lazy()
                .Select(
                    Expr.Col("name"),
                    Expr.Col("name").Str().Lengths().Alias("len"),
                    Expr.Col("name").Str().Contains("a").Alias("has_a"),
                    Expr.Col("name").Str().StartsWith("B").Alias("is_b"),
                    Expr.Col("name").Str().EndsWith("e").Alias("ends_e"),
                    Expr.Col("name").Str().ToUppercase().Alias("upper"),
                    Expr.Col("name").Str().ToLowercase().Alias("lower")
                )
                .CollectAsync()
                .FirstAsync();

            var lenCol = result.GetColumn("len") as Int32Series;
            var hasACol = result.GetColumn("has_a") as BooleanSeries;
            var isBCol = result.GetColumn("is_b") as BooleanSeries;
            var upperCol = result.GetColumn("upper") as Utf8StringSeries;

            Assert.NotNull(lenCol);
            Assert.NotNull(hasACol);
            Assert.NotNull(isBCol);
            Assert.NotNull(upperCol);

            // Lengths
            Assert.Equal(5, lenCol.Memory.Span[0]); // Alice
            Assert.Equal(3, lenCol.Memory.Span[1]); // Bob
            Assert.Equal(7, lenCol.Memory.Span[2]); // Charlie
            Assert.Equal(5, lenCol.Memory.Span[3]); // David

            // Contains "a" (case sensitive)
            Assert.False(hasACol.Memory.Span[0]); // Alice
            Assert.False(hasACol.Memory.Span[1]); // Bob
            Assert.True(hasACol.Memory.Span[2]);  // Charlie
            Assert.True(hasACol.Memory.Span[3]);  // David

            // StartsWith "B"
            Assert.False(isBCol.Memory.Span[0]);
            Assert.True(isBCol.Memory.Span[1]);

            // ToUpper
            Assert.Equal("ALICE", Encoding.UTF8.GetString(upperCol.GetStringSpan(0)));
            Assert.Equal("BOB", Encoding.UTF8.GetString(upperCol.GetStringSpan(1)));
        }

        [Fact]
        public async Task TestStringReplace()
        {
            var df = CreateStringDF("name", new[] { "hello world", "goodbye world", "hello hello", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("name").Str().Replace("hello", "hi").Alias("replaced"),
                    Expr.Col("name").Str().ReplaceAll("l", "L").Alias("replaced_all"),
                    Expr.Col("name").Str().Replace("nonexistent", "x").Alias("no_match")
                )
                .CollectAsync()
                .FirstAsync();

            var replaced = result.GetColumn("replaced") as Utf8StringSeries;
            var replacedAll = result.GetColumn("replaced_all") as Utf8StringSeries;
            var noMatch = result.GetColumn("no_match") as Utf8StringSeries;

            Assert.NotNull(replaced);
            Assert.NotNull(replacedAll);
            Assert.NotNull(noMatch);

            Assert.Equal("hi world", Encoding.UTF8.GetString(replaced.GetStringSpan(0)));
            Assert.Equal("goodbye world", Encoding.UTF8.GetString(replaced.GetStringSpan(1))); // no "hello"
            Assert.Equal("hi hello", Encoding.UTF8.GetString(replaced.GetStringSpan(2))); // first only
            Assert.Equal("", Encoding.UTF8.GetString(replaced.GetStringSpan(3))); // empty string

            Assert.Equal("heLLo worLd", Encoding.UTF8.GetString(replacedAll.GetStringSpan(0)));
            Assert.Equal("goodbye worLd", Encoding.UTF8.GetString(replacedAll.GetStringSpan(1)));

            Assert.Equal("hello world", Encoding.UTF8.GetString(noMatch.GetStringSpan(0)));
        }

        [Fact]
        public async Task TestStringStrip()
        {
            var df = CreateStringDF("s", new[] { "  hello  ", "world", "  spaced  ", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().Strip().Alias("stripped"),
                    Expr.Col("s").Str().LStrip().Alias("lstripped"),
                    Expr.Col("s").Str().RStrip().Alias("rstripped")
                )
                .CollectAsync()
                .FirstAsync();

            var stripped = result.GetColumn("stripped") as Utf8StringSeries;
            var lstripped = result.GetColumn("lstripped") as Utf8StringSeries;
            var rstripped = result.GetColumn("rstripped") as Utf8StringSeries;

            Assert.NotNull(stripped);
            Assert.NotNull(lstripped);
            Assert.NotNull(rstripped);

            Assert.Equal("hello", Encoding.UTF8.GetString(stripped.GetStringSpan(0)));
            Assert.Equal("world", Encoding.UTF8.GetString(stripped.GetStringSpan(1)));
            Assert.Equal("spaced", Encoding.UTF8.GetString(stripped.GetStringSpan(2)));
            Assert.Equal("", Encoding.UTF8.GetString(stripped.GetStringSpan(3)));

            Assert.Equal("hello  ", Encoding.UTF8.GetString(lstripped.GetStringSpan(0)));
            Assert.Equal("world", Encoding.UTF8.GetString(lstripped.GetStringSpan(1)));

            Assert.Equal("  hello", Encoding.UTF8.GetString(rstripped.GetStringSpan(0)));
            Assert.Equal("world", Encoding.UTF8.GetString(rstripped.GetStringSpan(1)));
        }

        [Fact]
        public async Task TestStringSplit()
        {
            var df = CreateStringDF("s", new[] { "a,b,c", "hello world", "single", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().Split(",").Alias("split_by_comma"),
                    Expr.Col("s").Str().Split(" ").Alias("split_by_space")
                )
                .CollectAsync()
                .FirstAsync();

            var splitComma = result.GetColumn("split_by_comma") as ListSeries;
            var splitSpace = result.GetColumn("split_by_space") as ListSeries;

            Assert.NotNull(splitComma);
            Assert.NotNull(splitSpace);

            // "a,b,c" split by comma → ["a", "b", "c"]
            Assert.Equal(3, splitComma.Offsets.Memory.Span[1] - splitComma.Offsets.Memory.Span[0]);
            var inner0 = splitComma.Values as Utf8StringSeries;
            Assert.NotNull(inner0);
            Assert.Equal("a", Encoding.UTF8.GetString(inner0.GetStringSpan(0)));

            // "hello world" split by comma → ["hello world"] (no match)
            Assert.Equal(1, splitComma.Offsets.Memory.Span[2] - splitComma.Offsets.Memory.Span[1]);

            // "hello world" split by space → ["hello", "world"] (row index 1)
            Assert.Equal(2, splitSpace.Offsets.Memory.Span[2] - splitSpace.Offsets.Memory.Span[1]);
        }

        [Fact]
        public async Task TestStringSlice()
        {
            var df = CreateStringDF("s", new[] { "hello", "world", "ab", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().Slice(0, 3).Alias("first3"),
                    Expr.Col("s").Str().Slice(2).Alias("from2"),
                    Expr.Col("s").Str().Slice(100).Alias("beyond")
                )
                .CollectAsync()
                .FirstAsync();

            var first3 = result.GetColumn("first3") as Utf8StringSeries;
            var from2 = result.GetColumn("from2") as Utf8StringSeries;
            var beyond = result.GetColumn("beyond") as Utf8StringSeries;

            Assert.NotNull(first3);
            Assert.NotNull(from2);
            Assert.NotNull(beyond);

            Assert.Equal("hel", Encoding.UTF8.GetString(first3.GetStringSpan(0)));
            Assert.Equal("wor", Encoding.UTF8.GetString(first3.GetStringSpan(1)));
            Assert.Equal("ab", Encoding.UTF8.GetString(first3.GetStringSpan(2)));  // length=2, start=0 => full string
            Assert.Equal("", Encoding.UTF8.GetString(first3.GetStringSpan(3)));

            Assert.Equal("llo", Encoding.UTF8.GetString(from2.GetStringSpan(0)));
            Assert.Equal("rld", Encoding.UTF8.GetString(from2.GetStringSpan(1)));
            Assert.Equal("", Encoding.UTF8.GetString(from2.GetStringSpan(2)));  // "ab" start=2 => empty

            Assert.Equal("", Encoding.UTF8.GetString(beyond.GetStringSpan(0)));
        }

        [Fact]
        public async Task TestStringToDateAndDatetime()
        {
            var df = CreateStringDF("s", new[] { "2024-01-15", "2023-12-31", "invalid", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().ToDate().Alias("parsed_date")
                )
                .CollectAsync()
                .FirstAsync();

            var parsed = result.GetColumn("parsed_date") as DateSeries;
            Assert.NotNull(parsed);
            Assert.True(parsed.ValidityMask.IsValid(0));  // "2024-01-15" is valid
            Assert.True(parsed.ValidityMask.IsNull(2));   // "invalid" should be null
            Assert.True(parsed.ValidityMask.IsNull(3));   // "" should be null

            // Verify date values (days since epoch)
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var d0 = epoch.AddDays(parsed.Memory.Span[0]);
            Assert.Equal(2024, d0.Year);
            Assert.Equal(1, d0.Month);
            Assert.Equal(15, d0.Day);
        }

        [Fact]
        public async Task TestStringToDateWithCustomFormat()
        {
            var df = CreateStringDF("s", new[] { "15/01/2024", "31/12/2023", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().ToDate("dd/MM/yyyy").Alias("custom_date")
                )
                .CollectAsync()
                .FirstAsync();

            var parsed = result.GetColumn("custom_date") as DateSeries;
            Assert.NotNull(parsed);
            Assert.True(parsed.ValidityMask.IsValid(0));

            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var d0 = epoch.AddDays(parsed.Memory.Span[0]);
            Assert.Equal(2024, d0.Year);
            Assert.Equal(1, d0.Month);
            Assert.Equal(15, d0.Day);
        }

        [Fact]
        public async Task TestStringToDatetime()
        {
            var df = CreateStringDF("s", new[] { "2024-01-15 10:30:00", "invalid", "" });
            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().ToDatetime().Alias("parsed_dt")
                )
                .CollectAsync()
                .FirstAsync();

            var parsed = result.GetColumn("parsed_dt") as DatetimeSeries;
            Assert.NotNull(parsed);
            Assert.True(parsed.ValidityMask.IsValid(0));
            Assert.True(parsed.ValidityMask.IsNull(1));
            Assert.True(parsed.ValidityMask.IsNull(2));
        }

        [Fact]
        public async Task TestStringToDoubleAndIntCast()
        {
            var series = Utf8StringSeries.FromStrings("s", new[] { "$1,234.56", "100.5", "42", "invalid", "", null });
            var df = new DataFrame(new[] { series });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("s").Str().ReplaceAll("$", "").Str().ReplaceAll(",", "").Cast(typeof(double)).Alias("to_double"),
                    Expr.Col("s").Str().ReplaceAll("$", "").Str().ReplaceAll(",", "").Cast(typeof(long)).Alias("to_long")
                )
                .CollectAsync()
                .FirstAsync();

            var toDouble = result.GetColumn("to_double") as Float64Series;
            var toLong = result.GetColumn("to_long") as Int64Series;

            Assert.NotNull(toDouble);
            Assert.NotNull(toLong);

            // Double assertions
            Assert.True(toDouble.ValidityMask.IsValid(0));
            Assert.Equal(1234.56, toDouble.Memory.Span[0]);

            Assert.True(toDouble.ValidityMask.IsValid(1));
            Assert.Equal(100.5, toDouble.Memory.Span[1]);

            Assert.True(toDouble.ValidityMask.IsValid(2));
            Assert.Equal(42.0, toDouble.Memory.Span[2]);

            Assert.True(toDouble.ValidityMask.IsNull(3)); // invalid
            Assert.True(toDouble.ValidityMask.IsNull(4)); // empty
            Assert.True(toDouble.ValidityMask.IsNull(5)); // null

            // Long assertions
            Assert.True(toLong.ValidityMask.IsNull(0)); // "1234.56" is not a valid long
            Assert.True(toLong.ValidityMask.IsNull(1)); // "100.5" is not a valid long

            Assert.True(toLong.ValidityMask.IsValid(2)); // "42" -> 42
            Assert.Equal(42L, toLong.Memory.Span[2]);

            Assert.True(toLong.ValidityMask.IsNull(3));
            Assert.True(toLong.ValidityMask.IsNull(4));
            Assert.True(toLong.ValidityMask.IsNull(5));
        }

        [Fact]
        public async Task TestNumericAndBooleanToStringCastWithNulls()
        {
            var nums = Int64Series.FromValues("nums", new long?[] { 10L, null, -42L, 0L });
            var floats = Float64Series.FromValues("floats", new double?[] { 12.5, null, -3.25, 0.0 });
            var bools = BooleanSeries.FromValues("flags", new bool?[] { true, null, false, true });
            var decs = new DecimalSeries("decs", new decimal?[] { 1.25m, null, -99.50m, 0m });
            using var df = new DataFrame(new ISeries[] { nums, floats, bools, decs });

            using var result = await df.Lazy()
                .Select(
                    Expr.Col("nums").Cast(typeof(string)).Alias("nums_str"),
                    Expr.Col("floats").Cast(typeof(Utf8StringSeries)).Alias("floats_str"),
                    Expr.Col("flags").Cast(typeof(string)).Alias("flags_str"),
                    Expr.Col("decs").Cast(typeof(string)).Alias("decs_str")
                )
                .CollectAsync()
                .FirstAsync();

            var numsStr = Assert.IsType<Utf8StringSeries>(result.GetColumn("nums_str"));
            Assert.Equal("10", numsStr.GetString(0));
            Assert.True(numsStr.ValidityMask.IsNull(1));
            Assert.Equal("-42", numsStr.GetString(2));
            Assert.Equal("0", numsStr.GetString(3));

            var floatsStr = Assert.IsType<Utf8StringSeries>(result.GetColumn("floats_str"));
            Assert.Equal("12.5", floatsStr.GetString(0));
            Assert.True(floatsStr.ValidityMask.IsNull(1));
            Assert.Equal("-3.25", floatsStr.GetString(2));
            Assert.Equal("0", floatsStr.GetString(3));

            var flagsStr = Assert.IsType<Utf8StringSeries>(result.GetColumn("flags_str"));
            Assert.Equal("true", flagsStr.GetString(0));
            Assert.True(flagsStr.ValidityMask.IsNull(1));
            Assert.Equal("false", flagsStr.GetString(2));
            Assert.Equal("true", flagsStr.GetString(3));

            var decsStr = Assert.IsType<Utf8StringSeries>(result.GetColumn("decs_str"));
            Assert.Equal("1.25", decsStr.GetString(0));
            Assert.True(decsStr.ValidityMask.IsNull(1));
            Assert.Equal("-99.50", decsStr.GetString(2));
            Assert.Equal("0", decsStr.GetString(3));
        }

        [Fact]
        public async Task TestStringToStringCastProducesOwnedCopy()
        {
            var src = Utf8StringSeries.FromStrings("s", new[] { "alpha", null, "gamma" });
            using var df = new DataFrame(new[] { src });

            using (var result = await df.Lazy()
                .Select(Expr.Col("s").Cast(typeof(string)).Alias("s_copy"))
                .CollectAsync()
                .FirstAsync())
            {
                var copy = Assert.IsType<Utf8StringSeries>(result.GetColumn("s_copy"));
                Assert.NotSame(src, copy);
                Assert.Equal("alpha", copy.GetString(0));
                Assert.True(copy.ValidityMask.IsNull(1));
                Assert.Equal("gamma", copy.GetString(2));
            }

            // Source series remains valid and undisposed after result disposal
            Assert.Equal("alpha", src.GetString(0));
            Assert.True(src.ValidityMask.IsNull(1));
            Assert.Equal("gamma", src.GetString(2));
        }

        [Fact]
        public async Task TestFloatNaNInfinityAndOverflowCastToInt64ProducesNulls()
        {
            var f64 = Float64Series.FromValues("f64", new double?[]
            {
                12.7,
                double.NaN,
                double.PositiveInfinity,
                double.NegativeInfinity,
                1e30,
                -3.5,
                null
            });
            var f32 = Float32Series.FromValues("f32", new float?[]
            {
                7.2f,
                float.NaN,
                float.PositiveInfinity,
                1e25f,
                null,
                -2.0f,
                0.0f
            });
            using var df = new DataFrame(new ISeries[] { f64, f32 });

            using var result = await df.Lazy()
                .Select(
                    Expr.Col("f64").Cast(typeof(long)).Alias("i64_from_f64"),
                    Expr.Col("f32").Cast(typeof(long)).Alias("i64_from_f32")
                )
                .CollectAsync()
                .FirstAsync();

            var c64 = Assert.IsType<Int64Series>(result.GetColumn("i64_from_f64"));
            Assert.True(c64.ValidityMask.IsValid(0));
            Assert.Equal(13L, c64.Memory.Span[0]);
            Assert.True(c64.ValidityMask.IsNull(1)); // NaN
            Assert.True(c64.ValidityMask.IsNull(2)); // +Inf
            Assert.True(c64.ValidityMask.IsNull(3)); // -Inf
            Assert.True(c64.ValidityMask.IsNull(4)); // out-of-range
            Assert.True(c64.ValidityMask.IsValid(5));
            Assert.Equal(-4L, c64.Memory.Span[5]);
            Assert.True(c64.ValidityMask.IsNull(6));

            var c32 = Assert.IsType<Int64Series>(result.GetColumn("i64_from_f32"));
            Assert.True(c32.ValidityMask.IsValid(0));
            Assert.Equal(7L, c32.Memory.Span[0]);
            Assert.True(c32.ValidityMask.IsNull(1)); // NaN
            Assert.True(c32.ValidityMask.IsNull(2)); // +Inf
            Assert.True(c32.ValidityMask.IsNull(3)); // out-of-range
            Assert.True(c32.ValidityMask.IsNull(4));
            Assert.True(c32.ValidityMask.IsValid(5));
            Assert.Equal(-2L, c32.Memory.Span[5]);
        }

        [Fact]
        public async Task TestBooleanToDoubleAndUnsupportedCastRejection()
        {
            var bools = BooleanSeries.FromValues("b", new bool?[] { true, null, false });
            var dates = new DateSeries("d", new[] { new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc) });
            var dts = DatetimeSeries.FromValues("dt", new long?[] { 1_700_000_000_000_000_000L });

            using var boolDf = new DataFrame(new ISeries[] { bools });
            using var boolRes = await boolDf.Lazy()
                .Select(
                    Expr.Col("b").Cast(typeof(double)).Alias("b_double"),
                    Expr.Col("b").Cast(typeof(long)).Alias("b_long")
                )
                .CollectAsync()
                .FirstAsync();

            var bDouble = Assert.IsType<Float64Series>(boolRes.GetColumn("b_double"));
            Assert.Equal(1.0, bDouble.Memory.Span[0]);
            Assert.True(bDouble.ValidityMask.IsNull(1));
            Assert.Equal(0.0, bDouble.Memory.Span[2]);

            var bLong = Assert.IsType<Int64Series>(boolRes.GetColumn("b_long"));
            Assert.Equal(1L, bLong.Memory.Span[0]);
            Assert.True(bLong.ValidityMask.IsNull(1));
            Assert.Equal(0L, bLong.Memory.Span[2]);

            using var dtDf = new DataFrame(new ISeries[] { dates, dts });
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await dtDf.Lazy().Select(Expr.Col("d").Cast(typeof(double)).Alias("bad")).CollectAsync().FirstAsync());
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await dtDf.Lazy().Select(Expr.Col("dt").Cast(typeof(double)).Alias("bad")).CollectAsync().FirstAsync());
            await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await dtDf.Lazy().Select(Expr.Col("dt").Cast(typeof(long)).Alias("bad")).CollectAsync().FirstAsync());
        }

        [Fact]
        public async Task TestPre1970DatetimeAndOutOfRangeDateCastToString()
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var dts = DatetimeSeries.FromValues("dt", new long?[]
            {
                -1_000_000_000L,
                -86_400_000_000_000L,
                0L,
                long.MinValue,
                long.MaxValue,
                null
            });
            var dates = DateSeries.FromValues("d", new int?[]
            {
                -1,
                0,
                int.MinValue,
                int.MaxValue,
                null
            });

            using var dtDf = new DataFrame(new ISeries[] { dts });
            using var dtRes = await dtDf.Lazy()
                .Select(Expr.Col("dt").Cast(typeof(string)).Alias("dt_str"))
                .CollectAsync()
                .FirstAsync();

            var dtStr = Assert.IsType<Utf8StringSeries>(dtRes.GetColumn("dt_str"));
            Assert.Equal("1969-12-31 23:59:59.0000000", dtStr.GetString(0));
            Assert.Equal("1969-12-31 00:00:00.0000000", dtStr.GetString(1));
            Assert.Equal("1970-01-01 00:00:00.0000000", dtStr.GetString(2));
            Assert.Equal(epoch.AddTicks((long)(((Int128)long.MinValue - 99) / 100)).ToString("yyyy-MM-dd HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture), dtStr.GetString(3));
            Assert.Equal(epoch.AddTicks(long.MaxValue / 100).ToString("yyyy-MM-dd HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture), dtStr.GetString(4));
            Assert.True(dtStr.ValidityMask.IsNull(5));

            using var dateDf = new DataFrame(new ISeries[] { dates });
            using var dateRes = await dateDf.Lazy()
                .Select(Expr.Col("d").Cast(typeof(string)).Alias("d_str"))
                .CollectAsync()
                .FirstAsync();

            var dateStr = Assert.IsType<Utf8StringSeries>(dateRes.GetColumn("d_str"));
            Assert.Equal("1969-12-31", dateStr.GetString(0));
            Assert.Equal("1970-01-01", dateStr.GetString(1));
            Assert.Equal("0001-01-01", dateStr.GetString(2));
            Assert.Equal("9999-12-31", dateStr.GetString(3));
            Assert.True(dateStr.ValidityMask.IsNull(4));
        }
    }
}
