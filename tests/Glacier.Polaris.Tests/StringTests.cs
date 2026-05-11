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
    }
}
