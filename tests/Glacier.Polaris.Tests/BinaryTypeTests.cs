using System;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class BinaryTypeTests
    {
        private static readonly byte[] Blob1 = Encoding.UTF8.GetBytes("hello world");
        private static readonly byte[] Blob2 = Encoding.UTF8.GetBytes("foo bar");
        private static readonly byte[] Blob3 = Encoding.UTF8.GetBytes("world!");

        private static DataFrame MakeDf() => new DataFrame(new ISeries[]
        {
            new BinarySeries("data", new byte[]?[] { Blob1, Blob2, Blob3 })
        });

        // ---- Construction ------------------------------------------------

        [Fact]
        public void BinarySeries_GetValue_ReturnsCorrectBytes()
        {
            var s = new BinarySeries("b", new byte[]?[] { Blob1, Blob2 });
            Assert.Equal(Blob1, s.GetValue(0));
            Assert.Equal(Blob2, s.GetValue(1));
        }

        [Fact]
        public void BinarySeries_NullEntry_ReturnsNull()
        {
            var s = new BinarySeries("b", new byte[]?[] { Blob1, null, Blob3 });
            Assert.NotNull(s.GetValue(0));
            Assert.Null(s.GetValue(1));
            Assert.NotNull(s.GetValue(2));
        }

        // ---- Bin.Length --------------------------------------------------

        [Fact]
        public async Task BinLength_ReturnsCorrectByteCount()
        {
            var result = await MakeDf().Lazy()
                .Select(Expr.Col("data").Bin().Length().Alias("len"))
                .Collect();

            Assert.Equal(Blob1.Length, result.GetColumn("len").Get(0));
            Assert.Equal(Blob2.Length, result.GetColumn("len").Get(1));
            Assert.Equal(Blob3.Length, result.GetColumn("len").Get(2));
        }

        // ---- Bin.Contains ------------------------------------------------

        [Fact]
        public async Task BinContains_MatchingPattern_ReturnsTrue()
        {
            var pattern = Encoding.UTF8.GetBytes("world");
            var result = await MakeDf().Lazy()
                .Select(Expr.Col("data").Bin().Contains(pattern).Alias("has_world"))
                .Collect();

            Assert.True((bool)result.GetColumn("has_world").Get(0)!);  // "hello world" ✓
            Assert.False((bool)result.GetColumn("has_world").Get(1)!); // "foo bar"     ✗
            Assert.True((bool)result.GetColumn("has_world").Get(2)!);  // "world!"      ✓
        }

        // ---- Bin.StartsWith ----------------------------------------------

        [Fact]
        public async Task BinStartsWith_Prefix_FiltersCorrectly()
        {
            var prefix = Encoding.UTF8.GetBytes("foo");
            var result = await MakeDf().Lazy()
                .Select(Expr.Col("data").Bin().StartsWith(prefix).Alias("starts_foo"))
                .Collect();

            Assert.False((bool)result.GetColumn("starts_foo").Get(0)!);
            Assert.True((bool)result.GetColumn("starts_foo").Get(1)!);
            Assert.False((bool)result.GetColumn("starts_foo").Get(2)!);
        }

        // ---- Bin.EndsWith ------------------------------------------------

        [Fact]
        public async Task BinEndsWith_Suffix_FiltersCorrectly()
        {
            var suffix = Encoding.UTF8.GetBytes("!");
            var result = await MakeDf().Lazy()
                .Select(Expr.Col("data").Bin().EndsWith(suffix).Alias("ends_bang"))
                .Collect();

            Assert.False((bool)result.GetColumn("ends_bang").Get(0)!);
            Assert.False((bool)result.GetColumn("ends_bang").Get(1)!);
            Assert.True((bool)result.GetColumn("ends_bang").Get(2)!);
        }

        // ---- Bin.Decode --------------------------------------------------

        [Fact]
        public async Task BinDecode_Utf8_ProducesCorrectStrings()
        {
            var result = await MakeDf().Lazy()
                .Select(Expr.Col("data").Bin().Decode("utf-8").Alias("text"))
                .Collect();

            Assert.Equal("hello world", result.GetColumn("text").Get(0));
            Assert.Equal("foo bar",     result.GetColumn("text").Get(1));
            Assert.Equal("world!",      result.GetColumn("text").Get(2));
        }

    [Fact]
    public async Task BinDecode_Hex_DecodesHexStringToBinary()
    {
        // Polars' bin.decode("hex") takes a string column (hex-encoded text) and decodes to binary
        var hexStrings = new Utf8StringSeries("hex", new[] { "DEAD" });
        var df = new DataFrame(new ISeries[] { hexStrings });

        var result = await df.Lazy()
            .Select(Expr.Col("hex").Bin().Decode("hex").Alias("decoded"))
            .Collect();

        var binSeries = (BinarySeries)result.GetColumn("decoded");
        Assert.Equal(new byte[] { 0xDE, 0xAD }, binSeries.GetValue(0));
    }

        // ---- Bin.Encode --------------------------------------------------

        [Fact]
        public async Task BinEncode_Utf8_RoundTrips()
        {
            var strDf = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("s", new[] { "hello", "world" })
            });

            var encoded = await strDf.Lazy()
                .Select(Expr.Col("s").Bin().Encode("utf-8").Alias("bin"))
                .Collect();

            var binSeries = (BinarySeries)encoded.GetColumn("bin");
            Assert.Equal(Encoding.UTF8.GetBytes("hello"), binSeries.GetValue(0));
            Assert.Equal(Encoding.UTF8.GetBytes("world"), binSeries.GetValue(1));
        }

        // ---- Arrow round-trip -------------------------------------------

        [Fact]
        public void BinarySeries_ArrowRoundTrip_PreservesValues()
        {
            var original = MakeDf();
            var batch = original.ToArrow();
            var restored = DataFrame.FromArrow(batch);

            var orig = (BinarySeries)original.GetColumn("data");
            var rest = (BinarySeries)restored.GetColumn("data");

            for (int i = 0; i < 3; i++)
                Assert.Equal(orig.GetValue(i), rest.GetValue(i));
        }

        [Fact]
        public void BinarySeries_ArrowRoundTrip_NullsPreserved()
        {
            var df = new DataFrame(new ISeries[]
            {
                new BinarySeries("b", new byte[]?[] { Blob1, null, Blob3 })
            });

            var restored = DataFrame.FromArrow(df.ToArrow());
            var s = (BinarySeries)restored.GetColumn("b");

            Assert.NotNull(s.GetValue(0));
            Assert.Null(s.GetValue(1));
            Assert.NotNull(s.GetValue(2));
        }
    }
}
