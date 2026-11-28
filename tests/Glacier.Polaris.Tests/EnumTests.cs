using System;
using Xunit;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class EnumTests
    {
        [Fact]
        public void EnumSeries_Basic_Works()
        {
            var categories = new[] { "A", "B", "C" };
            var strings = new string?[] { "A", "C", "B", null, "A" };
            var series = EnumSeries.FromStrings("test", strings, categories);

            Assert.Equal(5, series.Length);
            Assert.Equal("A", series.Get(0));
            Assert.Equal("C", series.Get(1));
            Assert.Equal("B", series.Get(2));
            Assert.Null(series.Get(3));
            Assert.Equal("A", series.Get(4));

            // Verify codes
            var codes = series.Memory.Span;
            Assert.Equal(0u, codes[0]);
            Assert.Equal(2u, codes[1]);
            Assert.Equal(1u, codes[2]);
            Assert.Equal(0u, codes[4]);
        }

        [Fact]
        public void EnumSeries_InvalidCategory_SetsNull()
        {
            var categories = new[] { "A", "B" };
            var strings = new[] { "A", "C", "B" }; // "C" is invalid
            var series = EnumSeries.FromStrings("test", strings, categories);

            Assert.Equal("A", series.Get(0));
            Assert.Null(series.Get(1)); // "C" should be null
            Assert.Equal("B", series.Get(2));
        }

        [Fact]
        public void EnumSeries_FromStrings_NullableSpanAndCollectionExpression_Works()
        {
            var categories = new[] { "A", "B", "C" };
            ReadOnlySpan<string?> nullableSpan = new string?[] { "A", null, "C" };
            ReadOnlySpan<string> nonNullSpan = new[] { "B", "A" };

            using var s1 = EnumSeries.FromStrings("s1", nullableSpan, categories);
            using var s2 = EnumSeries.FromStrings("s2", nonNullSpan, categories);
            using var s3 = EnumSeries.FromStrings("s3", ["A", null, "B"], categories);

            Assert.True(s1.ValidityMask.IsValid(0));
            Assert.True(s1.ValidityMask.IsNull(1));
            Assert.Equal(2u, s1.Memory.Span[2]);

            Assert.Equal("B", s2.Get(0));
            Assert.Equal("A", s2.Get(1));

            Assert.Equal("A", s3.Get(0));
            Assert.True(s3.ValidityMask.IsNull(1));
            Assert.Equal("B", s3.Get(2));
        }
    }
}
