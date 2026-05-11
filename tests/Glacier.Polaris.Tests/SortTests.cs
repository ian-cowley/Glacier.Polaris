using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class SortTests
    {
        [Fact]
        public async Task TestSort()
        {
            var ids = new Int32Series("id", new[] { 3, 1, 4, 2 });
            var values = new Utf8StringSeries("val", new[] { "three", "one", "four", "two" });
            var df = new DataFrame(new List<ISeries> { ids, values });

            var lf = df.Lazy().Sort("id");
            var result = await lf.Collect();

            Assert.Equal(4, result.RowCount);
            var resIds = (Int32Series)result.GetColumn("id");
            Assert.Equal(1, resIds.Memory.Span[0]);
            Assert.Equal(2, resIds.Memory.Span[1]);
            Assert.Equal(3, resIds.Memory.Span[2]);
            Assert.Equal(4, resIds.Memory.Span[3]);
        }

        [Fact]
        public async Task TestTopK()
        {
            var ids = new Int32Series("id", new[] { 10, 5, 20, 1, 15 });
            var df = new DataFrame(new List<ISeries> { ids });

            // Sort().Limit(2) should return 1 and 5
            var lf = df.Lazy().Sort("id").Limit(2);
            var result = await lf.Collect();

            Assert.Equal(2, result.RowCount);
            var resIds = (Int32Series)result.GetColumn("id");
            Assert.Equal(1, resIds.Memory.Span[0]);
            Assert.Equal(5, resIds.Memory.Span[1]);
        }

        [Fact]
        public async Task TestStringSort()
        {
            // (a, 2), (a, 1), (b, 3)
            var col1 = new Utf8StringSeries("c1", new[] { "b", "a", "a" });
            var col2 = new Int32Series("c2", new[] { 3, 2, 1 });
            var df = new DataFrame(new List<ISeries> { col1, col2 });

            // Sort by c1 (ascending), then c2 (ascending)
            // c1: a, a, b
            // c2: 1, 2, 3
            var result = await df.Lazy().Sort("c1", "c2").Collect();

            var resC1 = (Utf8StringSeries)result.GetColumn("c1");
            var resC2 = (Int32Series)result.GetColumn("c2");

            Assert.Equal("a", System.Text.Encoding.UTF8.GetString(resC1.GetStringSpan(0)));
            Assert.Equal(1, resC2.Memory.Span[0]);
            
            Assert.Equal("a", System.Text.Encoding.UTF8.GetString(resC1.GetStringSpan(1)));
            Assert.Equal(2, resC2.Memory.Span[1]);
            
            Assert.Equal("b", System.Text.Encoding.UTF8.GetString(resC1.GetStringSpan(2)));
            Assert.Equal(3, resC2.Memory.Span[2]);
        }
    }
}
