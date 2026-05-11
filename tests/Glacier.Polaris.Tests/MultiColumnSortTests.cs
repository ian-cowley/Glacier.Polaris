using System;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class MultiColumnSortTests
    {
        [Fact]
        public async Task TestMixedDirectionSort()
        {
            var cat = new Utf8StringSeries("cat", new[] { "A", "A", "B", "B", "A" });
            var val = new Int32Series("val", new[] { 10, 30, 20, 40, 20 });
            var df = new DataFrame(new ISeries[] { cat, val });

            // Sort by cat ASC, val DESC
            // Expected:
            // A, 30
            // A, 20
            // A, 10
            // B, 40
            // B, 20
            var sorted = await df.Lazy()
                .Sort(new[] { "cat", "val" }, new[] { false, true })
                .Collect();

            var catRes = sorted.GetColumn("cat") as Utf8StringSeries;
            var valRes = sorted.GetColumn("val") as Int32Series;

            Assert.Equal("A", catRes!.GetString(0));
            Assert.Equal(30, valRes!.Memory.Span[0]);

            Assert.Equal("A", catRes.GetString(1));
            Assert.Equal(20, valRes.Memory.Span[1]);

            Assert.Equal("A", catRes.GetString(2));
            Assert.Equal(10, valRes.Memory.Span[2]);

            Assert.Equal("B", catRes.GetString(3));
            Assert.Equal(40, valRes.Memory.Span[3]);

            Assert.Equal("B", catRes.GetString(4));
            Assert.Equal(20, valRes.Memory.Span[4]);
        }

        [Fact]
        public async Task TestFloatSortDescending()
        {
            var val = new Float64Series("val", new[] { 1.5, 3.2, 2.1, 0.5 });
            var df = new DataFrame(new ISeries[] { val });

            var sorted = await df.Lazy()
                .Sort("val", descending: true)
                .Collect();

            var res = sorted.GetColumn("val") as Float64Series;
            Assert.Equal(3.2, res!.Memory.Span[0]);
            Assert.Equal(2.1, res.Memory.Span[1]);
            Assert.Equal(1.5, res.Memory.Span[2]);
            Assert.Equal(0.5, res.Memory.Span[3]);
        }
    }
}
