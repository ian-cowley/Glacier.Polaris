using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class ExplodeUnnestTests
    {
        [Fact]
        public async Task Test_Explode()
        {
            // Setup data:
            // id: [1, 2]
            // values: [[10, 11], [20]]
            var idSeries = new Int32Series("id", new[] { 1, 2 });
            
            var listOffsets = new Int32Series("offsets", new[] { 0, 2, 3 });
            var listValues = new Int32Series("item", new[] { 10, 11, 20 });
            var listSeries = new ListSeries("values", listOffsets, listValues);

            var df = new DataFrame(new ISeries[] { idSeries, listSeries });
            var lf = df.Lazy();

            var exploded = await lf.Explode("values").Collect();

            // Expected result:
            // id: [1, 1, 2]
            // values: [10, 11, 20]
            Assert.Equal(3, exploded.RowCount);
            
            var resId = (Int32Series)exploded.GetColumn("id");
            Assert.Equal(1, resId.Get(0));
            Assert.Equal(1, resId.Get(1));
            Assert.Equal(2, resId.Get(2));

            var resValues = (Int32Series)exploded.GetColumn("values");
            Assert.Equal(10, resValues.Get(0));
            Assert.Equal(11, resValues.Get(1));
            Assert.Equal(20, resValues.Get(2));
        }

        [Fact]
        public async Task Test_Unnest()
        {
            // Setup data:
            // id: [1, 2]
            // meta: [{a: 10, b: "x"}, {a: 20, b: "y"}]
            var idSeries = new Int32Series("id", new[] { 1, 2 });
            
            var aSeries = new Int32Series("a", new[] { 10, 20 });
            var bSeries = new Utf8StringSeries("b", new[] { "x", "y" });
            var structSeries = new StructSeries("meta", new ISeries[] { aSeries, bSeries });

            var df = new DataFrame(new ISeries[] { idSeries, structSeries });
            var lf = df.Lazy();

            var unnested = await lf.Unnest("meta").Collect();

            // Expected result columns: [id, a, b]
            Assert.Equal(2, unnested.RowCount);
            Assert.Equal(3, unnested.Columns.Count());
            
            Assert.Equal("id", unnested.Columns.ElementAt(0).Name);
            Assert.Equal("a", unnested.Columns.ElementAt(1).Name);
            Assert.Equal("b", unnested.Columns.ElementAt(2).Name);

            var resA = (Int32Series)unnested.GetColumn("a");
            Assert.Equal(10, resA.Get(0));
            Assert.Equal(20, resA.Get(1));

            var resB = (Utf8StringSeries)unnested.GetColumn("b");
            Assert.Equal("x", resB.Get(0));
            Assert.Equal("y", resB.Get(1));
        }
    }
}
