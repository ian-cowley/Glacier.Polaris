using System;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class LazyReshapingTests
    {
        [Fact]
        public async Task TestLazyPivot()
        {
            var date = new Int32Series("date", new[] { 1, 1, 2, 2 });
            var store = new Int32Series("store", new[] { 101, 102, 101, 102 });
            var cat = new Utf8StringSeries("cat", new[] { "A", "A", "B", "B" });
            var val = new Int32Series("val", new[] { 1, 2, 3, 4 });
            
            var df = new DataFrame(new ISeries[] { date, store, cat, val });

            var pivoted = await df.Lazy()
                .Pivot(index: new[] { "date", "store" }, pivot: "cat", values: "val")
                .Collect();

            Assert.Equal(4, pivoted.RowCount);
            Assert.Contains("A", pivoted.Columns.Select(c => c.Name));
            Assert.Contains("B", pivoted.Columns.Select(c => c.Name));
            
            var aCol = pivoted.GetColumn("A") as Int32Series;
            var bCol = pivoted.GetColumn("B") as Int32Series;
            
            int sumA = 0;
            for(int i=0; i<4; i++) if (!aCol.ValidityMask.IsNull(i)) sumA += aCol.Memory.Span[i];
            Assert.Equal(3, sumA);

            int sumB = 0;
            for(int i=0; i<4; i++) if (!bCol.ValidityMask.IsNull(i)) sumB += bCol.Memory.Span[i];
            Assert.Equal(7, sumB);
        }

        [Fact]
        public async Task TestLazyUnpivot()
        {
            var id = new Int32Series("id", new[] { 1, 2 });
            var valA = new Int32Series("A", new[] { 10, 20 });
            var valB = new Int32Series("B", new[] { 30, 40 });
            var df = new DataFrame(new ISeries[] { id, valA, valB });

            var melted = await df.Lazy()
                .Unpivot(new[] { "id" }, new[] { "A", "B" })
                .Collect();

            Assert.Equal(4, melted.RowCount);
            Assert.Contains("variable", melted.Columns.Select(c => c.Name));
            Assert.Contains("value", melted.Columns.Select(c => c.Name));
        }
    }
}
