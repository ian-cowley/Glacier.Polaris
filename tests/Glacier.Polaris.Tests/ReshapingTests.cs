using System;
using System.Linq;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class ReshapingTests
    {
        [Fact]
        public void TestMelt()
        {
            var id = new Int32Series("id", new[] { 1, 2 });
            var valA = new Int32Series("A", new[] { 10, 20 });
            var valB = new Int32Series("B", new[] { 30, 40 });
            var df = new DataFrame(new ISeries[] { id, valA, valB });

            var melted = df.Melt(new[] { "id" }, new[] { "A", "B" });

            // Expect:
            // id: [1, 2, 1, 2]
            // variable: ["A", "A", "B", "B"]
            // value: [10, 20, 30, 40]

            Assert.Equal(4, melted.RowCount);
            var idCol = melted.GetColumn("id") as Int32Series;
            var varCol = melted.GetColumn("variable") as Utf8StringSeries;
            var valCol = melted.GetColumn("value") as Int32Series;

            Assert.Equal(1, idCol.Memory.Span[0]);
            Assert.Equal(2, idCol.Memory.Span[1]);
            Assert.Equal(1, idCol.Memory.Span[2]);
            Assert.Equal(2, idCol.Memory.Span[3]);

            Assert.Equal("A", System.Text.Encoding.UTF8.GetString(varCol.GetStringSpan(0)));
            Assert.Equal("B", System.Text.Encoding.UTF8.GetString(varCol.GetStringSpan(2)));

            Assert.Equal(10, valCol.Memory.Span[0]);
            Assert.Equal(20, valCol.Memory.Span[1]);
            Assert.Equal(30, valCol.Memory.Span[2]);
            Assert.Equal(40, valCol.Memory.Span[3]);
        }

        [Fact]
        public void TestMultiIndexPivot()
        {
            var date = new Int32Series("date", new[] { 1, 1, 2, 2 });
            var store = new Int32Series("store", new[] { 101, 102, 101, 102 });
            var cat = new Utf8StringSeries("cat", new[] { "A", "A", "B", "B" });
            var val = new Int32Series("val", new[] { 1, 2, 3, 4 });
            
            var df = new DataFrame(new ISeries[] { date, store, cat, val });

            var pivoted = df.Pivot(index: new[] { "date", "store" }, pivot: "cat", values: "val");

            // date, store, A, B
            // 1, 101, 1, 0
            // 1, 102, 2, 0
            // 2, 101, 0, 3
            // 2, 102, 0, 4
            
            Assert.Equal(4, pivoted.RowCount);
            Assert.Equal(4, pivoted.Columns.Count);
            
            var aCol = pivoted.GetColumn("A") as Int32Series;
            var bCol = pivoted.GetColumn("B") as Int32Series;
            
            // Note: Order is non-deterministic due to parallel GroupBy.
            // We'll check sums or find rows.
            int sumA = 0;
            for(int i=0; i<4; i++) sumA += aCol.Memory.Span[i];
            Assert.Equal(3, sumA);

            int sumB = 0;
            for(int i=0; i<4; i++) sumB += bCol.Memory.Span[i];
            Assert.Equal(7, sumB);
        }
    }
}
