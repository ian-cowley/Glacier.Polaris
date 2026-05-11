using System;
using System.Linq;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class PivotTests
    {
        [Fact]
        public void TestPivotBasic()
        {
            // Data:
            // Date (int), Category (string), Value (int)
            // 1, A, 10
            // 1, B, 20
            // 2, A, 5
            
            var dates = new Int32Series("date", new int[] { 1, 1, 2 });
            var values = new Int32Series("val", new int[] { 10, 20, 5 });
            
            var categories = new string[] { "A", "B", "A" };
            int totalBytes = categories.Sum(s => System.Text.Encoding.UTF8.GetByteCount(s));
            var catSeries = new Utf8StringSeries("cat", 3, totalBytes);
            var offsetSpan = catSeries.Offsets.Span;
            var dataSpan = catSeries.DataBytes.Span;
            int currentOffset = 0;
            for (int i = 0; i < categories.Length; i++)
            {
                offsetSpan[i] = currentOffset;
                var bytes = System.Text.Encoding.UTF8.GetBytes(categories[i]);
                bytes.CopyTo(dataSpan.Slice(currentOffset));
                currentOffset += bytes.Length;
            }
            offsetSpan[3] = currentOffset;

            var df = new DataFrame(new ISeries[] { dates, catSeries, values });

            var result = df.Pivot(index: "date", pivot: "cat", values: "val");

            // Result should have columns: date, A, B
            // Row 1 (date 1): A=10, B=20
            // Row 2 (date 2): A=5, B=0
            
            Assert.Equal(3, result.Columns.Count);
            
            var dateCol = result.GetColumn("date") as Int32Series;
            var aCol = result.GetColumn("A") as Int32Series;
            var bCol = result.GetColumn("B") as Int32Series;

            Assert.NotNull(dateCol);
            Assert.NotNull(aCol);
            Assert.NotNull(bCol);

            int rowDate1 = dateCol.Memory.Span[0] == 1 ? 0 : 1;
            int rowDate2 = dateCol.Memory.Span[0] == 2 ? 0 : 1;

            Assert.Equal(1, dateCol.Memory.Span[rowDate1]);
            Assert.Equal(2, dateCol.Memory.Span[rowDate2]);

            Assert.Equal(10, aCol.Memory.Span[rowDate1]);
            Assert.Equal(5, aCol.Memory.Span[rowDate2]);

            Assert.Equal(20, bCol.Memory.Span[rowDate1]);
            Assert.Equal(0, bCol.Memory.Span[rowDate2]);
        }
    }
}
