using System;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class GroupByTests
    {
        [Fact]
        public void TestMultiColumnGroupByAgg()
        {
            // Data:
            // A (int), B (int), Value (int)
            // 1, 10, 100
            // 1, 10, 200
            // 1, 20, 300
            // 2, 10, 400
            
            var colA = new Int32Series("A", new int[] { 1, 1, 1, 2 });
            var colB = new Int32Series("B", new int[] { 10, 10, 20, 10 });
            var colVal = new Int32Series("Val", new int[] { 100, 200, 300, 400 });
            
            var df = new DataFrame(new ISeries[] { colA, colB, colVal });

            // Group by A and B, sum Val, count Val
            var result = df.GroupBy("A", "B").Agg(
                ("Val", "sum"),
                ("Val", "count")
            );

            // Groups:
            // (1, 10) -> Sum 300, Count 2
            // (1, 20) -> Sum 300, Count 1
            // (2, 10) -> Sum 400, Count 1
            
            var sumCol = result.GetColumn("Val_sum") as Int32Series;
            var countCol = result.GetColumn("Val_count") as Int32Series;

            Assert.NotNull(sumCol);
            Assert.NotNull(countCol);
            Assert.Equal(3, sumCol.Length);
            
            var sums = new int[3];
            sumCol.Memory.Span.CopyTo(sums);
            Array.Sort(sums);
            
            Assert.Equal(300, sums[0]);
            Assert.Equal(300, sums[1]);
            Assert.Equal(400, sums[2]);

            var counts = new int[3];
            countCol.Memory.Span.CopyTo(counts);
            Array.Sort(counts);
            Assert.Equal(1, counts[0]);
            Assert.Equal(1, counts[1]);
            Assert.Equal(2, counts[2]);
        }
    }
}
