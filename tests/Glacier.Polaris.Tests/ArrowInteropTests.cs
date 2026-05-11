using Glacier.Polaris;
using Xunit;
using Apache.Arrow;
using System.Linq;

namespace Glacier.Polaris.Tests
{
    public class ArrowInteropTests
    {
        [Fact]
        public void TestDataFrameToArrowAndBack()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Data.Int32Series("a", new[] { 1, 2, 3 }),
                new Data.Float64Series("b", new[] { 1.1, 2.2, 3.3 }),
                new Data.Utf8StringSeries("c", new[] { "apple", "banana", "cherry" })
            });

            // To Arrow
            var batch = df.ToArrowRecordBatch();
            Assert.Equal(3, batch.Length);
            Assert.Equal(3, batch.ColumnCount);

            // Back to DataFrame
            var df2 = DataFrame.FromArrowRecordBatch(batch);
            Assert.Equal(df.RowCount, df2.RowCount);
            Assert.Equal(df.Columns.Count(), df2.Columns.Count());

            Assert.Equal(1, df2.GetColumn("a").Get(0));
            Assert.Equal(2.2, df2.GetColumn("b").Get(1));
            Assert.Equal("cherry", df2.GetColumn("c").Get(2));
        }

        [Fact]
        public void TestArrowWithNulls()
        {
            var s = new Data.Int32Series("a", 3);
            s.Memory.Span[0] = 1;
            s.Memory.Span[2] = 3;
            s.ValidityMask.SetNull(1);

            var df = new DataFrame(new[] { s });
            var batch = df.ToArrowRecordBatch();
            
            var df2 = DataFrame.FromArrowRecordBatch(batch);
            Assert.Equal(1, df2.GetColumn("a").Get(0));
            Assert.Null(df2.GetColumn("a").Get(1));
            Assert.Equal(3, df2.GetColumn("a").Get(2));
        }
    }
}
