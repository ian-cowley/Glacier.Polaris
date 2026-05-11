using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class PivotMeltTests
    {
        [Fact]
        public void TestPivotSum()
        {
            var df = new DataFrame(new ISeries[] {
                new Utf8StringSeries("key", new string[] { "A", "A", "B", "B" }),
                new Utf8StringSeries("piv", new string[] { "X", "Y", "X", "Y" }),
                new Int32Series("val", new int[] { 10, 20, 30, 40 })
            });

            var result = df.Pivot("key", "piv", "val", "sum");

            Assert.Equal(2, result.RowCount);
            
            var keyCol = result.GetColumn("key");
            var xCol = result.GetColumn("X") as Int32Series;
            var yCol = result.GetColumn("Y") as Int32Series;

            Assert.NotNull(xCol);
            Assert.NotNull(yCol);

            for (int i = 0; i < result.RowCount; i++)
            {
                string key = (string)keyCol.Get(i)!;
                int x = xCol.Memory.Span[i];
                int y = yCol.Memory.Span[i];
                System.Console.WriteLine($"Row {i}: Key={key}, X={x}, Y={y}");

                if (key == "A")
                {
                    Assert.Equal(10, x);
                    Assert.Equal(20, y);
                }
                else if (key == "B")
                {
                    Assert.Equal(30, x);
                    Assert.Equal(40, y);
                }
            }
        }

        [Fact]
        public void TestPivotMean()
        {
             var df = new DataFrame(new ISeries[] {
                new Utf8StringSeries("key", new string[] { "A", "A", "B", "B" }),
                new Utf8StringSeries("piv", new string[] { "X", "X", "Y", "Y" }),
                new Int32Series("val", new int[] { 10, 20, 30, 40 })
            });

            var result = df.Pivot("key", "piv", "val", "mean");

            var keyCol = result.GetColumn("key");
            var xCol = result.GetColumn("X") as Float64Series;
            var yCol = result.GetColumn("Y") as Float64Series;

            Assert.NotNull(xCol);
            Assert.NotNull(yCol);

            for (int i = 0; i < result.RowCount; i++)
            {
                string key = (string)keyCol.Get(i)!;
                if (key == "A") Assert.Equal(15.0, xCol.Memory.Span[i]);
                else if (key == "B") Assert.Equal(35.0, yCol.Memory.Span[i]);
            }
        }

        [Fact]
        public void TestMelt()
        {
            var df = new DataFrame(new ISeries[] {
                new Utf8StringSeries("id", new string[] { "A", "B" }),
                new Int32Series("v1", new int[] { 1, 2 }),
                new Int32Series("v2", new int[] { 10, 20 })
            });

            var result = df.Melt(new[] { "id" }, new[] { "v1", "v2" });

            Assert.Equal(4, result.RowCount);
            Assert.Equal("A", (string)result.GetColumn("id").Get(0)!);
            Assert.Equal("v1", (string)result.GetColumn("variable").Get(0)!);
            Assert.Equal(1, (int)result.GetColumn("value").Get(0)!);

            Assert.Equal("A", (string)result.GetColumn("id").Get(2)!);
            Assert.Equal("v2", (string)result.GetColumn("variable").Get(2)!);
            Assert.Equal(10, (int)result.GetColumn("value").Get(2)!);
        }

        [Fact]
        public async Task TestLazyUnpivot()
        {
            var df = new DataFrame(new ISeries[] {
                new Utf8StringSeries("id", new string[] { "A" }),
                new Int32Series("v1", new int[] { 1 })
            });

            var result = await df.Lazy()
                .Unpivot(new[] { "id" }, new[] { "v1" })
                .Collect();

            Assert.Equal(1, result.RowCount);
            Assert.Equal("v1", (string)result.GetColumn("variable").Get(0)!);
            Assert.Equal(1, (int)result.GetColumn("value").Get(0)!);
        }
    }
}
