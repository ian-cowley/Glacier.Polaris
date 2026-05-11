using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class ListStructTests
    {
        [Fact]
        public async Task TestListAggregations()
        {
            var values = new Int32Series("v", new int[] { 1, 2, 3, 4, 5, 6 });
            var offsets = new Int32Series("o", new int[] { 0, 3, 5, 6 }); // [1,2,3], [4,5], [6]
            var list = new ListSeries("l", offsets, values);
            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy().Select(
                Expr.Col("l").List().Sum().Alias("sum"),
                Expr.Col("l").List().Mean().Alias("mean"),
                Expr.Col("l").List().Min().Alias("min"),
                Expr.Col("l").List().Max().Alias("max"),
                Expr.Col("l").List().Lengths().Alias("len")
            ).Collect();

            Assert.Equal(6.0, (double)result.GetColumn("sum").Get(0)!);
            Assert.Equal(9.0, (double)result.GetColumn("sum").Get(1)!);
            Assert.Equal(6.0, (double)result.GetColumn("sum").Get(2)!);

            Assert.Equal(2.0, (double)result.GetColumn("mean").Get(0)!);
            Assert.Equal(4.5, (double)result.GetColumn("mean").Get(1)!);
            Assert.Equal(6.0, (double)result.GetColumn("mean").Get(2)!);

            Assert.Equal(1, (int)result.GetColumn("min").Get(0)!);
            Assert.Equal(4, (int)result.GetColumn("min").Get(1)!);

            Assert.Equal(3, (int)result.GetColumn("max").Get(0)!);
            Assert.Equal(5, (int)result.GetColumn("max").Get(1)!);

            Assert.Equal(3, (int)result.GetColumn("len").Get(0)!);
            Assert.Equal(2, (int)result.GetColumn("len").Get(1)!);
            Assert.Equal(1, (int)result.GetColumn("len").Get(2)!);
        }

        [Fact]
        public async Task TestStructFieldAccess()
        {
            var a = new Int32Series("a", new int[] { 1, 2, 3 });
            var b = new Utf8StringSeries("b", new string[] { "x", "y", "z" });
            var s = new StructSeries("s", new ISeries[] { a, b });
            var df = new DataFrame(new ISeries[] { s });

            var result = await df.Lazy().Select(
                Expr.Col("s").Struct().Field("a").Alias("a_field"),
                Expr.Col("s").Struct().Field("b").Alias("b_field")
            ).Collect();

            Assert.Equal(1, (int)result.GetColumn("a_field").Get(0)!);
            Assert.Equal("y", (string)result.GetColumn("b_field").Get(1)!);
        }
    }
}
