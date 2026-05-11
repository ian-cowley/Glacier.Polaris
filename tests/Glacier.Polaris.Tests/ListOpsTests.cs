using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class ListOpsTests
    {
        [Fact]
        public async Task TestListGet()
        {
            var values = new Int32Series("v", new int[] { 1, 2, 3, 4, 5, 6 });
            var offsets = new Int32Series("o", new int[] { 0, 3, 5, 6 }); // [1,2,3], [4,5], [6]
            var list = new ListSeries("l", offsets, values);

            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("l").List().Get(0).Alias("first"),
                    Expr.Col("l").List().Get(1).Alias("second"),
                    Expr.Col("l").List().Get(-1).Alias("last")
                )
                .Collect();

            Assert.Equal(1, result.GetColumn("first").Get(0));
            Assert.Equal(4, result.GetColumn("first").Get(1));
            Assert.Equal(6, result.GetColumn("first").Get(2));

            Assert.Equal(2, result.GetColumn("second").Get(0));
            Assert.Equal(5, result.GetColumn("second").Get(1));
            Assert.Null(result.GetColumn("second").Get(2));

            Assert.Equal(3, result.GetColumn("last").Get(0));
            Assert.Equal(5, result.GetColumn("last").Get(1));
            Assert.Equal(6, result.GetColumn("last").Get(2));
        }

        [Fact]
        public async Task TestListContains()
        {
            var values = new Int32Series("v", new int[] { 1, 2, 3, 4, 1, 6 });
            var offsets = new Int32Series("o", new int[] { 0, 3, 5, 6 }); // [1,2,3], [4,1], [6]
            var list = new ListSeries("l", offsets, values);

            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("l").List().Contains(Expr.Lit(1)).Alias("has_1"),
                    Expr.Col("l").List().Contains(Expr.Lit(4)).Alias("has_4")
                )
                .Collect();

            Assert.Equal(true, result.GetColumn("has_1").Get(0));
            Assert.Equal(true, result.GetColumn("has_1").Get(1));
            Assert.Equal(false, result.GetColumn("has_1").Get(2));

            Assert.Equal(false, result.GetColumn("has_4").Get(0));
            Assert.Equal(true, result.GetColumn("has_4").Get(1));
            Assert.Equal(false, result.GetColumn("has_4").Get(2));
        }

        [Fact]
        public async Task TestListJoin()
        {
            var values = new Utf8StringSeries("v", new string[] { "a", "b", "c", "d", "e" });
            var offsets = new Int32Series("o", new int[] { 0, 3, 5 }); // [a,b,c], [d,e]
            var list = new ListSeries("l", offsets, values);

            var df = new DataFrame(new ISeries[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("l").List().Join("-").Alias("joined")
                )
                .Collect();

            Assert.Equal("a-b-c", result.GetColumn("joined").Get(0));
            Assert.Equal("d-e", result.GetColumn("joined").Get(1));
        }

        [Fact]
        public async Task TestListUnique()
        {
            var values = new Int32Series("v", new int[] { 1, 1, 2, 3, 4, 3, 4 });
            var offsets = new Int32Series("o", new int[] { 0, 3, 7, 7 }); // [1,1,2], [3,4,3,4], []
            var list = new ListSeries("l", offsets, values);
            var df = new DataFrame(new[] { list });

            var result = await df.Lazy()
                .Select(
                    Expr.Col("l").List().Unique().Alias("unique")
                )
                .Collect();

            var unique0 = (object?[])result.GetColumn("unique").Get(0)!;
            Assert.Equal(new[] { 1, 2 }, unique0.Cast<int>().ToArray());

            var unique1 = (object?[])result.GetColumn("unique").Get(1)!;
            Assert.Equal(new[] { 3, 4 }, unique1.Cast<int>().ToArray());
        }
    }
}
