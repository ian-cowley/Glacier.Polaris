using System;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using System.Linq;

namespace Glacier.Polaris.Tests
{
    public class NestedAggTests
    {
        [Fact]
        public void TestListAggregationArithmetic()
        {
            // df with List column
            // [[1, 2], [3, 4, 5]]
            var values = new Int32Series("values", new int[] { 1, 2, 3, 4, 5 });
            var offsets = new Int32Series("offsets", new int[] { 0, 2, 5 });
            var listCol = new ListSeries("nested", offsets, values);
            
            var df = new DataFrame(new ISeries[] { listCol });

            // select nested.sum() * 2
            var result = df.Select(
                Expr.Col("nested").List().Sum().Alias("sum"),
                (Expr.Col("nested").List().Sum() * 2.0).Alias("sum_double")
            );

            var sumCol = result.GetColumn("sum") as Float64Series;
            var sumDoubleCol = result.GetColumn("sum_double") as Float64Series;

            Assert.NotNull(sumCol);
            Assert.NotNull(sumDoubleCol);
            
            // Group 1: 1+2 = 3
            // Group 2: 3+4+5 = 12
            Assert.Equal(3.0, (double)sumCol.Get(0)!);
            Assert.Equal(12.0, (double)sumCol.Get(1)!);
            
            Assert.Equal(6.0, (double)sumDoubleCol.Get(0)!);
            Assert.Equal(24.0, (double)sumDoubleCol.Get(1)!);
        }

        [Fact]
        public void TestStructFieldExtraction()
        {
            // df with Struct column
            var nameCol = Utf8StringSeries.FromStrings("name", new string[] { "Alice", "Bob" });
            var ageCol = new Int32Series("age", new int[] { 25, 30 });
            var structCol = new StructSeries("person", new ISeries[] { nameCol, ageCol });
            
            var df = new DataFrame(new ISeries[] { structCol });

            // select person.field("name"), person.field("age")
            var result = df.Select(
                Expr.Col("person").Struct().Field("name").Alias("extracted_name"),
                Expr.Col("person").Struct().Field("age").Alias("extracted_age")
            );

            var names = result.GetColumn("extracted_name") as Utf8StringSeries;
            var ages = result.GetColumn("extracted_age") as Int32Series;

            Assert.NotNull(names);
            Assert.NotNull(ages);
            
            Assert.Equal("Alice", names.GetString(0));
            Assert.Equal("Bob", names.GetString(1));
            Assert.Equal(25, ages.Get(0));
            Assert.Equal(30, ages.Get(1));
        }

        [Fact]
        public void TestGroupByImplode()
        {
            // df: id, val
            // 1, 10
            // 1, 20
            // 2, 30
            var idCol = new Int32Series("id", new int[] { 1, 1, 2 });
            var valCol = new Int32Series("val", new int[] { 10, 20, 30 });
            var df = new DataFrame(new ISeries[] { idCol, valCol });

            // groupby id, implode val
            var result = df.GroupBy("id").Agg(
                ("val", "implode")
            );

            var implodeCol = result.GetColumn("val_implode") as ListSeries;
            Assert.NotNull(implodeCol);
            Assert.Equal(2, implodeCol.Length);

            // Group 1: [10, 20]
            // Group 2: [30]
            
            // Verify content using Lengths kernel
            var lens = Compute.ListKernels.Lengths(implodeCol) as Int32Series;
            
            // The order of groups in result might be [1, 2] or [2, 1] depending on hashing.
            var resIdCol = result.GetColumn("id") as Int32Series;
            for (int i = 0; i < result.RowCount; i++)
            {
                int id = (int)resIdCol!.Get(i)!;
                int len = (int)lens!.Get(i)!;
                if (id == 1) Assert.Equal(2, len);
                else if (id == 2) Assert.Equal(1, len);
            }
        }
    }
}
