using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class DataFrameOperationsTests
    {
        [Fact]
        public void TestToDataTable_Basic()
        {
            // Arrange
            var df = new DataFrame(new ISeries[] {
                new Int32Series("id", new int[] { 1, 2, 3 }),
                new Float64Series("val", new double[] { 1.1, 2.2, 3.3 }),
                new Utf8StringSeries("name", new string[] { "a", "b", "c" })
            });

            // Act
            var table = df.ToDataTable();

            // Assert
            Assert.Equal(3, table.Columns.Count);
            Assert.Equal(3, table.Rows.Count);
            Assert.Equal("id", table.Columns[0].ColumnName);
            Assert.Equal(typeof(int), table.Columns[0].DataType);
            Assert.Equal(typeof(double), table.Columns[1].DataType);
            Assert.Equal(typeof(string), table.Columns[2].DataType);
            Assert.Equal(1, table.Rows[0]["id"]);
            Assert.Equal(1.1, table.Rows[0]["val"]);
            Assert.Equal("a", table.Rows[0]["name"]);
            Assert.Equal(3, table.Rows[2]["id"]);
            Assert.Equal(3.3, table.Rows[2]["val"]);
            Assert.Equal("c", table.Rows[2]["name"]);
        }

        [Fact]
        public void TestToDataTable_Nulls()
        {
            // Arrange
            var df = new DataFrame(new ISeries[] {
                Int32Series.FromValues("a", new int?[] { 1, null, 3 }),
                Utf8StringSeries.FromStrings("b", new[] { "x", null, "z" })
            });

            // Act
            var table = df.ToDataTable();

            // Assert
            Assert.Equal(3, table.Rows.Count);
            Assert.Equal(1, table.Rows[0]["a"]);
            Assert.Equal(DBNull.Value, table.Rows[1]["a"]);
            Assert.Equal("x", table.Rows[0]["b"]);
            Assert.Equal(DBNull.Value, table.Rows[1]["b"]);
            Assert.Equal("z", table.Rows[2]["b"]);
        }

        [Fact]
        public void TestToDataTable_Empty()
        {
            // Arrange
            var df = new DataFrame();

            // Act
            var table = df.ToDataTable();

            // Assert
            Assert.Equal(0, table.Columns.Count);
            Assert.Equal(0, table.Rows.Count);
        }

        [Fact]
        public void TestDropNulls_AnyNull()
        {
            // Arrange: DataFrame with nulls
            var df = new DataFrame(new ISeries[] {
                Int32Series.FromValues("a", new int?[] { 1, null, 3, null, 5 }),
                Int32Series.FromValues("b", new int?[] { 10, 20, null, 40, 50 })
            });

            // Act: Drop rows where ANY column is null
            var result = df.DropNulls();

            // Assert: Only rows 0 and 4 should remain (both cols non-null)
            Assert.Equal(2, result.RowCount);
            Assert.Equal(1, ((Int32Series)result.GetColumn("a")).Memory.Span[0]);
            Assert.Equal(5, ((Int32Series)result.GetColumn("a")).Memory.Span[1]);
        }

        [Fact]
        public void TestDropNulls_Subset()
        {
            var df = new DataFrame(new ISeries[] {
                Int32Series.FromValues("a", new int?[] { 1, null, 3, null, 5 }),
                Int32Series.FromValues("b", new int?[] { 10, 20, null, 40, 50 })
            });

            // Act: Drop rows where column "a" is null only
            var result = df.DropNulls(new[] { "a" });

            // Assert: Rows 1 and 3 removed (a is null there)
            Assert.Equal(3, result.RowCount);
            Assert.Equal(1, ((Int32Series)result.GetColumn("a")).Memory.Span[0]);
            Assert.Equal(3, ((Int32Series)result.GetColumn("a")).Memory.Span[1]);
            Assert.Equal(5, ((Int32Series)result.GetColumn("a")).Memory.Span[2]);
        }

        [Fact]
        public void TestDropNulls_AllNull()
        {
            var df = new DataFrame(new ISeries[] {
                Int32Series.FromValues("a", new int?[] { 1, null, 3 }),
                Int32Series.FromValues("b", new int?[] { 10, null, 30 })
            });

            // Act: Drop only rows where ALL subset columns are null
            var result = df.DropNulls(null, anyNull: false);

            // With anyNull=false: row removed only if ALL columns are null
            // Row 1: a=null, b=null -> both null -> remove
            // Rows 0,2 have at least one valid -> keep
            Assert.Equal(2, result.RowCount);
        }

        [Fact]
        public void TestFillNan()
        {
            var df = new DataFrame(new ISeries[] {
                new Float64Series("x", new double[] { 1.0, double.NaN, 3.0, double.NaN }),
                new Int32Series("y", 4)
            });

            var result = df.FillNan(0.0);

            var xCol = (Float64Series)result.GetColumn("x");
            Assert.Equal(1.0, xCol.Memory.Span[0]);
            Assert.Equal(0.0, xCol.Memory.Span[1]);
            Assert.Equal(3.0, xCol.Memory.Span[2]);
            Assert.Equal(0.0, xCol.Memory.Span[3]);
        }

        [Fact]
        public void TestWithRowIndex()
        {
            var df = new DataFrame(new ISeries[] {
                new Int32Series("val", new int[] { 10, 20, 30 })
            });

            var result = df.WithRowIndex();

            Assert.Equal(2, result.Columns.Count);
            var idxCol = (Int32Series)result.GetColumn("index");
            Assert.Equal(3, idxCol.Length);
            Assert.Equal(0, idxCol.Memory.Span[0]);
            Assert.Equal(1, idxCol.Memory.Span[1]);
            Assert.Equal(2, idxCol.Memory.Span[2]);
        }

        [Fact]
        public void TestWithRowIndex_CustomName()
        {
            var df = new DataFrame(new ISeries[] {
                new Int32Series("val", new int[] { 10, 20 })
            });

            var result = df.WithRowIndex("row_id");

            Assert.Equal(2, result.Columns.Count);
            var idxCol = (Int32Series)result.GetColumn("row_id");
            Assert.Equal(0, idxCol.Memory.Span[0]);
            Assert.Equal(1, idxCol.Memory.Span[1]);
        }

        [Fact]
        public void TestRename()
        {
            var df = new DataFrame(new ISeries[] {
                new Int32Series("old_name", new int[] { 1, 2, 3 }),
                new Float64Series("other", new double[] { 1.0, 2.0, 3.0 })
            });

            var result = df.Rename(new Dictionary<string, string> {
                { "old_name", "new_name" }
            });

            Assert.Equal(2, result.Columns.Count);
            Assert.Equal("new_name", result.Columns[0].Name);
            Assert.Equal("other", result.Columns[1].Name);
            Assert.Equal(1, ((Int32Series)result.GetColumn("new_name")).Memory.Span[0]);
        }

        [Fact]
        public void TestNullCount()
        {
            var df = new DataFrame(new ISeries[] {
                Int32Series.FromValues("a", new int?[] { 1, null, 3, null, 5 }),
                Int32Series.FromValues("b", new int?[] { null, null, null, 4, 5 })
            });

            var result = df.NullCount();

            Assert.Equal(2, result.Columns.Count);
            Assert.Equal("column", result.Columns[0].Name);
            Assert.Equal("null_count", result.Columns[1].Name);

            var names = (Utf8StringSeries)result.GetColumn("column");
            var counts = (Int32Series)result.GetColumn("null_count");

            Assert.Equal("a", names.GetString(0));
            Assert.Equal(2, counts.Memory.Span[0]);
            Assert.Equal("b", names.GetString(1));
            Assert.Equal(3, counts.Memory.Span[1]);
        }

        [Fact]
        public void TestSchemaAndDtypes()
        {
            var df = new DataFrame(new ISeries[] {
                new Int32Series("int_col", 3),
                new Float64Series("float_col", 3),
                new Utf8StringSeries("str_col", 3)
            });

            Assert.Equal(3, df.Schema.Count);
            Assert.Equal(typeof(int), df.Schema["int_col"]);
            Assert.Equal(typeof(double), df.Schema["float_col"]);
            Assert.Equal(typeof(string), df.Schema["str_col"]);

            Assert.Equal(3, df.Dtypes.Length);
            Assert.Equal(typeof(int), df.Dtypes[0]);
        }

        [Fact]
        public void TestClone()
        {
            var original = new DataFrame(new ISeries[] {
                new Int32Series("a", new int[] { 1, 2, 3 }),
                new Float64Series("b", new double[] { 1.0, 2.0, 3.0 })
            });

            var cloned = original.Clone();

            Assert.Equal(original.RowCount, cloned.RowCount);
            Assert.Equal(2, ((Int32Series)cloned.GetColumn("a")).Memory.Span[1]);

            // Mutate original to verify independence
            ((Int32Series)original.GetColumn("a")).Memory.Span[1] = 999;
            Assert.Equal(2, ((Int32Series)cloned.GetColumn("a")).Memory.Span[1]);
        }
    }
}
