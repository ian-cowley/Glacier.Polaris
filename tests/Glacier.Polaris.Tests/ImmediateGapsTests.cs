using System;
using System.Linq;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Apache.Arrow;

namespace Glacier.Polaris.Tests
{
    /// <summary>
    /// Tests validating parity with Python Polars for the "Immediate Gaps" phase:
    ///   - Transpose
    ///   - FillNull strategies (Forward, Backward, Min, Max, Mean, Zero, One)
    ///   - FillNull with literal value
    ///   - Arrow round-trip (ToArrow / FromArrow)
    ///   - ArraySeries (fixed-size list)
    /// </summary>
    public class ImmediateGapsTests
    {
        // ─────────────────────────────────────────────────────────────────────
        // TRANSPOSE
        // ─────────────────────────────────────────────────────────────────────

        [Fact]
        public void Transpose_IntColumns_RotatesCorrectly()
        {
            // Python Polars:
            //   df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
            //   df.transpose()  =>  column_0=[1,3], column_1=[2,4]
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 1, 2 }),
                new Int32Series("b", new[] { 3, 4 }),
            });

            var t = df.Transpose();

            Assert.Equal(2, t.RowCount);          // 2 original columns → 2 rows
            Assert.Equal(2, t.Columns.Count);     // 2 original rows → 2 columns

            var col0 = (Int32Series)t.GetColumn("column_0");
            var col1 = (Int32Series)t.GetColumn("column_1");

            Assert.Equal(1, col0.Memory.Span[0]); // a[0]
            Assert.Equal(3, col0.Memory.Span[1]); // b[0]
            Assert.Equal(2, col1.Memory.Span[0]); // a[1]
            Assert.Equal(4, col1.Memory.Span[1]); // b[1]
        }

        [Fact]
        public void Transpose_IncludeHeader_AddsHeaderColumn()
        {
            // Python Polars:
            //   df.transpose(include_header=True, header_name="field")
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("x", new[] { 10, 20 }),
                new Int32Series("y", new[] { 30, 40 }),
            });

            var t = df.Transpose(include_header: true, header_name: "field");

            // First column should be the original column names
            var header = (Utf8StringSeries)t.GetColumn("field");
            Assert.Equal("x", header.GetString(0));
            Assert.Equal("y", header.GetString(1));
        }

        [Fact]
        public void Transpose_CustomColumnNames_AppliesNames()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 1, 2, 3 }),
            });

            var t = df.Transpose(column_names: new[] { "r0", "r1", "r2" });

            Assert.NotNull(t.GetColumn("r0"));
            Assert.NotNull(t.GetColumn("r1"));
            Assert.NotNull(t.GetColumn("r2"));
        }

        [Fact]
        public void Transpose_MixedNumericTypes_CoercesToFloat64()
        {
            // Mixed Int32 + Float64 should coerce all result columns to Float64
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 1 }),
                new Float64Series("b", new[] { 2.5 }),
            });

            var t = df.Transpose();

            // Both value columns should be Float64 after coercion
            var col0 = t.GetColumn("column_0");
            Assert.True(col0 is Float64Series, $"Expected Float64Series but got {col0.GetType().Name}");
        }

        [Fact]
        public void Transpose_EmptyDataFrame_ReturnsEmpty()
        {
            var df = new DataFrame();
            var t = df.Transpose();
            Assert.Equal(0, t.RowCount);
        }

        [Fact]
        public void Transpose_SingleRow_CorrectShape()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 7 }),
                new Int32Series("b", new[] { 8 }),
                new Int32Series("c", new[] { 9 }),
            });

            var t = df.Transpose();
            // 3 columns → 3 rows; 1 original row → 1 column
            Assert.Equal(3, t.RowCount);
            Assert.Equal(1, t.Columns.Count);
        }

        // ─────────────────────────────────────────────────────────────────────
        // FILLNULL STRATEGIES
        // ─────────────────────────────────────────────────────────────────────

        [Fact]
        public async System.Threading.Tasks.Task FillNull_ForwardFill_PropagatesToEnd()
        {
            // Python: s.fill_null(strategy="forward")
            // null at end carries last valid value forward
            var s = new Int32Series("v", 5);
            s.Memory.Span[0] = 10;
            s.Memory.Span[2] = 20;
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetValid(2);
            s.ValidityMask.SetNull(3);
            s.ValidityMask.SetNull(4);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Forward)).Collect();

            var span = ((Int32Series)result.GetColumn("v")).Memory.Span;
            Assert.Equal(10, span[1]); // forward from index 0
            Assert.Equal(20, span[3]); // forward from index 2
            Assert.Equal(20, span[4]); // forward from index 2
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_ForwardFill_NullAtStart_RemainsNull()
        {
            // Python: null leading values with no prior valid → stays null
            var s = new Int32Series("v", 4);
            s.Memory.Span[2] = 5;
            s.ValidityMask.SetNull(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetValid(2);
            s.ValidityMask.SetNull(3);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Forward)).Collect();

            var col = result.GetColumn("v");
            Assert.True(col.ValidityMask.IsNull(0));
            Assert.True(col.ValidityMask.IsNull(1));
            Assert.Equal(5, ((Int32Series)col).Memory.Span[3]);
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_BackwardFill_PropagatesToStart()
        {
            // Python: s.fill_null(strategy="backward")
            var s = new Int32Series("v", 5);
            s.Memory.Span[1] = 100;
            s.Memory.Span[3] = 200;
            s.ValidityMask.SetNull(0);
            s.ValidityMask.SetValid(1);
            s.ValidityMask.SetNull(2);
            s.ValidityMask.SetValid(3);
            s.ValidityMask.SetNull(4); // trailing null: no future value, remains null

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Backward)).Collect();

            var col = (Int32Series)result.GetColumn("v");
            Assert.Equal(100, col.Memory.Span[0]); // back-filled from index 1
            Assert.Equal(200, col.Memory.Span[2]); // back-filled from index 3
            Assert.True(result.GetColumn("v").ValidityMask.IsNull(4)); // trailing null stays null
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_MinStrategy_FillsWithColumnMin()
        {
            // Python: s.fill_null(strategy="min")
            var s = new Int32Series("v", 4);
            s.Memory.Span[0] = 3;
            s.Memory.Span[2] = 7;
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetValid(2);
            s.ValidityMask.SetNull(3);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Min)).Collect();

            var col = (Int32Series)result.GetColumn("v");
            Assert.Equal(3, col.Memory.Span[1]); // min(3,7)=3
            Assert.Equal(3, col.Memory.Span[3]); // min(3,7)=3
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_MaxStrategy_FillsWithColumnMax()
        {
            var s = new Int32Series("v", 4);
            s.Memory.Span[0] = 3;
            s.Memory.Span[2] = 7;
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetValid(2);
            s.ValidityMask.SetNull(3);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Max)).Collect();

            var col = (Int32Series)result.GetColumn("v");
            Assert.Equal(7, col.Memory.Span[1]); // max(3,7)=7
            Assert.Equal(7, col.Memory.Span[3]);
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_MeanStrategy_FillsWithColumnMean()
        {
            // Python: s.fill_null(strategy="mean") on Float64
            var s = new Float64Series("v", new[] { 2.0, double.NaN, 4.0 });
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetValid(2);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Mean)).Collect();

            var col = (Float64Series)result.GetColumn("v");
            Assert.Equal(3.0, col.Memory.Span[1], precision: 10); // mean(2,4)=3
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_ZeroStrategy_FillsWithZero()
        {
            var s = new Int32Series("v", 3);
            s.Memory.Span[0] = 5;
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetNull(2);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Zero)).Collect();

            var col = (Int32Series)result.GetColumn("v");
            Assert.Equal(0, col.Memory.Span[1]);
            Assert.Equal(0, col.Memory.Span[2]);
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_OneStrategy_FillsWithOne()
        {
            var s = new Int32Series("v", 3);
            s.Memory.Span[0] = 5;
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetNull(2);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.One)).Collect();

            var col = (Int32Series)result.GetColumn("v");
            Assert.Equal(1, col.Memory.Span[1]);
            Assert.Equal(1, col.Memory.Span[2]);
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_String_ForwardFill()
        {
            var s = Utf8StringSeries.FromStrings("v", new string?[] { "hello", null, null });

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Forward)).Collect();

            var col = (Utf8StringSeries)result.GetColumn("v");
            Assert.Equal("hello", col.GetString(1));
            Assert.Equal("hello", col.GetString(2));
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_NoNulls_ReturnsSameValues()
        {
            // Guard: filling a series with no nulls should be a no-op
            var s = new Int32Series("v", new[] { 1, 2, 3 });
            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Forward)).Collect();

            var col = (Int32Series)result.GetColumn("v");
            Assert.Equal(1, col.Memory.Span[0]);
            Assert.Equal(2, col.Memory.Span[1]);
            Assert.Equal(3, col.Memory.Span[2]);
            Assert.True(col.ValidityMask.IsValid(0));
            Assert.True(col.ValidityMask.IsValid(1));
            Assert.True(col.ValidityMask.IsValid(2));
        }

        [Fact]
        public async System.Threading.Tasks.Task FillNull_AllNulls_Forward_RemainsAllNull()
        {
            var s = new Int32Series("v", 3);
            s.ValidityMask.SetNull(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetNull(2);

            var result = await new DataFrame(new[] { s })
                .Lazy().Select(Expr.Col("v").FillNull(FillStrategy.Forward)).Collect();

            var col = result.GetColumn("v");
            Assert.True(col.ValidityMask.IsNull(0));
            Assert.True(col.ValidityMask.IsNull(1));
            Assert.True(col.ValidityMask.IsNull(2));
        }

        // ─────────────────────────────────────────────────────────────────────
        // APACHE ARROW ROUND-TRIP
        // ─────────────────────────────────────────────────────────────────────

        [Fact]
        public void Arrow_RoundTrip_Int32()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 10, 20, 30 })
            });

            var batch = df.ToArrow();
            var df2 = DataFrame.FromArrow(batch);

            Assert.Equal(3, df2.RowCount);
            Assert.Equal(10, df2.GetColumn("a").Get(0));
            Assert.Equal(20, df2.GetColumn("a").Get(1));
            Assert.Equal(30, df2.GetColumn("a").Get(2));
        }

        [Fact]
        public void Arrow_RoundTrip_Float64()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Float64Series("f", new[] { 1.1, 2.2, 3.3 })
            });

            var batch = df.ToArrow();
            var df2 = DataFrame.FromArrow(batch);

            Assert.Equal(1.1, (double)df2.GetColumn("f").Get(0)!, 10);
            Assert.Equal(3.3, (double)df2.GetColumn("f").Get(2)!, 10);
        }

        [Fact]
        public void Arrow_RoundTrip_Utf8()
        {
            var df = new DataFrame(new ISeries[]
            {
                Utf8StringSeries.FromStrings("s", new string?[] { "alpha", "beta", "gamma" })
            });

            var batch = df.ToArrow();
            var df2 = DataFrame.FromArrow(batch);

            Assert.Equal("alpha", df2.GetColumn("s").Get(0));
            Assert.Equal("gamma", df2.GetColumn("s").Get(2));
        }

        [Fact]
        public void Arrow_RoundTrip_Boolean()
        {
            var df = new DataFrame(new ISeries[]
            {
                new BooleanSeries("b", new[] { true, false, true })
            });

            var batch = df.ToArrow();
            var df2 = DataFrame.FromArrow(batch);

            Assert.Equal(true, df2.GetColumn("b").Get(0));
            Assert.Equal(false, df2.GetColumn("b").Get(1));
        }

        [Fact]
        public void Arrow_RoundTrip_NullsPreserved()
        {
            // Python: pa.array([1, None, 3]) → null at index 1 survives round-trip
            var s = new Int32Series("a", 3);
            s.Memory.Span[0] = 1;
            s.Memory.Span[2] = 3;
            s.ValidityMask.SetValid(0);
            s.ValidityMask.SetNull(1);
            s.ValidityMask.SetValid(2);

            var df = new DataFrame(new[] { s });
            var df2 = DataFrame.FromArrow(df.ToArrow());

            Assert.Equal(1, df2.GetColumn("a").Get(0));
            Assert.Null(df2.GetColumn("a").Get(1));
            Assert.Equal(3, df2.GetColumn("a").Get(2));
        }

        [Fact]
        public void Arrow_RoundTrip_MultiColumn()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1, 2, 3 }),
                new Float64Series("score", new[] { 0.5, 0.75, 1.0 }),
                Utf8StringSeries.FromStrings("label", new string?[] { "low", "med", "high" })
            });

            var batch = df.ToArrow();
            Assert.Equal(3, batch.ColumnCount);
            Assert.Equal(3, batch.Length);

            var df2 = DataFrame.FromArrow(batch);
            Assert.Equal(df.RowCount, df2.RowCount);
            Assert.Equal(3, df2.Columns.Count());
            Assert.Equal(2, df2.GetColumn("id").Get(1));
            Assert.Equal("med", df2.GetColumn("label").Get(1));
        }

        [Fact]
        public void Arrow_Schema_ColumnNamesPreserved()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("alpha", new[] { 1 }),
                new Int32Series("beta",  new[] { 2 }),
                new Int32Series("gamma", new[] { 3 }),
            });

            var batch = df.ToArrow();
            Assert.Equal("alpha", batch.Schema.GetFieldByIndex(0).Name);
            Assert.Equal("beta",  batch.Schema.GetFieldByIndex(1).Name);
            Assert.Equal("gamma", batch.Schema.GetFieldByIndex(2).Name);
        }

        [Fact]
        public void Arrow_ToArrowArray_AllSeriesTypes()
        {
            // Verify ToArrowArray() doesn't throw for all major types
            ISeries[] series = new ISeries[]
            {
                new Int32Series("i32", new[] { 1 }),
                new Float64Series("f64", new[] { 1.0 }),
                new BooleanSeries("bl", new[] { true }),
                Utf8StringSeries.FromStrings("str", new string?[] { "x" }),
            };

            foreach (var s in series)
            {
                var arr = s.ToArrowArray();
                Assert.NotNull(arr);
                Assert.Equal(1, arr.Length);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // ARRAY SERIES (fixed-size list)
        // ─────────────────────────────────────────────────────────────────────

        [Fact]
        public void ArraySeries_Get_ReturnsCorrectRow()
        {
            // Width=3: row 0 = [1,2,3], row 1 = [4,5,6]
            var flat = new Int32Series("flat", new[] { 1, 2, 3, 4, 5, 6 });
            var arr = new ArraySeries("features", width: 3, values: flat);

            Assert.Equal(2, arr.Length);
            Assert.Equal(3, arr.Width);

            var row0 = (object?[])arr.Get(0)!;
            Assert.Equal(1, row0[0]);
            Assert.Equal(2, row0[1]);
            Assert.Equal(3, row0[2]);

            var row1 = (object?[])arr.Get(1)!;
            Assert.Equal(4, row1[0]);
        }

        [Fact]
        public void ArraySeries_ToArrowArray_ProducesFixedSizeList()
        {
            var flat = new Int32Series("flat", new[] { 1, 2, 3, 4, 5, 6 });
            var arr = new ArraySeries("features", width: 3, values: flat);

            var arrowArr = arr.ToArrowArray();
            Assert.NotNull(arrowArr);
            Assert.Equal(2, arrowArr.Length);
            Assert.IsType<FixedSizeListArray>(arrowArr);
        }

        [Fact]
        public void ArraySeries_CloneEmpty_HasCorrectDimensions()
        {
            var flat = new Int32Series("flat", new[] { 1, 2 });
            var arr = new ArraySeries("v", width: 2, values: flat);

            var clone = (ArraySeries)arr.CloneEmpty(5);
            Assert.Equal(2, clone.Width);
        }

        [Fact]
        public void ArraySeries_ValidityMask_NullRows()
        {
            var flat = new Int32Series("flat", new[] { 1, 2, 3, 4 });
            var arr = new ArraySeries("v", width: 2, values: flat);
            arr.ValidityMask.SetNull(1);

            Assert.True(arr.ValidityMask.IsValid(0));
            Assert.True(arr.ValidityMask.IsNull(1));
            Assert.Null(arr.Get(1));
        }
    }
}
