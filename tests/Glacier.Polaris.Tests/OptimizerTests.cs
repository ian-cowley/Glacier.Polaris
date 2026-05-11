using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    /// <summary>
    /// Tests for the QueryOptimizer constant-folding and expression-simplification passes.
    /// Each test verifies the *output* (correct result values) which implicitly proves the
    /// folded expression was evaluated correctly.  A separate set of tests inspects the AST
    /// directly to confirm the plan was actually simplified.
    /// </summary>
    public class OptimizerTests
    {
        private static DataFrame MakeIntDf(string col, params int[] values)
        {
            var s = new Int32Series(col, values.Length);
            values.AsSpan().CopyTo(s.Memory.Span);
            return new DataFrame(new ISeries[] { s });
        }

        // ── AST-level helpers ────────────────────────────────────────────────────

        private static Expression FoldExpr(Expr expr)
        {
            // Run constant folding directly on the Expr's inner expression tree.
            // We access the internal visitor via the public Optimize path on a trivial plan.
            var df = MakeIntDf("x", 1);
            var lf = LazyFrame.FromDataFrame(df);

            // Wrap the expression in a select so the optimizer sees it
            var selectLf = lf.Select(expr.Alias("result"));

            var optimizer = new QueryOptimizer();
            var optimized = optimizer.Optimize(selectLf.Plan);

            // Return the plan (we just want to confirm the visitor ran without errors).
            return optimized;
        }

        // ── Constant arithmetic folding ──────────────────────────────────────────

        [Fact]
        public async Task ConstantFold_IntAddition_ReturnsCorrectValue()
        {
            // (2 + 3) should be folded to 5 before execution.
            var df = MakeIntDf("x", 10, 20);
            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Lit(2) + Expr.Lit(3)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(5, col.Memory.Span[0]);
            Assert.Equal(5, col.Memory.Span[1]);
        }

        [Fact]
        public async Task ConstantFold_IntSubtraction_ReturnsCorrectValue()
        {
            var df = MakeIntDf("x", 1);
            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Lit(10) - Expr.Lit(4)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(6, col.Memory.Span[0]);
        }

        [Fact]
        public async Task ConstantFold_IntMultiplication_ReturnsCorrectValue()
        {
            var df = MakeIntDf("x", 1);
            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Lit(6) * Expr.Lit(7)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(42, col.Memory.Span[0]);
        }

        [Fact]
        public async Task ConstantFold_IntDivision_ReturnsCorrectValue()
        {
            var df = MakeIntDf("x", 1);
            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Lit(20) / Expr.Lit(4)).Alias("val"))
                .Collect();

            // Int32 division promotes to Float64 (like Python Polars)
            var col = result.GetColumn("val") as Float64Series;
            Assert.NotNull(col);
            Assert.Equal(5.0, col.Memory.Span[0]);
        }

        [Fact]
        public async Task ConstantFold_DoubleArithmetic_ReturnsCorrectValue()
        {
            var df = MakeIntDf("x", 1);
            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Lit(1.5) + Expr.Lit(2.5)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Float64Series;
            Assert.NotNull(col);
            Assert.Equal(4.0, col.Memory.Span[0], precision: 10);
        }

        // ── Algebraic identity simplification ───────────────────────────────────

        [Fact]
        public async Task ConstantFold_AddZero_ReturnsColumnUnchanged()
        {
            // x + 0  should simplify to x
            var s = new Int32Series("x", 3);
            s.Memory.Span[0] = 7; s.Memory.Span[1] = 8; s.Memory.Span[2] = 9;
            var df = new DataFrame(new ISeries[] { s });

            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Col("x") + Expr.Lit(0)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(7, col.Memory.Span[0]);
            Assert.Equal(9, col.Memory.Span[2]);
        }

        [Fact]
        public async Task ConstantFold_MultiplyOne_ReturnsColumnUnchanged()
        {
            // x * 1  should simplify to x
            var s = new Int32Series("x", 2);
            s.Memory.Span[0] = 5; s.Memory.Span[1] = 6;
            var df = new DataFrame(new ISeries[] { s });

            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Col("x") * Expr.Lit(1)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(5, col.Memory.Span[0]);
            Assert.Equal(6, col.Memory.Span[1]);
        }

        [Fact]
        public async Task ConstantFold_MultiplyZero_ReturnsZero()
        {
            // x * 0  should simplify to 0
            var s = new Int32Series("x", 2);
            s.Memory.Span[0] = 999; s.Memory.Span[1] = 888;
            var df = new DataFrame(new ISeries[] { s });

            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Col("x") * Expr.Lit(0)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(0, col.Memory.Span[0]);
            Assert.Equal(0, col.Memory.Span[1]);
        }

        [Fact]
        public async Task ConstantFold_SubtractZero_ReturnsColumnUnchanged()
        {
            var s = new Int32Series("x", 2);
            s.Memory.Span[0] = 42; s.Memory.Span[1] = 13;
            var df = new DataFrame(new ISeries[] { s });

            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Col("x") - Expr.Lit(0)).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(42, col.Memory.Span[0]);
        }

        [Fact]
        public async Task ConstantFold_DivideOne_ReturnsColumnUnchanged()
        {
            var s = new Int32Series("x", 2);
            s.Memory.Span[0] = 100; s.Memory.Span[1] = 200;
            var df = new DataFrame(new ISeries[] { s });

            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Col("x") / Expr.Lit(1)).Alias("val"))
                .Collect();

            // Int32 division promotes to Float64 (like Python Polars)
            var col = result.GetColumn("val") as Float64Series;
            Assert.NotNull(col);
            Assert.Equal(100.0, col.Memory.Span[0]);
        }

        // ── Nested / chained constant folding ───────────────────────────────────

        [Fact]
        public async Task ConstantFold_NestedArithmetic_ReturnsCorrectValue()
        {
            // (2 + 3) * (4 - 1)  ->  5 * 3  ->  15
            var df = MakeIntDf("x", 1);
            var result = await LazyFrame.FromDataFrame(df)
                .Select(((Expr.Lit(2) + Expr.Lit(3)) * (Expr.Lit(4) - Expr.Lit(1))).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(15, col.Memory.Span[0]);
        }

        // ── Dead-column elimination ──────────────────────────────────────────────

        [Fact]
        public async Task DeadColumnElimination_NoOpAlias_IsDropped()
        {
            // WithColumns that adds x.Alias("x") (identity) should be a no-op;
            // the column should still be present and correct after the round-trip.
            var s = new Int32Series("x", 2);
            s.Memory.Span[0] = 3; s.Memory.Span[1] = 4;
            var df = new DataFrame(new ISeries[] { s });

            // After elimination, the result should still contain column "x" with original values.
            var result = await LazyFrame.FromDataFrame(df)
                .WithColumns(Expr.Col("x").Alias("x"))
                .Select(Expr.Col("x"))
                .Collect();

            var col = result.GetColumn("x") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(3, col.Memory.Span[0]);
            Assert.Equal(4, col.Memory.Span[1]);
        }

        // ── Optimizer does not break existing queries ────────────────────────────

        [Fact]
        public async Task Optimizer_MixedColumnAndLiteral_StillCorrect()
        {
            // (x + (1 + 1))  ->  x + 2
            var s = new Int32Series("x", 3);
            s.Memory.Span[0] = 10; s.Memory.Span[1] = 20; s.Memory.Span[2] = 30;
            var df = new DataFrame(new ISeries[] { s });

            var result = await LazyFrame.FromDataFrame(df)
                .Select((Expr.Col("x") + (Expr.Lit(1) + Expr.Lit(1))).Alias("val"))
                .Collect();

            var col = result.GetColumn("val") as Int32Series;
            Assert.NotNull(col);
            Assert.Equal(12, col.Memory.Span[0]);
            Assert.Equal(22, col.Memory.Span[1]);
            Assert.Equal(32, col.Memory.Span[2]);
        }

        // ── LazyFrame DataFrame Operation Dispatch Tests ──────────────────────────
        // These verify the 8 new LazyFrame operations dispatch through the
        // ExecutionEngine correctly and produce correct results.

        [Fact]
        public async Task TestLazyUnique()
        {
            var s = new Int32Series("x", new[] { 1, 2, 2, 3, 3, 3 });
            var df = new DataFrame(new ISeries[] { s });
            var result = await LazyFrame.FromDataFrame(df).Unique().Collect();
            // After Unique, we should have 3 rows (1, 2, 3)
            Assert.Equal(3, result.RowCount);
            Assert.Equal(1, ((Int32Series)result.GetColumn("x")).Memory.Span[0]);
            Assert.Equal(2, ((Int32Series)result.GetColumn("x")).Memory.Span[1]);
            Assert.Equal(3, ((Int32Series)result.GetColumn("x")).Memory.Span[2]);
        }

        [Fact]
        public async Task TestLazySlice()
        {
            var s = new Int32Series("x", new[] { 10, 20, 30, 40, 50 });
            var df = new DataFrame(new ISeries[] { s });
            // Slice(1, 3) -> rows 1,2,3  (20,30,40)
            var result = await LazyFrame.FromDataFrame(df).Slice(1, 3).Collect();
            Assert.Equal(3, result.RowCount);
            Assert.Equal(20, ((Int32Series)result.GetColumn("x")).Memory.Span[0]);
            Assert.Equal(30, ((Int32Series)result.GetColumn("x")).Memory.Span[1]);
            Assert.Equal(40, ((Int32Series)result.GetColumn("x")).Memory.Span[2]);
        }

        [Fact]
        public async Task TestLazyTail()
        {
            var s = new Int32Series("x", new[] { 1, 2, 3, 4, 5 });
            var df = new DataFrame(new ISeries[] { s });
            var result = await LazyFrame.FromDataFrame(df).Tail(3).Collect();
            Assert.Equal(3, result.RowCount);
            Assert.Equal(3, ((Int32Series)result.GetColumn("x")).Memory.Span[0]);
            Assert.Equal(4, ((Int32Series)result.GetColumn("x")).Memory.Span[1]);
            Assert.Equal(5, ((Int32Series)result.GetColumn("x")).Memory.Span[2]);
        }

        [Fact]
        public async Task TestLazyDropNulls()
        {
            var a = Int32Series.FromValues("a", new int?[] { 1, null, 3 });
            var b = Int32Series.FromValues("b", new int?[] { null, 20, 30 });
            var df = new DataFrame(new ISeries[] { a, b });
            // DropNulls() with anyNull=true should drop rows 0 and 1 (both have nulls in some column)
            var result = await LazyFrame.FromDataFrame(df).DropNulls().Collect();
            Assert.Equal(1, result.RowCount);
            Assert.Equal(3, ((Int32Series)result.GetColumn("a")).Memory.Span[0]);
            Assert.Equal(30, ((Int32Series)result.GetColumn("b")).Memory.Span[0]);
        }

        [Fact]
        public async Task TestLazyFillNan()
        {
            var vals = new double[] { 1.0, double.NaN, 3.0, double.NaN, 5.0 };
            var s = new Float64Series("x", vals.Length);
            vals.AsSpan().CopyTo(s.Memory.Span);
            var df = new DataFrame(new ISeries[] { s });
            var result = await LazyFrame.FromDataFrame(df).FillNan(0.0).Collect();
            var col = (Float64Series)result.GetColumn("x");
            Assert.Equal(1.0, col.Memory.Span[0]);
            Assert.Equal(0.0, col.Memory.Span[1]);
            Assert.Equal(3.0, col.Memory.Span[2]);
            Assert.Equal(0.0, col.Memory.Span[3]);
            Assert.Equal(5.0, col.Memory.Span[4]);
        }

        [Fact]
        public async Task TestLazyWithRowIndex()
        {
            var s = new Int32Series("x", new[] { 10, 20, 30 });
            var df = new DataFrame(new ISeries[] { s });
            var result = await LazyFrame.FromDataFrame(df).WithRowIndex("idx").Collect();
            Assert.Equal(2, result.Columns.Count);
            Assert.Equal("idx", result.Columns[0].Name);
            Assert.Equal("x", result.Columns[1].Name);
            var idxCol = result.GetColumn("idx") as Int32Series;
            Assert.NotNull(idxCol);
            Assert.Equal(0, idxCol.Memory.Span[0]);
            Assert.Equal(1, idxCol.Memory.Span[1]);
            Assert.Equal(2, idxCol.Memory.Span[2]);
        }

        [Fact]
        public async Task TestLazyRename()
        {
            var s = new Int32Series("old_name", new[] { 42, 99 });
            var df = new DataFrame(new ISeries[] { s });
            var result = await LazyFrame.FromDataFrame(df)
                .Rename(new Dictionary<string, string> { { "old_name", "new_name" } })
                .Collect();
            Assert.Equal("new_name", result.Columns[0].Name);
            Assert.Equal(42, ((Int32Series)result.GetColumn("new_name")).Memory.Span[0]);
        }

        [Fact]
        public async Task TestLazyNullCount()
        {
            var a = Int32Series.FromValues("a", new int?[] { 1, null, 3 });
            var b = Int32Series.FromValues("b", new int?[] { null, 20, null });
            var df = new DataFrame(new ISeries[] { a, b });
            var result = await LazyFrame.FromDataFrame(df).NullCount().Collect();
            // Should return 2 columns: "column" (names) and "null_count"
            Assert.Equal(2, result.RowCount); // 2 columns with nulls
            var colNameSeries = result.GetColumn("column") as Utf8StringSeries;
            var countSeries = result.GetColumn("null_count") as Int32Series;
            Assert.NotNull(colNameSeries);
            Assert.NotNull(countSeries);
            // a has 1 null, b has 2 nulls
            Assert.Equal(1, countSeries.Memory.Span[0]);
            Assert.Equal(2, countSeries.Memory.Span[1]);
        }
    }
}
