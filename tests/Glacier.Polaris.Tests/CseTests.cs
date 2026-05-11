using System.Collections.Generic;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    /// <summary>
    /// Verifies that the Common Subexpression Elimination optimizer pass produces
    /// correct results (semantics are preserved; the computation happens or not is
    /// an internal detail we validate indirectly via correctness).
    /// </summary>
    public class CseTests
    {
        // ── helpers ──────────────────────────────────────────────────────────

        private static DataFrame MakeFrame(int[] a, int[] b)
        {
            var colA = new Int32Series("a", a);
            var colB = new Int32Series("b", b);
            return new DataFrame(new ISeries[] { colA, colB });
        }

        private static DataFrame MakeFrameDouble(double[] a, double[] b)
        {
            var colA = new Float64Series("a", a);
            var colB = new Float64Series("b", b);
            return new DataFrame(new ISeries[] { colA, colB });
        }

        // ── CSE correctness: same expression used twice ───────────────────────

        [Fact]
        public async Task Cse_DuplicateArithmetic_BothColumnsCorrect()
        {
            // (a + b) is computed twice; CSE should hoist it, but results must be identical
            var df = MakeFrame(new[] { 1, 2, 3 }, new[] { 10, 20, 30 });
            var lf = LazyFrame.FromDataFrame(df).Select(
                (Expr.Col("a") + Expr.Col("b")).Alias("sum1"),
                (Expr.Col("a") + Expr.Col("b")).Alias("sum2")
            );

            var result = await lf.Collect();
            Assert.Equal(3, result.RowCount);

            var sum1 = (Int32Series)result.GetColumn("sum1");
            var sum2 = (Int32Series)result.GetColumn("sum2");

            Assert.Equal(new[] { 11, 22, 33 }, sum1.Memory.ToArray());
            Assert.Equal(new[] { 11, 22, 33 }, sum2.Memory.ToArray());
        }

        [Fact]
        public async Task Cse_DuplicateArithmetic_TripleUse_AllColumnsCorrect()
        {
            // (a * b) used three times
            var df = MakeFrame(new[] { 2, 3, 4 }, new[] { 5, 6, 7 });
            var lf = LazyFrame.FromDataFrame(df).Select(
                (Expr.Col("a") * Expr.Col("b")).Alias("p1"),
                (Expr.Col("a") * Expr.Col("b")).Alias("p2"),
                (Expr.Col("a") * Expr.Col("b")).Alias("p3")
            );

            var result = await lf.Collect();
            var p1 = (Int32Series)result.GetColumn("p1");
            var p2 = (Int32Series)result.GetColumn("p2");
            var p3 = (Int32Series)result.GetColumn("p3");

            int[] expected = { 10, 18, 28 };
            Assert.Equal(expected, p1.Memory.ToArray());
            Assert.Equal(expected, p2.Memory.ToArray());
            Assert.Equal(expected, p3.Memory.ToArray());
        }

        // ── CSE in WithColumns ────────────────────────────────────────────────

        [Fact]
        public async Task Cse_WithColumns_DuplicateExprProducesCorrectValues()
        {
            var df = MakeFrame(new[] { 1, 2, 3 }, new[] { 4, 5, 6 });
            var lf = LazyFrame.FromDataFrame(df)
                .WithColumns(
                    (Expr.Col("a") + Expr.Col("b")).Alias("c"),
                    (Expr.Col("a") + Expr.Col("b")).Alias("d")
                );

            var result = await lf.Collect();
            var c = (Int32Series)result.GetColumn("c");
            var d = (Int32Series)result.GetColumn("d");

            Assert.Equal(new[] { 5, 7, 9 }, c.Memory.ToArray());
            Assert.Equal(new[] { 5, 7, 9 }, d.Memory.ToArray());
        }

        // ── No CSE for unique expressions ─────────────────────────────────────

        [Fact]
        public async Task Cse_UniqueExpressions_NotAffected()
        {
            // Different expressions — no CSE hoisting should occur, but results must be correct
            var df = MakeFrame(new[] { 3, 6, 9 }, new[] { 1, 2, 3 });
            var lf = LazyFrame.FromDataFrame(df).Select(
                (Expr.Col("a") + Expr.Col("b")).Alias("sum"),
                (Expr.Col("a") - Expr.Col("b")).Alias("diff"),
                (Expr.Col("a") * Expr.Col("b")).Alias("prod")
            );

            var result = await lf.Collect();
            Assert.Equal(new[] { 4, 8, 12 }, ((Int32Series)result.GetColumn("sum")).Memory.ToArray());
            Assert.Equal(new[] { 2, 4, 6  }, ((Int32Series)result.GetColumn("diff")).Memory.ToArray());
            Assert.Equal(new[] { 3, 12, 27 }, ((Int32Series)result.GetColumn("prod")).Memory.ToArray());
        }

        // ── CSE with double-precision arithmetic ──────────────────────────────

        [Fact]
        public async Task Cse_Float64_DuplicateExprIsCorrect()
        {
            var df = MakeFrameDouble(new[] { 1.5, 2.5, 3.5 }, new[] { 0.5, 1.5, 2.5 });
            var lf = LazyFrame.FromDataFrame(df).Select(
                (Expr.Col("a") + Expr.Col("b")).Alias("x"),
                (Expr.Col("a") + Expr.Col("b")).Alias("y")
            );

            var result = await lf.Collect();
            var x = ((Float64Series)result.GetColumn("x")).Memory.ToArray();
            var y = ((Float64Series)result.GetColumn("y")).Memory.ToArray();

            Assert.Equal(new[] { 2.0, 4.0, 6.0 }, x);
            Assert.Equal(new[] { 2.0, 4.0, 6.0 }, y);
        }

        // ── CSE with nested expressions ───────────────────────────────────────

        [Fact]
        public async Task Cse_NestedDuplicateExpr_ProducesCorrectResult()
        {
            // (a + b) * (a + b) — inner (a+b) used twice but CSE only hoists compound
            var df = MakeFrame(new[] { 1, 2, 3 }, new[] { 2, 3, 4 });
            var sum = Expr.Col("a") + Expr.Col("b");

            var lf = LazyFrame.FromDataFrame(df).Select(
                (sum * sum).Alias("squared_sum")
            );

            var result = await lf.Collect();
            var sq = (Int32Series)result.GetColumn("squared_sum");
            Assert.Equal(new[] { 9, 25, 49 }, sq.Memory.ToArray());
        }

        // ── Optimizer plan-rewrite smoke-test: temp column cleaned up ─────────

        [Fact]
        public async Task Cse_TempColumnsNotExposedToFinalOutput()
        {
            // The injected __cse_N columns should NOT appear in a Select result
            var df = MakeFrame(new[] { 1, 2 }, new[] { 3, 4 });
            var lf = LazyFrame.FromDataFrame(df).Select(
                (Expr.Col("a") + Expr.Col("b")).Alias("s1"),
                (Expr.Col("a") + Expr.Col("b")).Alias("s2")
            );

            var result = await lf.Collect();
            var colNames = result.Columns.Select(c => c.Name).ToList();

            // Must have s1, s2
            Assert.Contains("s1", colNames);
            Assert.Contains("s2", colNames);

            // Must NOT have leaked __cse_ temp columns
            Assert.DoesNotContain(colNames, n => n.StartsWith("__cse_"));
        }
    }
}
