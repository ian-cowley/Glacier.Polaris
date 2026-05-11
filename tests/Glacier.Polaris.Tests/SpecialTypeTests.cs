using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    // ─────────────────────────────────────────────────────────────────────────
    // Special Data Type Tests: Decimal, Object, Null
    // ─────────────────────────────────────────────────────────────────────────

    public class SpecialTypeTests
    {
        // ── DecimalSeries ─────────────────────────────────────────────────────

        [Fact]
        public void DecimalSeries_Construction_StoresValues()
        {
            var values = new decimal?[] { 1.5m, 2.75m, null, 100.001m };
            var s = new DecimalSeries("price", values);

            Assert.Equal(4, s.Length);
            Assert.Equal(1.5m,     s.GetValue(0));
            Assert.Equal(2.75m,    s.GetValue(1));
            Assert.Null(s.GetValue(2));
            Assert.Equal(100.001m, s.GetValue(3));
        }

        [Fact]
        public void DecimalSeries_ValidityMask_ReflectsNulls()
        {
            var s = new DecimalSeries("d", new decimal?[] { 1m, null, 3m });

            Assert.True(s.ValidityMask.IsValid(0));
            Assert.True(s.ValidityMask.IsNull(1));
            Assert.True(s.ValidityMask.IsValid(2));
        }

        [Fact]
        public void DecimalSeries_Get_ReturnsBoxedDecimal()
        {
            var s = new DecimalSeries("d", new decimal?[] { 42.5m, null });

            Assert.Equal(42.5m, (decimal)s.Get(0)!);
            Assert.Null(s.Get(1));
        }

        [Fact]
        public void DecimalSeries_ArrowRoundTrip_PreservesValues()
        {
            var original = new DecimalSeries("amount", new decimal?[] { 1.23m, null, 456.789m }, precision: 18, scale: 3);

            var arrowArray = original.ToArrowArray();
            Assert.NotNull(arrowArray);

            // Round-trip through DataFrame Arrow interop
            var df = new DataFrame(new List<ISeries> { original });
            var batch = df.ToArrowRecordBatch();
            var df2 = DataFrame.FromArrow(batch);

            var s2 = (DecimalSeries)df2.GetColumn("amount")!;
            Assert.Equal(3, s2.Length);
            Assert.Equal(1.23m,    s2.GetValue(0));
            Assert.Null(s2.GetValue(1));
            Assert.Equal(456.789m, s2.GetValue(2));
        }

        [Fact]
        public async Task DecimalSeries_LazyArithmetic_AddTwoDecimalColumns()
        {
            var a = new DecimalSeries("a", new decimal?[] { 1.0m, 2.0m, 3.0m });
            var b = new DecimalSeries("b", new decimal?[] { 0.5m, 1.5m, 2.5m });
            var df = new DataFrame(new List<ISeries> { a, b });

            var result = await LazyFrame.FromDataFrame(df)
                .Select(Expr.Col("a") + Expr.Col("b"))
                .Collect();

            var col = (DecimalSeries)result.Columns[0];
            Assert.Equal(1.5m, col.GetValue(0));
            Assert.Equal(3.5m, col.GetValue(1));
            Assert.Equal(5.5m, col.GetValue(2));
        }

        [Fact]
        public async Task DecimalSeries_NullPropagation_ArithmeticWithNull()
        {
            var a = new DecimalSeries("a", new decimal?[] { 10m, null, 30m });
            var b = new DecimalSeries("b", new decimal?[] { 1m,  2m,   3m  });
            var df = new DataFrame(new List<ISeries> { a, b });

            var result = await LazyFrame.FromDataFrame(df)
                .Select(Expr.Col("a") + Expr.Col("b"))
                .Collect();

            var col = (DecimalSeries)result.Columns[0];
            Assert.Equal(11m, col.GetValue(0));
            Assert.Null(col.GetValue(1));   // null propagated
            Assert.Equal(33m, col.GetValue(2));
        }

        [Fact]
        public async Task DecimalLiteral_InSelect_BroadcastsCorrectly()
        {
            var a = new DecimalSeries("a", new decimal?[] { 1m, 2m, 3m });
            var df = new DataFrame(new List<ISeries> { a });

            var result = await LazyFrame.FromDataFrame(df)
                .Select(Expr.Col("a") * Expr.Lit(10m))
                .Collect();

            var col = (DecimalSeries)result.Columns[0];
            Assert.Equal(10m, col.GetValue(0));
            Assert.Equal(20m, col.GetValue(1));
            Assert.Equal(30m, col.GetValue(2));
        }

        // ── ObjectSeries ──────────────────────────────────────────────────────

        [Fact]
        public void ObjectSeries_Construction_StoresMixedTypes()
        {
            var values = new object?[] { 42, "hello", 3.14, null, true };
            var s = new ObjectSeries("obj", values);

            Assert.Equal(5, s.Length);
            Assert.Equal(42,      s.GetValue(0));
            Assert.Equal("hello", s.GetValue(1));
            Assert.Equal(3.14,    s.GetValue(2));
            Assert.Null(s.GetValue(3));
            Assert.Equal(true,    s.GetValue(4));
        }

        [Fact]
        public void ObjectSeries_ValidityMask_ReflectsNulls()
        {
            var s = new ObjectSeries("o", new object?[] { 1, null, "x" });

            Assert.True(s.ValidityMask.IsValid(0));
            Assert.True(s.ValidityMask.IsNull(1));
            Assert.True(s.ValidityMask.IsValid(2));
        }

        [Fact]
        public void ObjectSeries_CloneEmpty_HasCorrectLength()
        {
            var s = new ObjectSeries("o", new object?[] { 1, 2, 3 });
            var clone = (ObjectSeries)s.CloneEmpty(5);

            Assert.Equal(5, clone.Length);
            Assert.Equal("o", clone.Name);
        }

        [Fact]
        public void ObjectSeries_ToArrowArray_SerialisesAsStrings()
        {
            var s = new ObjectSeries("o", new object?[] { 42, "hi", null });
            var arr = s.ToArrowArray();

            Assert.NotNull(arr);
            Assert.Equal(3, arr.Length);
            // Arrow type will be StringArray (lossy serialisation)
            Assert.IsType<Apache.Arrow.StringArray>(arr);
            var sa = (Apache.Arrow.StringArray)arr;
            Assert.Equal("42", sa.GetString(0));
            Assert.Equal("hi", sa.GetString(1));
            Assert.True(sa.IsNull(2));
        }

        [Fact]
        public void ObjectSeries_Take_CopiesCorrectValues()
        {
            var s = new ObjectSeries("o", new object?[] { "a", "b", "c", "d" });
            var target = (ObjectSeries)s.CloneEmpty(2);
            s.Take(target, new ReadOnlySpan<int>(new[] { 0, 2 }));

            Assert.Equal("a", target.GetValue(0));
            Assert.Equal("c", target.GetValue(1));
        }

        // ── NullSeries ────────────────────────────────────────────────────────

        [Fact]
        public void NullSeries_AllEntriesAreNull()
        {
            var s = new NullSeries("n", 5);

            Assert.Equal(5, s.Length);
            for (int i = 0; i < 5; i++)
            {
                Assert.True(s.ValidityMask.IsNull(i));
                Assert.Null(s.Get(i));
            }
        }

        [Fact]
        public void NullSeries_ArrowRoundTrip_ProducesNullArray()
        {
            var s = new NullSeries("n", 4);
            var arr = s.ToArrowArray();

            Assert.NotNull(arr);
            Assert.IsType<Apache.Arrow.NullArray>(arr);
            Assert.Equal(4, arr.Length);
        }

        [Fact]
        public void NullSeries_ArrowInterop_FromArrow_RoundTrips()
        {
            var s = new NullSeries("null_col", 3);
            var df = new DataFrame(new List<ISeries> { s });
            var batch = df.ToArrowRecordBatch();
            var df2 = DataFrame.FromArrow(batch);

            var s2 = df2.GetColumn("null_col");
            Assert.NotNull(s2);
            Assert.IsType<NullSeries>(s2);
            Assert.Equal(3, s2!.Length);
            for (int i = 0; i < 3; i++)
                Assert.Null(s2.Get(i));
        }

        [Fact]
        public async Task NullSeries_InDataFrame_FilterReturnsEmptyResults()
        {
            // A NullSeries alongside a real column — filter on the real column still works
            var ids    = new Int32Series("id",  new[] { 1, 2, 3 });
            var nulls  = new NullSeries("n", 3);
            var df     = new DataFrame(new List<ISeries> { ids, nulls });

            var result = await LazyFrame.FromDataFrame(df)
                .Filter(e => Expr.Col("id") > 1)
                .Collect();

            Assert.Equal(2, result.RowCount);
        }

        [Fact]
        public async Task NullSeries_Arithmetic_WithNonNull_PropagatesNull()
        {
            var a    = new Int32Series("a",  new[] { 1, 2, 3 });
            var nulls = new NullSeries("n", 3);
            var df   = new DataFrame(new List<ISeries> { a, nulls });

            var result = await LazyFrame.FromDataFrame(df)
                .Select(Expr.Col("a") + Expr.Col("n"))
                .Collect();

            // Null + anything = Null → result column should be NullSeries
            var col = result.Columns[0];
            Assert.NotNull(col);
            Assert.IsType<NullSeries>(col);
        }
    }
}
