using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class PredicateErgonomicsTests
    {
        [Fact]
        public void Test_All_And_Any_BooleanSeries()
        {
            var sTrue = new BooleanSeries("b1", new[] { true, true, true });
            var sFalse = new BooleanSeries("b2", new[] { true, false, true });
            var sAllFalse = new BooleanSeries("b3", new[] { false, false });

            Assert.True((bool)((BooleanSeries)sTrue.All()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sFalse.All()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sAllFalse.All()).Memory.Span[0]);

            Assert.True((bool)((BooleanSeries)sTrue.Any()).Memory.Span[0]);
            Assert.True((bool)((BooleanSeries)sFalse.Any()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sAllFalse.Any()).Memory.Span[0]);

            // Test null handling
            var sWithNull = new BooleanSeries("b_null", 3);
            sWithNull.Memory.Span[0] = true;
            sWithNull.Memory.Span[1] = true;
            sWithNull.ValidityMask.SetNull(2); // [true, true, null]

            Assert.True((bool)((BooleanSeries)sWithNull.All()).Memory.Span[0]);
            Assert.True((bool)((BooleanSeries)sWithNull.Any()).Memory.Span[0]);

            var sFalseWithNull = new BooleanSeries("b_fn", 3);
            sFalseWithNull.Memory.Span[0] = false;
            sFalseWithNull.Memory.Span[1] = false;
            sFalseWithNull.ValidityMask.SetNull(2); // [false, false, null]

            Assert.False((bool)((BooleanSeries)sFalseWithNull.All()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sFalseWithNull.Any()).Memory.Span[0]);
        }

        [Fact]
        public void Test_All_And_Any_NumericSeries()
        {
            var sNonZero = new Int32Series("nz", new[] { 1, 2, 3 });
            var sWithZero = new Int32Series("wz", new[] { 1, 0, 3 });
            var sAllZero = new Int32Series("az", new[] { 0, 0, 0 });

            Assert.True((bool)((BooleanSeries)sNonZero.All()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sWithZero.All()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sAllZero.All()).Memory.Span[0]);

            Assert.True((bool)((BooleanSeries)sNonZero.Any()).Memory.Span[0]);
            Assert.True((bool)((BooleanSeries)sWithZero.Any()).Memory.Span[0]);
            Assert.False((bool)((BooleanSeries)sAllZero.Any()).Memory.Span[0]);
        }

        [Fact]
        public async Task Test_All_And_Any_LazyFrameExpressions()
        {
            var df = new DataFrame(new ISeries[]
            {
                new BooleanSeries("flags", new[] { true, true, false, true })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("flags").All().Alias("all_flags"),
                    Expr.Col("flags").Any().Alias("any_flags")
                )
                .Collect();

            Assert.Equal(1, res.RowCount);
            Assert.False((bool)((BooleanSeries)res.GetColumn("all_flags")).Memory.Span[0]);
            Assert.True((bool)((BooleanSeries)res.GetColumn("any_flags")).Memory.Span[0]);
        }

        [Fact]
        public void Test_GroupBy_All_And_Any()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("group", new[] { "A", "A", "B", "B" }),
                new BooleanSeries("val", new[] { true, true, true, false })
            });

            var agg = df.GroupBy("group").Agg(
                ("val", "all"),
                ("val", "any")
            );

            Assert.Equal(2, agg.RowCount);
            var allCol = (BooleanSeries)agg.GetColumn("val_all");
            var anyCol = (BooleanSeries)agg.GetColumn("val_any");

            // Group A: [true, true] -> all=true, any=true
            Assert.True(allCol.Memory.Span[0]);
            Assert.True(anyCol.Memory.Span[0]);

            // Group B: [true, false] -> all=false, any=true
            Assert.False(allCol.Memory.Span[1]);
            Assert.True(anyCol.Memory.Span[1]);
        }

        [Fact]
        public async Task Test_IsIn_Literals_And_Expr()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("nums", new[] { 10, 20, 30, 40, 50 }),
                new Utf8StringSeries("tags", new[] { "alpha", "beta", "gamma", "delta", "alpha" })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("nums").IsIn(20, 40, 99).Alias("in_nums"),
                    Expr.Col("tags").IsIn("alpha", "delta").Alias("in_tags")
                )
                .Collect();

            var inNums = (BooleanSeries)res.GetColumn("in_nums");
            var inTags = (BooleanSeries)res.GetColumn("in_tags");

            Assert.Equal(new[] { false, true, false, true, false }, inNums.Memory.Span.ToArray());
            Assert.Equal(new[] { true, false, false, true, true }, inTags.Memory.Span.ToArray());
        }

        [Fact]
        public async Task Test_IsBetween_All_Closed_Variants()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("x", new[] { 1, 2, 3, 4, 5 })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("x").IsBetween(2, 4, "both").Alias("both"),
                    Expr.Col("x").IsBetween(2, 4, "left").Alias("left"),
                    Expr.Col("x").IsBetween(2, 4, "right").Alias("right"),
                    Expr.Col("x").IsBetween(2, 4, "none").Alias("none")
                )
                .Collect();

            Assert.Equal(new[] { false, true, true, true, false }, ((BooleanSeries)res.GetColumn("both")).Memory.Span.ToArray());
            Assert.Equal(new[] { false, true, true, false, false }, ((BooleanSeries)res.GetColumn("left")).Memory.Span.ToArray());
            Assert.Equal(new[] { false, false, true, true, false }, ((BooleanSeries)res.GetColumn("right")).Memory.Span.ToArray());
            Assert.Equal(new[] { false, false, true, false, false }, ((BooleanSeries)res.GetColumn("none")).Memory.Span.ToArray());
        }

        [Fact]
        public async Task Test_Float_Predicates_Nan_And_Infinity()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Float64Series("val", new[] { 1.5, double.NaN, double.PositiveInfinity, double.NegativeInfinity, 42.0 })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("val").IsNan().Alias("is_nan"),
                    Expr.Col("val").IsNotNan().Alias("is_not_nan"),
                    Expr.Col("val").IsFinite().Alias("is_finite"),
                    Expr.Col("val").IsInfinite().Alias("is_infinite")
                )
                .Collect();

            Assert.Equal(new[] { false, true, false, false, false }, ((BooleanSeries)res.GetColumn("is_nan")).Memory.Span.ToArray());
            Assert.Equal(new[] { true, false, true, true, true }, ((BooleanSeries)res.GetColumn("is_not_nan")).Memory.Span.ToArray());
            Assert.Equal(new[] { true, false, false, false, true }, ((BooleanSeries)res.GetColumn("is_finite")).Memory.Span.ToArray());
            Assert.Equal(new[] { false, false, true, true, false }, ((BooleanSeries)res.GetColumn("is_infinite")).Memory.Span.ToArray());
        }

        [Fact]
        public void Test_DataFrame_Drop_Columns()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 1, 2 }),
                new Int32Series("b", new[] { 3, 4 }),
                new Int32Series("c", new[] { 5, 6 })
            });

            var droppedB = df.Drop("b");
            Assert.Equal(2, droppedB.Columns.Count);
            Assert.Equal(new[] { "a", "c" }, droppedB.Columns.Select(c => c.Name).ToArray());

            var droppedAC = df.Drop("a", "c");
            Assert.Single(droppedAC.Columns);
            Assert.Equal("b", droppedAC.Columns[0].Name);
        }

        [Fact]
        public void Test_DataFrame_WithColumns_Eager()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("x", new[] { 10, 20, 30 })
            });

            var enriched = df.WithColumns(
                (Expr.Col("x") * 2).Alias("x2"),
                (Expr.Col("x") + 5).Alias("x_plus_5")
            );

            Assert.Equal(3, enriched.Columns.Count);
            Assert.Equal(3, enriched.RowCount);
            Assert.Equal(new[] { 20, 40, 60 }, ((Int32Series)enriched.GetColumn("x2")).Memory.Span.ToArray());
            Assert.Equal(new[] { 15, 25, 35 }, ((Int32Series)enriched.GetColumn("x_plus_5")).Memory.Span.ToArray());
        }

        [Fact]
        public void Test_DataFrame_VStack_And_HStack()
        {
            var df1 = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1, 2 }),
                new Float64Series("val", new[] { 1.1, 2.2 })
            });

            var df2 = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 3, 4 }),
                new Float64Series("val", new[] { 3.3, 4.4 })
            });

            // VStack
            var vstacked = df1.VStack(df2);
            Assert.Equal(4, vstacked.RowCount);
            Assert.Equal(new[] { 1, 2, 3, 4 }, ((Int32Series)vstacked.GetColumn("id")).Memory.Span.ToArray());

            // HStack
            var dfExtra = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("status", new[] { "ok", "pending" })
            });

            var hstacked = df1.HStack(dfExtra);
            Assert.Equal(3, hstacked.Columns.Count);
            Assert.Equal(2, hstacked.RowCount);
            Assert.Equal("ok", ((Utf8StringSeries)hstacked.GetColumn("status")).GetString(0));
            Assert.Equal("pending", ((Utf8StringSeries)hstacked.GetColumn("status")).GetString(1));
        }

        [Fact]
        public void Test_DataFrame_PartitionBy()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Utf8StringSeries("category", new[] { "X", "Y", "X", "Z", "Y" }),
                new Int32Series("score", new[] { 10, 20, 30, 40, 50 })
            });

            var partitions = df.PartitionBy("category");
            Assert.Equal(3, partitions.Count);

            // Category X has 2 items
            var xPart = partitions.First(p => ((Utf8StringSeries)p.GetColumn("category")).GetString(0) == "X");
            Assert.Equal(2, xPart.RowCount);
            Assert.Equal(new[] { 10, 30 }, ((Int32Series)xPart.GetColumn("score")).Memory.Span.ToArray());

            // Dictionary partition
            var dict = df.PartitionByDict("category");
            Assert.Equal(3, dict.Count);
            Assert.True(dict.ContainsKey("X"));
            Assert.True(dict.ContainsKey("Y"));
            Assert.True(dict.ContainsKey("Z"));
            Assert.Equal(2, dict["Y"].RowCount);
        }

        [Fact]
        public async Task Test_Math_Sign_Pow_Log1p_Cbrt()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Float64Series("val", new[] { -10.0, 0.0, 8.0, 27.0 }),
                new Float64Series("exp", new[] { 2.0, 3.0, 0.5, 2.0 }),
                new Int32Series("int_val", new[] { -50, 0, 100, -1 })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("val").Sign().Alias("sign_f64"),
                    Expr.Col("int_val").Sign().Alias("sign_i32"),
                    Expr.Col("val").Pow(2.0).Alias("val_sq"),
                    Expr.Col("val").Pow(Expr.Col("exp")).Alias("val_pow_series"),
                    Expr.Col("val").Log1p().Alias("log1p_val"),
                    Expr.Col("val").Cbrt().Alias("cbrt_val")
                )
                .Collect();

            var signF64 = (Int32Series)res.GetColumn("sign_f64");
            Assert.Equal(-1, signF64.Memory.Span[0]);
            Assert.Equal(0, signF64.Memory.Span[1]);
            Assert.Equal(1, signF64.Memory.Span[2]);

            var signI32 = (Int32Series)res.GetColumn("sign_i32");
            Assert.Equal(-1, signI32.Memory.Span[0]);
            Assert.Equal(0, signI32.Memory.Span[1]);
            Assert.Equal(1, signI32.Memory.Span[2]);

            var valSq = (Float64Series)res.GetColumn("val_sq");
            Assert.Equal(100.0, valSq.Memory.Span[0]);
            Assert.Equal(0.0, valSq.Memory.Span[1]);
            Assert.Equal(64.0, valSq.Memory.Span[2]);

            var cbrtVal = (Float64Series)res.GetColumn("cbrt_val");
            Assert.Equal(2.0, cbrtVal.Memory.Span[2], 5);
            Assert.Equal(3.0, cbrtVal.Memory.Span[3], 5);

            var log1pVal = (Float64Series)res.GetColumn("log1p_val");
            Assert.Equal(0.0, log1pVal.Memory.Span[1], 5);
        }

        [Fact]
        public async Task Test_Math_Dot_Product()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Float64Series("a", new[] { 1.0, 2.0, 3.0 }),
                new Float64Series("b", new[] { 4.0, 5.0, 6.0 })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("a").Dot(Expr.Col("b")).Alias("dot_prod")
                )
                .Collect();

            var dotCol = (Float64Series)res.GetColumn("dot_prod");
            Assert.Equal(1, dotCol.Length);
            // 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
            Assert.Equal(32.0, dotCol.Memory.Span[0]);
        }

        [Fact]
        public async Task Test_Coalesce_Expressions()
        {
            var s1 = new Float64Series("c1", 4);
            s1.Memory.Span[0] = 100.0;
            s1.ValidityMask.SetNull(1);
            s1.ValidityMask.SetNull(2);
            s1.ValidityMask.SetNull(3);

            var s2 = new Float64Series("c2", 4);
            s2.ValidityMask.SetNull(0);
            s2.Memory.Span[1] = 200.0;
            s2.ValidityMask.SetNull(2);
            s2.ValidityMask.SetNull(3);

            var s3 = new Float64Series("c3", new[] { 999.0, 999.0, 300.0, 400.0 });

            var df = new DataFrame(new ISeries[] { s1, s2, s3 });

            var res = await df.Lazy()
                .Select(
                    Expr.Coalesce(Expr.Col("c1"), Expr.Col("c2"), Expr.Col("c3")).Alias("coalesced")
                )
                .Collect();

            var coalesced = (Float64Series)res.GetColumn("coalesced");
            Assert.Equal(4, coalesced.Length);
            Assert.Equal(100.0, coalesced.Memory.Span[0]);
            Assert.Equal(200.0, coalesced.Memory.Span[1]);
            Assert.Equal(300.0, coalesced.Memory.Span[2]);
            Assert.Equal(400.0, coalesced.Memory.Span[3]);
        }

        [Fact]
        public async Task Test_Cumulative_Expressions()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Float64Series("x", new[] { 10.0, 20.0, 5.0, 30.0 })
            });

            var res = await df.Lazy()
                .Select(
                    Expr.Col("x").CumSum().Alias("cum_sum"),
                    Expr.Col("x").CumMin().Alias("cum_min"),
                    Expr.Col("x").CumMax().Alias("cum_max"),
                    Expr.Col("x").CumProd().Alias("cum_prod"),
                    Expr.Col("x").CumCount().Alias("cum_count")
                )
                .Collect();

            var cumSum = (Float64Series)res.GetColumn("cum_sum");
            Assert.Equal(new[] { 10.0, 30.0, 35.0, 65.0 }, cumSum.Memory.Span.ToArray());

            var cumMin = (Float64Series)res.GetColumn("cum_min");
            Assert.Equal(new[] { 10.0, 10.0, 5.0, 5.0 }, cumMin.Memory.Span.ToArray());

            var cumMax = (Float64Series)res.GetColumn("cum_max");
            Assert.Equal(new[] { 10.0, 20.0, 20.0, 30.0 }, cumMax.Memory.Span.ToArray());

            var cumProd = (Float64Series)res.GetColumn("cum_prod");
            Assert.Equal(new[] { 10.0, 200.0, 1000.0, 30000.0 }, cumProd.Memory.Span.ToArray());

            var cumCount = (Int32Series)res.GetColumn("cum_count");
            Assert.Equal(new[] { 1, 2, 3, 4 }, cumCount.Memory.Span.ToArray());
        }

        [Fact]
        public void Test_Series_Ergonomics_And_Slicing()
        {
            var s = new Float64Series("val", new[] { -5.55, 2.22, 10.88, -1.0 });

            // Abs
            var absS = (Float64Series)s.Abs();
            Assert.Equal(5.55, absS.Memory.Span[0], 2);

            // Round
            var roundS = (Float64Series)s.Round(1);
            Assert.Equal(-5.6, roundS.Memory.Span[0], 1);
            Assert.Equal(2.2, roundS.Memory.Span[1], 1);

            // Head and Tail
            var head2 = (Float64Series)s.Head(2);
            Assert.Equal(2, head2.Length);
            Assert.Equal(-5.55, head2.Memory.Span[0]);
            Assert.Equal(2.22, head2.Memory.Span[1]);

            var tail2 = (Float64Series)s.Tail(2);
            Assert.Equal(2, tail2.Length);
            Assert.Equal(10.88, tail2.Memory.Span[0]);
            Assert.Equal(-1.0, tail2.Memory.Span[1]);

            // Shift and Diff
            var shifted = (Float64Series)s.Shift(1);
            Assert.Equal(4, shifted.Length);
            Assert.True(shifted.ValidityMask.IsNull(0));
            Assert.Equal(-5.55, shifted.Memory.Span[1]);

            var diff = (Float64Series)s.Diff(1);
            Assert.Equal(4, diff.Length);
            Assert.True(diff.ValidityMask.IsNull(0));

            // Cumulative
            var csum = (Float64Series)s.CumSum();
            Assert.Equal(-5.55, csum.Memory.Span[0], 2);
            Assert.Equal(-3.33, csum.Memory.Span[1], 2);
        }

        [Fact]
        public void Test_DataFrame_Head_Eager()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1, 2, 3, 4, 5, 6, 7 }),
                new Float64Series("val", new[] { 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0 })
            });

            var h3 = df.Head(3);
            Assert.Equal(3, h3.RowCount);
            Assert.Equal(new[] { 1, 2, 3 }, ((Int32Series)h3.GetColumn("id")).Memory.Span.ToArray());

            var defaultHead = df.Head();
            Assert.Equal(5, defaultHead.RowCount);
            Assert.Equal(new[] { 1, 2, 3, 4, 5 }, ((Int32Series)defaultHead.GetColumn("id")).Memory.Span.ToArray());
            Assert.Equal(new[] { "id", "val" }, df.ColumnNames);
        }

        [Fact]
        public async Task Test_LazyFrame_Head()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1, 2, 3, 4, 5, 6, 7 }),
                new Float64Series("val", new[] { 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0 })
            });

            var res = await df.Lazy().Head(4).Collect();
            Assert.Equal(4, res.RowCount);
            Assert.Equal(new[] { 1, 2, 3, 4 }, ((Int32Series)res.GetColumn("id")).Memory.Span.ToArray());
        }
    }
}



