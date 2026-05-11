using System;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using System.Linq;

namespace Glacier.Polaris.Tests
{
    public class WindowAdvTests
    {
        [Fact]
        public void TestRollingSumInt32()
        {
            var df = new DataFrame(new ISeries[] { new Int32Series("a", new int[] { 1, 2, 3, 4, 5 }) });
            var result = df.Select(
                Expr.Col("a").RollingSum(3).Alias("rolling_sum")
            );

            var rollingCol = result.GetColumn("rolling_sum") as Int32Series;
            Assert.NotNull(rollingCol);
            
            // [null, null, 1+2+3=6, 2+3+4=9, 3+4+5=12]
            Assert.True(rollingCol.ValidityMask.IsNull(0));
            Assert.True(rollingCol.ValidityMask.IsNull(1));
            Assert.Equal(6, rollingCol.Get(2));
            Assert.Equal(9, rollingCol.Get(3));
            Assert.Equal(12, rollingCol.Get(4));
        }

        [Fact]
        public void TestRollingMeanFloat64()
        {
            var df = new DataFrame(new ISeries[] { new Float64Series("a", new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 }) });
            var result = df.Select(
                Expr.Col("a").RollingMean(3).Alias("rolling_mean")
            );

            var rollingCol = result.GetColumn("rolling_mean") as Float64Series;
            Assert.NotNull(rollingCol);
            
            // [null, null, 2.0, 3.0, 4.0]
            Assert.True(rollingCol.ValidityMask.IsNull(0));
            Assert.True(rollingCol.ValidityMask.IsNull(1));
            Assert.Equal(2.0, (double)rollingCol.Get(2)!);
            Assert.Equal(3.0, (double)rollingCol.Get(3)!);
            Assert.Equal(4.0, (double)rollingCol.Get(4)!);
        }

        [Fact]
        public void TestExpandingSum()
        {
            var df = new DataFrame(new ISeries[] { new Int32Series("a", new int[] { 1, 2, 3, 4, 5 }) });
            var result = df.Select(
                Expr.Col("a").ExpandingSum().Alias("expanding_sum")
            );

            var expandingCol = result.GetColumn("expanding_sum") as Int32Series;
            Assert.NotNull(expandingCol);
            
            // [1, 3, 6, 10, 15]
            Assert.Equal(1, expandingCol.Get(0));
            Assert.Equal(3, expandingCol.Get(1));
            Assert.Equal(6, expandingCol.Get(2));
            Assert.Equal(10, expandingCol.Get(3));
            Assert.Equal(15, expandingCol.Get(4));
        }

        [Fact]
        public void TestEWMMean()
        {
            var df = new DataFrame(new ISeries[] { new Float64Series("a", new double[] { 10.0, 20.0, 30.0 }) });
            // alpha = 0.5
            // e0 = 10
            // e1 = 0.5 * 20 + 0.5 * 10 = 15
            // e2 = 0.5 * 30 + 0.5 * 15 = 22.5
            var result = df.Select(
                Expr.Col("a").EWMMean(0.5).Alias("ewm")
            );

            var ewmCol = result.GetColumn("ewm") as Float64Series;
            Assert.NotNull(ewmCol);
            
            Assert.Equal(10.0, (double)ewmCol.Get(0)!);
            Assert.Equal(15.0, (double)ewmCol.Get(1)!);
            Assert.Equal(22.5, (double)ewmCol.Get(2)!);
        }

        [Fact]
        public void TestRollingMinMax()
        {
            var df = new DataFrame(new ISeries[] { new Int32Series("a", new int[] { 5, 2, 8, 1, 9 }) });
            var result = df.Select(
                Expr.Col("a").RollingMin(3).Alias("min"),
                Expr.Col("a").RollingMax(3).Alias("max")
            );

            var minCol = result.GetColumn("min") as Int32Series;
            var maxCol = result.GetColumn("max") as Int32Series;

            // [5, 2, 8, 1, 9]
            // window 3:
            // [null, null, min(5,2,8)=2, min(2,8,1)=1, min(8,1,9)=1]
            // [null, null, max(5,2,8)=8, max(2,8,1)=8, max(8,1,9)=9]
            
            Assert.Equal(2, minCol!.Get(2));
            Assert.Equal(1, minCol!.Get(3));
            Assert.Equal(1, minCol!.Get(4));

            Assert.Equal(8, maxCol!.Get(2));
            Assert.Equal(8, maxCol!.Get(3));
            Assert.Equal(9, maxCol!.Get(4));
        }

        [Fact]
        public void TestComplexAgg()
        {
            var df = new DataFrame(new ISeries[] { 
                new Int32Series("id", new int[] { 1, 1, 2, 2 }),
                new Int32Series("val", new int[] { 10, 20, 30, 40 })
            });

            var result = df.GroupBy("id").Agg(
                (Expr.Col("val").Sum() * 2.0).Alias("val_sum_2x")
            ).Sort("id");

            var aggCol = result.GetColumn("val_sum_2x") as Float64Series;
            Assert.NotNull(aggCol);

            // id 1: (10+20)*2 = 60
            // id 2: (30+40)*2 = 140
            Assert.Equal(60.0, aggCol!.Get(0));
            Assert.Equal(140.0, aggCol!.Get(1));
        }
    }
}
