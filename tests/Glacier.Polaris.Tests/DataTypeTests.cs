using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class DataTypeTests
    {
        [Fact]
        public async Task TestAllNumericTypes()
        {
            var df = new DataFrame(new System.Collections.Generic.List<ISeries>
            {
                new Int8Series("i8", 1),
                new Int16Series("i16", 1),
                new Int32Series("i32", new[] { 1 }),
                new Int64Series("i64", new[] { 1L }),
                new UInt8Series("u8", 1),
                new UInt16Series("u16", 1),
                new UInt32Series("u32", 1),
                new UInt64Series("u64", 1),
                new Float32Series("f32", 1),
                new Float64Series("f64", new[] { 1.0 }),
                new BooleanSeries("bool", new[] { true }),
                new DateSeries("date", 1),
                new DatetimeSeries("datetime", 1)
            });

            var lf = df.Lazy();
            var result = await lf.Collect();

            Assert.Equal(1, result.RowCount);
            Assert.Equal(13, result.Columns.Count);
            
            Assert.IsType<Int8Series>(result.GetColumn("i8"));
            Assert.IsType<Int16Series>(result.GetColumn("i16"));
            Assert.IsType<Int32Series>(result.GetColumn("i32"));
            Assert.IsType<Int64Series>(result.GetColumn("i64"));
            Assert.IsType<UInt8Series>(result.GetColumn("u8"));
            Assert.IsType<UInt16Series>(result.GetColumn("u16"));
            Assert.IsType<UInt32Series>(result.GetColumn("u32"));
            Assert.IsType<UInt64Series>(result.GetColumn("u64"));
            Assert.IsType<Float32Series>(result.GetColumn("f32"));
            Assert.IsType<Float64Series>(result.GetColumn("f64"));
            Assert.IsType<BooleanSeries>(result.GetColumn("bool"));
            Assert.IsType<DateSeries>(result.GetColumn("date"));
            Assert.IsType<DatetimeSeries>(result.GetColumn("datetime"));
        }
        [Fact]
        public async Task TestFiltersAndLiterals()
        {
            var df = new DataFrame(new System.Collections.Generic.List<ISeries>
            {
                new Int32Series("a", new[] { 1, 2, 3, 4, 5 }),
                new Float64Series("b", new[] { 1.0, 2.0, 3.0, 4.0, 5.0 }),
                new Utf8StringSeries("c", new[] { "apple", "banana", "cherry", "date", "elderberry" })
            });

            var lf = df.Lazy()
                .Filter(c => Expr.Col("a") > 2)
                .WithColumns(new[] {
                    (Expr.Col("b") + 10.0).Alias("b_plus_10"),
                    Expr.Col("c").Str().StartsWith("c").Alias("is_c")
                });

            var result = await lf.Collect();

            Assert.Equal(3, result.RowCount); // 3, 4, 5
            
            var aCol = (Int32Series)result.GetColumn("a");
            Assert.Equal(3, aCol.Memory.Span[0]);
            
            var bPlus10 = (Float64Series)result.GetColumn("b_plus_10");
            Assert.Equal(13.0, bPlus10.Memory.Span[0]);

            var isC = (BooleanSeries)result.GetColumn("is_c");
            Assert.True(isC.Memory.Span[0]); // cherry starts with c
            Assert.False(isC.Memory.Span[1]); // date does not
        }
    }
}
