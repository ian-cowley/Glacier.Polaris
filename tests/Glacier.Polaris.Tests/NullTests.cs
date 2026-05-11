using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class NullTests
    {
        [Fact]
        public async Task TestNullPropagation()
        {
            var a = new Int32Series("a", new[] { 1, 2, 3 });
            a.ValidityMask.SetNull(1); // Row 1 is null

            var df = new DataFrame(new System.Collections.Generic.List<ISeries> { a });
            
            var lf = df.Lazy()
                .Select(Expr.Col("a") + 10);

            var result = await lf.Collect();
            var resCol = (Int32Series)result.GetColumn("res");

            Assert.True(resCol.ValidityMask.IsValid(0));
            Assert.True(resCol.ValidityMask.IsNull(1));
            Assert.True(resCol.ValidityMask.IsValid(2));
            
            Assert.Equal(11, resCol.Memory.Span[0]);
            Assert.Equal(13, resCol.Memory.Span[2]);
        }

        [Fact]
        public async Task TestIsNullExpression()
        {
            var a = new Int32Series("a", new[] { 1, 2, 3 });
            a.ValidityMask.SetNull(1);

            var df = new DataFrame(new System.Collections.Generic.List<ISeries> { a });
            
            var lf = df.Lazy()
                .Select(Expr.Col("a").IsNull().Alias("is_null"), 
                        Expr.Col("a").IsNotNull().Alias("is_not_null"));

            var result = await lf.Collect();
            
            var isNull = (BooleanSeries)result.GetColumn("is_null");
            var isNotNull = (BooleanSeries)result.GetColumn("is_not_null");

            Assert.False(isNull.Memory.Span[0]);
            Assert.True(isNull.Memory.Span[1]);
            Assert.False(isNull.Memory.Span[2]);

            Assert.True(isNotNull.Memory.Span[0]);
            Assert.False(isNotNull.Memory.Span[1]);
            Assert.True(isNotNull.Memory.Span[2]);
        }

        [Fact]
        public void TestCategoricalCreation()
        {
            var strings = new[] { "a", "b", null, "a" };
            var cat = CategoricalSeries.FromStrings("cat", strings);

            Assert.Equal(4, cat.Length);
            Assert.Equal(2, cat.RevMap.Length); // "a", "b"
            Assert.Equal(0u, cat.Memory.Span[0]);
            Assert.Equal(1u, cat.Memory.Span[1]);
            Assert.True(cat.ValidityMask.IsNull(2));
            Assert.Equal(0u, cat.Memory.Span[3]);
        }
        [Fact]
        public async Task TestCategoricalCast()
        {
            var df = new DataFrame(new System.Collections.Generic.List<ISeries>
            {
                new Utf8StringSeries("s", new[] { "apple", "banana", "apple", "cherry" })
            });

            var lf = df.Lazy()
                .Select(Expr.Col("s").Cast(typeof(CategoricalSeries)).Alias("c"));

            var result = await lf.Collect();
            var cat = (CategoricalSeries)result.GetColumn("c");

            Assert.Equal(4, cat.Length);
            Assert.Equal(3, cat.RevMap.Length); // apple, banana, cherry
            Assert.Equal(0u, cat.Memory.Span[0]); // apple
            Assert.Equal(1u, cat.Memory.Span[1]); // banana
            Assert.Equal(0u, cat.Memory.Span[2]); // apple
            Assert.Equal(2u, cat.Memory.Span[3]); // cherry
        }
    }
}
