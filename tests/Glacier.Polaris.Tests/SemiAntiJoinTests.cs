using System;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class SemiAntiJoinTests
    {
        [Fact]
        public void TestSemiJoin()
        {
            var leftKeys = new Int32Series("Key", new[] { 1, 2, 3 });
            var leftVals = new Float64Series("Val", new[] { 1.1, 2.2, 3.3 });
            var leftDf = new DataFrame(new ISeries[] { leftKeys, leftVals });

            var rightKeys = new Int32Series("Key", new[] { 2, 4 });
            var rightDf = new DataFrame(new ISeries[] { rightKeys });

            var result = leftDf.JoinSemi(rightDf, "Key");

            // Only Key 2 matches
            Assert.Equal(1, result.RowCount);
            Assert.Equal(2, ((Int32Series)result.GetColumn("Key")).Memory.Span[0]);
            Assert.Equal(2.2, ((Float64Series)result.GetColumn("Val")).Memory.Span[0]);
            Assert.Equal(2, result.Columns.Count);
        }

        [Fact]
        public void TestAntiJoin()
        {
            var leftKeys = new Int32Series("Key", new[] { 1, 2, 3 });
            var leftVals = new Float64Series("Val", new[] { 1.1, 2.2, 3.3 });
            var leftDf = new DataFrame(new ISeries[] { leftKeys, leftVals });

            var rightKeys = new Int32Series("Key", new[] { 2, 4 });
            var rightDf = new DataFrame(new ISeries[] { rightKeys });

            var result = leftDf.JoinAnti(rightDf, "Key");

            // Keys 1 and 3 do not match 2 or 4
            Assert.Equal(2, result.RowCount);
            var keys = ((Int32Series)result.GetColumn("Key")).Memory.Span;
            Assert.Contains(1, keys.ToArray());
            Assert.Contains(3, keys.ToArray());
            Assert.Equal(2, result.Columns.Count);
        }
    }
}
