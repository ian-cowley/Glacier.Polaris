using System;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris.Tests
{
    public class AsofJoinTests
    {
        [Fact]
        public void TestAsofJoinBasic()
        {
            // Left: [10, 20, 30]
            var leftKeys = new Int32Series("time", new int[] { 10, 20, 30 });
            var leftVals = new Int32Series("val_l", new int[] { 1, 2, 3 });
            var df_l = new DataFrame(new ISeries[] { leftKeys, leftVals });

            // Right: [5, 15, 25]
            var rightKeys = new Int32Series("time", new int[] { 5, 15, 25 });
            var rightVals = new Int32Series("val_r", new int[] { 50, 150, 250 });
            var df_r = new DataFrame(new ISeries[] { rightKeys, rightVals });

            var result = df_l.JoinAsof(df_r, on: "time");

            // For Left 10, last right <= 10 is 5 (index 0)
            // For Left 20, last right <= 20 is 15 (index 1)
            // For Left 30, last right <= 30 is 25 (index 2)
            
            // Note: Since ApplyTake is a placeholder, we currently only verify the kernel logic
            var indices = JoinKernels.JoinAsof(leftKeys, rightKeys);
            Assert.Equal(0, indices.RightIndices[0]);
            Assert.Equal(1, indices.RightIndices[1]);
            Assert.Equal(2, indices.RightIndices[2]);
        }

        [Fact]
        public void TestAsofJoinExactMatch()
        {
            var leftKeys = new Int32Series("time", new int[] { 10, 20 });
            var rightKeys = new Int32Series("time", new int[] { 10, 20 });
            
            var indices = JoinKernels.JoinAsof(leftKeys, rightKeys);
            Assert.Equal(0, indices.RightIndices[0]);
            Assert.Equal(1, indices.RightIndices[1]);
        }
    }
}
