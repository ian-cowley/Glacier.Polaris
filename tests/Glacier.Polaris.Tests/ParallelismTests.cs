using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class ParallelismTests
    {
        [Fact]
        public async Task TestJoinParallelism()
        {
            var df1 = new DataFrame(new System.Collections.Generic.List<ISeries> { new Int32Series("a", new[] { 1 }) });
            var df2 = new DataFrame(new System.Collections.Generic.List<ISeries> { new Int32Series("a", new[] { 1 }) });

            var sw = Stopwatch.StartNew();
            
            // Branch 1 takes 500ms
            var lf1 = df1.Lazy().Delay(500);
            // Branch 2 takes 500ms
            var lf2 = df2.Lazy().Delay(500);

            // Join should run them in parallel (ApplyJoin uses Task.Run for right side)
            var lf = lf1.Join(lf2, "a", JoinType.Inner);
            
            var result = await lf.Collect();
            sw.Stop();
            
            Assert.Equal(1, result.RowCount);
            // If parallel, should be ~500ms + overhead. If sequential, should be ~1000ms.
            Assert.True(sw.ElapsedMilliseconds < 800, $"Execution took too long: {sw.ElapsedMilliseconds}ms. Parallelism might be broken.");
        }
    }
}
