using System;
using System.Collections.Generic;
using System.Threading;
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
            var df1 = new DataFrame(new List<ISeries> { new Int32Series("a", new[] { 1 }) });
            var df2 = new DataFrame(new List<ISeries> { new Int32Series("a", new[] { 1 }) });

            using var barrier = new CountdownEvent(2);
            int concurrentArrivals = 0;

            var lf1 = df1.Lazy().Map(batch =>
            {
                barrier.Signal();
                if (barrier.Wait(TimeSpan.FromSeconds(5)))
                {
                    Interlocked.Increment(ref concurrentArrivals);
                }
                return batch;
            });

            var lf2 = df2.Lazy().Map(batch =>
            {
                barrier.Signal();
                if (barrier.Wait(TimeSpan.FromSeconds(5)))
                {
                    Interlocked.Increment(ref concurrentArrivals);
                }
                return batch;
            });

            var lf = lf1.Join(lf2, "a", JoinType.Inner);
            var result = await lf.Collect();

            Assert.Equal(1, result.RowCount);
            Assert.Equal(2, concurrentArrivals);
        }
    }
}
