using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Glacier.Polaris.Compute
{
    /// <summary>
    /// Implements a lightweight thread-pool scheduler for parallel chunk execution.
    /// Polars uses Rayon in Rust; here we use Parallel.ForEachAsync with an unbounded degree of parallelism
    /// constrained by the .NET ThreadPool.
    /// </summary>
    public class ChunkScheduler
    {
        public static async Task ProcessChunksAsync<T>(
            IAsyncEnumerable<T> chunks, 
            Func<T, CancellationToken, ValueTask> processAction,
            CancellationToken cancellationToken = default)
        {
            var options = new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };

            await Parallel.ForEachAsync(chunks, options, processAction);
        }
    }
}
