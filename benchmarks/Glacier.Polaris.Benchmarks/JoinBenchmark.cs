using System;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Glacier.Polaris.Data;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris.Benchmarks
{
    [MemoryDiagnoser]
    [BenchmarkDotNet.Attributes.InProcess]
    public class JoinBenchmark
    {
        private Int32Series _left = null!;
        private Int32Series _right = null!;

        [Params(1_000_000, 10_000_000)]
        public int N;

        [GlobalSetup]
        public void Setup()
        {
            _left = new Int32Series("L", N);
            _right = new Int32Series("R", N);

            var lSpan = _left.Memory.Span;
            var rSpan = _right.Memory.Span;

            var rnd = new Random(42);
            for (int i = 0; i < N; i++)
            {
                lSpan[i] = rnd.Next(0, N / 2);
                rSpan[i] = rnd.Next(0, N / 2);
            }
        }

        [Benchmark(Baseline = true)]
        public int LinqJoin()
        {
            // Simulate a LINQ Join
            var leftList = Enumerable.Range(0, _left.Length).Select(i => _left.Memory.Span[i]).ToList();
            var rightList = Enumerable.Range(0, _right.Length).Select(i => _right.Memory.Span[i]).ToList();

            var query = from l in leftList
                        join r in rightList on l equals r
                        select l;

            return query.Count();
        }

        [Benchmark]
        public int ParallelJoin()
        {
            var result = JoinKernels.InnerJoin(_left, _right);
            return result.LeftIndices.Length;
        }
    }
}
