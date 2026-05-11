using System;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris.Benchmarks
{
    [MemoryDiagnoser]
    [BenchmarkDotNet.Attributes.InProcess]
    public class SortBenchmark
    {
        private int[] _data = null!;
        private int[] _copy = null!;

        [Params(1_000_000, 10_000_000)]
        public int N;

        [GlobalSetup]
        public void Setup()
        {
            _data = new int[N];
            _copy = new int[N];
            var rnd = new Random(42);
            for (int i = 0; i < N; i++)
            {
                _data[i] = rnd.Next();
            }
        }

        [Benchmark(Baseline = true)]
        public void ArraySort()
        {
            _data.CopyTo(_copy, 0);
            Array.Sort(_copy);
        }

        [Benchmark]
        public int[] ArgSort()
        {
            return SortKernels.ArgSort(_data);
        }
    }
}
