using BenchmarkDotNet.Running;

namespace Glacier.Polaris.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            FullSuiteBenchmark.Run();
        }
    }
}
