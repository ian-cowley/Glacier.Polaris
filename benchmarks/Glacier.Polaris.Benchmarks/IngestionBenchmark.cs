using System;
using System.IO;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Glacier.Polaris.IO;
using Glacier.Polaris;
using System.Collections.Generic;

namespace Glacier.Polaris.Benchmarks
{
    [MemoryDiagnoser]
    [BenchmarkDotNet.Attributes.InProcess]
    public class IngestionBenchmark
    {
        private const string FilePath = "benchmark_data.csv";

        [GlobalSetup]
        public void Setup()
        {
            // Create a 1 million row CSV
            using var writer = new StreamWriter(FilePath);
            for (int i = 0; i < 1_000_000; i++)
            {
                writer.WriteLine($"{i}");
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            if (File.Exists(FilePath))
                File.Delete(FilePath);
        }

        [Benchmark]
        public async Task<int> ReadPipelinesZeroAllocAsync()
        {
            var reader = new CsvReader(FilePath);
            int chunks = 0;

            await foreach (var df in reader.ReadAsync())
            {
                chunks++;
            }

            return chunks;
        }

        [Benchmark(Baseline = true)]
        public async Task<int> ReadStandardStreamReaderAsync()
        {
            using var stream = new StreamReader(FilePath);
            int rows = 0;
            string? line;
            while ((line = await stream.ReadLineAsync()) != null)
            {
                if (int.TryParse(line, out int val))
                {
                    rows++;
                }
            }
            return rows;
        }
    }
}
