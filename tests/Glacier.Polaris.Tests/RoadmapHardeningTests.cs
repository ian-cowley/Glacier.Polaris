using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class RoadmapHardeningTests
    {
        [Fact]
        public async Task TestExternalMergeSort_SpillsToDiskAndMergesCorrectly()
        {
            const int numChunks = 5;
            const int rowsPerChunk = 1000;
            var rng = new Random(42);

            async IAsyncEnumerable<DataFrame> GenerateChunks()
            {
                for (int c = 0; c < numChunks; c++)
                {
                    var ids = new int[rowsPerChunk];
                    var vals = new double[rowsPerChunk];
                    var tags = new string[rowsPerChunk];

                    for (int i = 0; i < rowsPerChunk; i++)
                    {
                        ids[i] = rng.Next(100_000);
                        vals[i] = rng.NextDouble();
                        tags[i] = $"tag_{rng.Next(50)}";
                    }

                    yield return new DataFrame(new ISeries[]
                    {
                        new Int32Series("id", ids),
                        new Float64Series("val", vals),
                        new Utf8StringSeries("tag", tags)
                    });
                }
            }

            // Budget of 1 KB forces spilling to disk on every chunk!
            var sortedStream = ExternalMergeSort.SortAsync(
                GenerateChunks(),
                new[] { "id" },
                new[] { false },
                budgetBytes: 1024,
                outputBatchSize: 400);

            var collectedChunks = new List<DataFrame>();
            await foreach (var batch in sortedStream)
            {
                collectedChunks.Add(batch);
            }

            Assert.True(collectedChunks.Count > 1, "Should have yielded multiple batches.");
            int totalRows = collectedChunks.Sum(b => b.RowCount);
            Assert.Equal(numChunks * rowsPerChunk, totalRows);

            // Verify monotonicity across all batches
            int lastId = int.MinValue;
            foreach (var batch in collectedChunks)
            {
                var idCol = (Int32Series)batch.GetColumn("id");
                var span = idCol.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    Assert.True(span[i] >= lastId, $"Sort invariant violated: {span[i]} < {lastId}");
                    lastId = span[i];
                }
            }
        }

        [Fact]
        public async Task TestExternalMergeSort_DescendingWithNulls()
        {
            var ids1 = new int[] { 10, 50, 30 };
            var s1 = new Int32Series("id", ids1);
            s1.ValidityMask.SetNull(1); // 50 is null

            var ids2 = new int[] { 70, 20, 90 };
            var s2 = new Int32Series("id", ids2);

            async IAsyncEnumerable<DataFrame> GenerateChunks()
            {
                yield return new DataFrame(new ISeries[] { s1 });
                yield return new DataFrame(new ISeries[] { s2 });
            }

            var sortedStream = ExternalMergeSort.SortAsync(
                GenerateChunks(),
                new[] { "id" },
                new[] { true }, // descending
                budgetBytes: 10);

            var batches = new List<DataFrame>();
            await foreach (var b in sortedStream) batches.Add(b);

            var full = batches.Count == 1 ? batches[0] : DataFrame.Concat(batches);
            Assert.Equal(6, full.RowCount);

            var col = (Int32Series)full.GetColumn("id");
            // Valid non-null values should be descending: 90, 70, 30, 20, 10, then null
            Assert.Equal(90, col.Memory.Span[0]);
            Assert.Equal(70, col.Memory.Span[1]);
            Assert.Equal(30, col.Memory.Span[2]);
            Assert.Equal(20, col.Memory.Span[3]);
            Assert.Equal(10, col.Memory.Span[4]);
            Assert.True(col.ValidityMask.IsNull(5));
        }

        [Fact]
        public void TestQueryOptimizer_NestedStructPushdownTracking()
        {
            var optimizer = new QueryOptimizer();
            var lf = LazyFrame.ScanCsv("dummy.csv")
                .Select(Expr.Col("user").Struct().Field("address").Struct().Field("zipcode"));
            optimizer.Optimize(lf.Plan);

            Assert.True(optimizer.NestedFieldPaths.ContainsKey("user"));
            var paths = optimizer.NestedFieldPaths["user"];
            Assert.Contains("address.zipcode", paths);
            Assert.Contains("address", paths);
        }

        [Fact]
        public void TestStringKernels_WildcardSIMD()
        {
            var strings = new[] { "apple", "banana", "avocado", "cherry", "alpha", "beta" };
            var series = new Utf8StringSeries("s", strings);

            var result = new int[strings.Length];
            // Pattern a.*a should match "banana" (contains an 'a' then another 'a'), "avocado", "alpha"
            StringKernels.RegexMatch(series.DataBytes.Span, series.Offsets.Span, "a.*a", result.AsSpan());

            Assert.Equal(0, result[0]); // apple: has only one 'a'
            Assert.Equal(1, result[1]); // banana: has multiple 'a's
            Assert.Equal(1, result[2]); // avocado: has multiple 'a's
            Assert.Equal(0, result[3]); // cherry: no 'a'
            Assert.Equal(1, result[4]); // alpha: has multiple 'a's
            Assert.Equal(0, result[5]); // beta: has only one 'a'
        }

        [Fact]
        public async Task TestParquet_RoundTripMultiTypes()
        {
            string path = Path.Combine(Path.GetTempPath(), $"polaris_test_types_{Guid.NewGuid():N}.parquet");
            try
            {
                var df = new DataFrame(new ISeries[]
                {
                    new Int32Series("i32", new[] { 1, 2, 3 }),
                    new Int64Series("i64", new[] { 100L, 200L, 300L }),
                    new Float64Series("f64", new[] { 1.5, 2.5, 3.5 }),
                    new Float32Series("f32", new[] { 10.5f, 20.5f, 30.5f }),
                    new BooleanSeries("b", new[] { true, false, true }),
                    new Utf8StringSeries("s", new[] { "hello", "world", "polaris" }),
                    new DecimalSeries("dec", new decimal?[] { 12.34m, 56.78m, 90.12m })
                });

                df.WriteParquet(path);

                var readBack = await LazyFrame.ScanParquet(path).Collect();
                Assert.Equal(3, readBack.RowCount);
                Assert.Equal(7, readBack.Columns.Count);

                var i32Col = (Int32Series)readBack.GetColumn("i32");
                Assert.Equal(1, i32Col.Memory.Span[0]);
                Assert.Equal(3, i32Col.Memory.Span[2]);

                var strCol = (Utf8StringSeries)readBack.GetColumn("s");
                Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(strCol.GetStringSpan(0)));
                Assert.Equal("polaris", System.Text.Encoding.UTF8.GetString(strCol.GetStringSpan(2)));

                var decCol = (DecimalSeries)readBack.GetColumn("dec");
                Assert.Equal(12.34m, decCol.Memory.Span[0]);
            }
            finally
            {
                if (File.Exists(path))
                {
                    try { File.Delete(path); } catch { }
                }
            }
        }
    }
}
