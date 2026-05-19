using System;
using System.IO;
using Xunit;
using Glacier.Polaris.Data;

namespace Glacier.Polaris.Tests
{
    public class MmfTests : IDisposable
    {
        private readonly string _tempFileDouble;
        private readonly string _tempFileInt;

        public MmfTests()
        {
            string tempDir = Path.GetTempPath();
            _tempFileDouble = Path.Combine(tempDir, $"polaris_mmf_double_{Guid.NewGuid():N}.bin");
            _tempFileInt = Path.Combine(tempDir, $"polaris_mmf_int_{Guid.NewGuid():N}.bin");
        }

        public void Dispose()
        {
            if (File.Exists(_tempFileDouble)) try { File.Delete(_tempFileDouble); } catch { }
            if (File.Exists(_tempFileInt)) try { File.Delete(_tempFileInt); } catch { }
        }

        [Fact]
        public void TestFloat64SeriesMmfPersistence()
        {
            const int length = 5;

            // 1. Create and write to MMF series
            using (var series = Float64Series.FromMmf("vals", _tempFileDouble, length))
            {
                Assert.Equal(length, series.Length);
                var span = series.Memory.Span;
                span[0] = 10.5;
                span[1] = 20.5;
                span[2] = 30.5;
                span[3] = 40.5;
                span[4] = 50.5;
            } // disposes/flushes MMF to disk

            // 2. Reopen and verify values
            using (var series = Float64Series.FromMmf("vals", _tempFileDouble, length))
            {
                Assert.Equal(10.5, series.Memory.Span[0]);
                Assert.Equal(50.5, series.Memory.Span[4]);

                // Perform an aggregation operation
                double sum = 0;
                var span = series.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    sum += span[i];
                }
                Assert.Equal(152.5, sum);
            }
        }

        [Fact]
        public void TestInt32SeriesMmfPersistenceAndDataFrame()
        {
            const int length = 4;

            // 1. Create and write to MMF series
            using (var series = Int32Series.FromMmf("ids", _tempFileInt, length))
            {
                var span = series.Memory.Span;
                span[0] = 100;
                span[1] = 200;
                span[2] = 300;
                span[3] = 400;
            }

            // 2. Reopen and load into a DataFrame
            using (var series = Int32Series.FromMmf("ids", _tempFileInt, length))
            {
                var df = new DataFrame(new ISeries[] { series });
                Assert.Equal(1, df.Columns.Count);
                Assert.Equal(length, df.RowCount);

                // Try accessing values via DataFrame API
                var col = df.GetColumn("ids") as Int32Series;
                Assert.NotNull(col);
                Assert.Equal(300, col.Memory.Span[2]);
            }
        }
    }
}
