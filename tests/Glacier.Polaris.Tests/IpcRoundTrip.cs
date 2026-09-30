using System;
using System.IO;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class IpcRoundTrip
    {
        [Fact]
        public void WriteAndReadIpc()
        {
            // Create a DataFrame
            var df = new DataFrame(new ISeries[]
            {
                Data.Int32Series.FromValues("a", new int?[] { 1, 2, 3 }),
                Data.Utf8StringSeries.FromStrings("b", new string?[] { "x", "y", "z" }),
                Data.Float64Series.FromValues("c", new double?[] { 1.5, 2.5, 3.5 })
            });

            var filePath = Path.GetTempFileName() + ".arrow";
            try
            {
                // Write IPC
                df.WriteIpc(filePath);
                Assert.True(File.Exists(filePath));

                // Read it back
                using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                using var reader = new Apache.Arrow.Ipc.ArrowFileReader(fs);
                var batch = reader.ReadNextRecordBatch();
                Assert.NotNull(batch);

                var df2 = DataFrame.FromArrowRecordBatch(batch);
                Assert.Equal(3, df2.RowCount);
                Assert.Equal(3, df2.Columns.Count);
                Assert.Equal("a", df2.Columns[0].Name);
                Assert.Equal(1, df2.GetColumn("a")!.Get(0));
            }
            finally
            {
                if (File.Exists(filePath)) File.Delete(filePath);
            }
        }

        [Fact]
        public void EstimatedSizeWorks()
        {
            var df = new DataFrame(new ISeries[]
            {
                Data.Int32Series.FromValues("a", new int?[] { 1, 2, 3 }),
                Data.Utf8StringSeries.FromStrings("b", new string?[] { "x", "y", "z" })
            });

            long size = df.EstimatedSize();
            Assert.True(size > 0, $"EstimatedSize should be positive, got {size}");

            // Int32: 3 elements * 4 bytes = 12 + validity = 8 = ~20
            // String: data = 3 bytes, offsets = 4*4=16 + validity = 8 = ~27
            Assert.True(size >= 20, $"Size should be at least ~20 for 3 int32 values, got {size}");
        }

        [Fact]
        public void ToDictionaryWorks()
        {
            var df = new DataFrame(new ISeries[]
            {
                Data.Int32Series.FromValues("a", new int?[] { 1, 2, 3 }),
                Data.Utf8StringSeries.FromStrings("b", new string?[] { "x", "y", "z" })
            });

            var dict = df.ToDictionary();
            Assert.Equal(2, dict.Count);
            Assert.True(dict.ContainsKey("a"));
            Assert.True(dict.ContainsKey("b"));
            Assert.Equal(3, dict["a"].Count);
            Assert.Equal(1, dict["a"][0]);
            Assert.Equal(2, dict["a"][1]);
            Assert.Equal(3, dict["a"][2]);
            Assert.Equal("x", dict["b"][0]);
            Assert.Equal("y", dict["b"][1]);
            Assert.Equal("z", dict["b"][2]);
        }

        [Fact]
        public void CloneWorks()
        {
            var df = new DataFrame(new ISeries[]
            {
                Data.Int32Series.FromValues("a", new int?[] { 1, 2, 3 })
            });

            var cloned = df.Clone();
            Assert.Equal(df.RowCount, cloned.RowCount);
            Assert.Equal(df.GetColumn("a")!.Get(0), cloned.GetColumn("a")!.Get(0));

            // Verify deep copy: modify cloned should not affect original
            var clonedCol = cloned.GetColumn("a") as Data.Int32Series;
            clonedCol!.Memory.Span[0] = 99;
            Assert.Equal(1, df.GetColumn("a")!.Get(0));
        }

        [Fact]
        public void WriteAndReadGlacierStorageArrowIpc()
        {
            var df = new DataFrame(new ISeries[]
            {
                Data.Int32Series.FromValues("x", new int?[] { 10, 20, 30, 40 }),
                Data.Float32Series.FromValues("y", new float?[] { 1.1f, 2.2f, 3.3f, 4.4f })
            });

            using var ms = new MemoryStream();
            Glacier.Polaris.IO.GlacierStorageBridge.WriteGlacierArrowIpc(df, ms, leaveOpen: true);
            Assert.True(ms.Length > 0);

            ms.Position = 0;
            using var reader = new Glacier.Storage.Arrow.ArrowStreamReader(ms);
            var batch = reader.ReadNextRecordBatch();
            Assert.NotNull(batch);
            Assert.Equal(4, batch.RowCount);
            Assert.Equal(2, batch.Columns.Count);
            Assert.Equal("x", batch.Columns[0].Field.Name);
            Assert.Equal("y", batch.Columns[1].Field.Name);
            Assert.Equal(10, batch.Columns[0].GetInt32(0));
            Assert.Equal(20, batch.Columns[0].GetInt32(1));
        }
    }
}

