using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Glacier.Polaris.IO;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class M2412HardeningTests
    {
        private sealed class SingleThreadedDeadlockDetectingSyncContext : SynchronizationContext
        {
            private readonly int _threadId = Environment.CurrentManagedThreadId;

            public override void Post(SendOrPostCallback d, object? state)
            {
                if (Environment.CurrentManagedThreadId == _threadId)
                    throw new InvalidOperationException("Deadlock hazard: synchronous call posted continuation back to blocked thread.");
                throw new InvalidOperationException("Deadlock hazard: async continuation posted back to blocked single-threaded SynchronizationContext.");
            }

            public override void Send(SendOrPostCallback d, object? state)
            {
                throw new InvalidOperationException("Deadlock hazard: Send called on blocked single-threaded SynchronizationContext.");
            }
        }

        [Fact]
        public async Task ParquetWriter_OverwriteLargerFile_TruncatesCleanly_AndRoundTripsAllTypesWithNulls()
        {
            string path = Path.Combine(Path.GetTempPath(), $"polaris_pq_m2412_{Guid.NewGuid():N}.parquet");
            try
            {
                // First write a large file
                var largeDf = new DataFrame(new ISeries[]
                {
                    new Int32Series("id", Enumerable.Range(0, 5000).ToArray()),
                    new Utf8StringSeries("pad", Enumerable.Range(0, 5000).Select(i => $"padding_payload_{i:D6}").ToArray())
                });
                largeDf.WriteParquet(path);
                long largeLen = new FileInfo(path).Length;

                // Overwrite with a much smaller DataFrame covering widened ints, ulong, binary, enum, categorical, null, list, array, struct, object
                var listOffsets = new Int32Series("off", new[] { 0, 2, 2, 3 });
                var listVals = Int32Series.FromValues("v", new int?[] { 10, null, 30 });
                var listCol = new ListSeries("list_col", listOffsets, listVals);
                listCol.ValidityMask.SetNull(1);

                var arrVals = new BinarySeries("av", new byte[]?[]
                {
                    new byte[] { 1, 2 }, new byte[] { 3 },
                    new byte[] { 4 }, null,
                    new byte[] { 5, 6 }, new byte[] { 7 }
                });
                var arrCol = new ArraySeries("arr_col", 2, arrVals);
                arrCol.ValidityMask.SetNull(1);

                var stCol = new StructSeries("st_col", new ISeries[]
                {
                    Int32Series.FromValues("x", new int?[] { 1, 2, 3 }),
                    Utf8StringSeries.FromStrings("s", new string?[] { "a", null, "c" })
                });
                stCol.ValidityMask.SetNull(1);

                var smallDf = new DataFrame(new ISeries[]
                {
                    Int8Series.FromValues("i8", new sbyte?[] { -5, null, 12 }),
                    Int16Series.FromValues("i16", new short?[] { -300, 400, null }),
                    UInt8Series.FromValues("u8", new byte?[] { 1, null, 250 }),
                    UInt16Series.FromValues("u16", new ushort?[] { null, 50000, 42 }),
                    UInt32Series.FromValues("u32", new uint?[] { 3_000_000_000u, null, 7u }),
                    UInt64Series.FromValues("u64", new ulong?[] { ulong.MaxValue - 5, null, 99UL }),
                    new BinarySeries("bin", new byte[]?[] { new byte[] { 0xDE, 0xAD }, null, Array.Empty<byte>() }),
                    new NullSeries("nul", 3),
                    EnumSeries.FromStrings("enm", new string?[] { "red", null, "blue" }, new[] { "red", "blue" }),
                    CategoricalSeries.FromStrings("cat", new string?[] { "x", "y", null }),
                    listCol,
                    arrCol,
                    stCol,
                    new ObjectSeries("obj", new object?[] { "alpha", null, 123 })
                });

                smallDf.WriteParquet(path);
                long smallLen = new FileInfo(path).Length;
                Assert.True(smallLen < largeLen, $"Expected truncated file ({smallLen} < {largeLen}).");

                var readBack = await LazyFrame.ScanParquet(path).Collect();
                Assert.Equal(3, readBack.RowCount);

                Assert.Equal(-5, readBack.GetColumn("i8").Get(0));
                Assert.True(readBack.GetColumn("i8").ValidityMask.IsNull(1));
                Assert.Equal(12, readBack.GetColumn("i8").Get(2));

                Assert.True(readBack.GetColumn("i16").ValidityMask.IsNull(2));
                Assert.True(readBack.GetColumn("u8").ValidityMask.IsNull(1));
                Assert.True(readBack.GetColumn("u16").ValidityMask.IsNull(0));
                Assert.Equal(3_000_000_000L, readBack.GetColumn("u32").Get(0));
                Assert.True(readBack.GetColumn("u32").ValidityMask.IsNull(1));

                var u64Read = Assert.IsType<UInt64Series>(readBack.GetColumn("u64"));
                Assert.Equal(ulong.MaxValue - 5, u64Read.Memory.Span[0]);
                Assert.True(u64Read.ValidityMask.IsNull(1));
                Assert.Equal(99UL, u64Read.Memory.Span[2]);

                var binRead = Assert.IsType<BinarySeries>(readBack.GetColumn("bin"));
                Assert.Equal(new byte[] { 0xDE, 0xAD }, binRead.GetValue(0));
                Assert.True(binRead.ValidityMask.IsNull(1));
                Assert.Equal(Array.Empty<byte>(), binRead.GetValue(2));

                Assert.True(readBack.GetColumn("nul").ValidityMask.IsNull(0));
                Assert.True(readBack.GetColumn("nul").ValidityMask.IsNull(2));

                Assert.Equal("red", readBack.GetColumn("enm").Get(0));
                Assert.True(readBack.GetColumn("enm").ValidityMask.IsNull(1));

                Assert.Equal("y", readBack.GetColumn("cat").Get(1));
                Assert.True(readBack.GetColumn("cat").ValidityMask.IsNull(2));

                Assert.Equal("[10,null]", readBack.GetColumn("list_col").Get(0));
                Assert.True(readBack.GetColumn("list_col").ValidityMask.IsNull(1));

                Assert.Equal("[\"AQI=\",\"Aw==\"]", readBack.GetColumn("arr_col").Get(0));
                Assert.True(readBack.GetColumn("arr_col").ValidityMask.IsNull(1));

                Assert.Equal("{\"x\":1,\"s\":\"a\"}", readBack.GetColumn("st_col").Get(0));
                Assert.True(readBack.GetColumn("st_col").ValidityMask.IsNull(1));

                Assert.Equal("alpha", readBack.GetColumn("obj").Get(0));
                Assert.True(readBack.GetColumn("obj").ValidityMask.IsNull(1));
                Assert.Equal("123", readBack.GetColumn("obj").Get(2));
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }

        [Fact]
        public async Task ParquetReader_MultiRowGroup_EarlyLimitAndCorruptStreamErrorPropagation()
        {
            string path = Path.Combine(Path.GetTempPath(), $"polaris_pq_mrg_{Guid.NewGuid():N}.parquet");
            string corruptPath = Path.Combine(Path.GetTempPath(), $"polaris_pq_bad_{Guid.NewGuid():N}.parquet");
            try
            {
                async IAsyncEnumerable<DataFrame> Batches()
                {
                    yield return new DataFrame(new ISeries[] { new Int32Series("id", new[] { 1, 2 }) });
                    yield return new DataFrame(new ISeries[] { new Int32Series("id", new[] { 3, 4 }) });
                    yield return new DataFrame(new ISeries[] { new Int32Series("id", new[] { 5, 6 }) });
                    await Task.CompletedTask;
                }

                await ParquetWriter.WriteStreamAsync(Batches(), path);

                // Early Limit(1) stops consuming mid-stream cleanly
                var limited = await LazyFrame.ScanParquet(path).Limit(1).Collect();
                Assert.Equal(1, limited.RowCount);
                Assert.Equal(1, limited.GetColumn("id").Get(0));

                // Corrupt parquet throws cleanly without hanging or masking error
                await File.WriteAllBytesAsync(corruptPath, new byte[] { 0x50, 0x41, 0x52, 0x31, 0, 1, 2, 3, 0x50, 0x41, 0x52, 0x31 });
                await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await foreach (var _ in new ParquetReader(corruptPath).ReadAsync()) { }
                });
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
                if (File.Exists(corruptPath)) File.Delete(corruptPath);
            }
        }

        [Fact]
        public async Task CsvParserAndReader_EscapedQuotes_MultiLineQuotedFields_AndSplitSegments()
        {
            // Multi-segment ReadOnlySequence<byte> with escaped "" and embedded \n split across segments
            byte[] seg1 = Encoding.UTF8.GetBytes("\"h\"\"1\",\"h,2\"\n\"a\"\"");
            byte[] seg2 = Encoding.UTF8.GetBytes("b\",\"line1\nline2\"\r\n1,2\n");
            var firstSeg = new BufferSegment(seg1);
            var secondSeg = firstSeg.Append(seg2);
            var seq = new ReadOnlySequence<byte>(firstSeg, 0, secondSeg, seg2.Length);

            Assert.True(CsvParser.TryReadLine(ref seq, out var headerSeq));
            var headers = CsvParser.ParseHeader(headerSeq.ToArray());
            Assert.Equal(new[] { "h\"1", "h,2" }, headers);

            Assert.True(CsvParser.TryReadLine(ref seq, out var row1Seq));
            var b0 = new Utf8StringColumnBuilder("c0");
            var b1 = new Utf8StringColumnBuilder("c1");
            CsvParser.ParseRow(row1Seq.ToArray(), new IColumnBuilder?[] { b0, b1 });
            using var s0 = (Utf8StringSeries)b0.Build();
            using var s1 = (Utf8StringSeries)b1.Build();
            Assert.Equal("a\"b", s0.GetString(0));
            Assert.Equal("line1\nline2", s1.GetString(0));
            b0.Dispose();
            b1.Dispose();

            Assert.True(CsvParser.TryReadLine(ref seq, out var row2Seq));
            Assert.Equal("1,2", Encoding.UTF8.GetString(row2Seq.ToArray()));

            // Full CsvReader + PeekHeaders with UTF-8 BOM, quoted commas, escaped "", and multi-line fields
            string csvPath = Path.Combine(Path.GetTempPath(), $"polaris_csv_rfc_{Guid.NewGuid():N}.csv");
            try
            {
                string csvContent = "\"col\"\"1\",\"col,2\",\"col\n3\"\r\n\"val\"\"a\",\"x,y\",\"multi\nline\"\r\n\"z\",simple,end\n";
                await File.WriteAllTextAsync(csvPath, csvContent, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

                var peeked = CsvReader.PeekHeaders(csvPath);
                Assert.Equal(new[] { "col\"1", "col,2", "col\n3" }, peeked);

                var df = await LazyFrame.ScanCsv(csvPath).Collect();
                Assert.Equal(2, df.RowCount);
                Assert.Equal("val\"a", df.GetColumn("col\"1").Get(0));
                Assert.Equal("x,y", df.GetColumn("col,2").Get(0));
                Assert.Equal("multi\nline", df.GetColumn("col\n3").Get(0));
                Assert.Equal("z", df.GetColumn("col\"1").Get(1));
            }
            finally
            {
                if (File.Exists(csvPath)) File.Delete(csvPath);
            }
        }

        [Fact]
        public async Task JsonReader_HybridIoDispatcher_SmallRandomAccess_BoundaryChunks_AndMultiMbMmf()
        {
            string smallPath = Path.Combine(Path.GetTempPath(), $"polaris_json_small_{Guid.NewGuid():N}.json");
            string largeNdjsonPath = Path.Combine(Path.GetTempPath(), $"polaris_json_mmf_{Guid.NewGuid():N}.ndjson");
            int origMaxChunk = HybridIoDispatcher.MaxChunkSize;

            try
            {
                // 1. Force multi-chunk boundaries even on small JSON array via MaxChunkSize = 64
                var sbArray = new StringBuilder("[");
                for (int i = 0; i < 40; i++)
                {
                    if (i > 0) sbArray.Append(',');
                    sbArray.Append($"{{\"id\":{i},\"name\":\"item_{i:D2}_payload\"}}");
                }
                sbArray.Append(']');
                await File.WriteAllTextAsync(smallPath, sbArray.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

                HybridIoDispatcher.MaxChunkSize = 64;
                var smallArrayDf = DataFrame.ReadJson(smallPath);
                Assert.Equal(40, smallArrayDf.RowCount);
                Assert.Equal(0, smallArrayDf.GetColumn("id").Get(0));
                Assert.Equal("item_39_payload", smallArrayDf.GetColumn("name").Get(39));

                // 2. Real > 1 MB NDJSON file exercising the MemoryMappedFile path
                HybridIoDispatcher.MaxChunkSize = 256 * 1024;
                var sbNd = new StringBuilder(1_250_000);
                int expectedRows = 0;
                while (sbNd.Length < 1_150_000)
                {
                    sbNd.Append($"{{\"id\":{expectedRows},\"msg\":\"row_{expectedRows:D6}_abcdefghij\"}}\n");
                    expectedRows++;
                }
                await File.WriteAllTextAsync(largeNdjsonPath, sbNd.ToString());
                Assert.True(new FileInfo(largeNdjsonPath).Length >= HybridIoDispatcher.OneMegaByte);

                var mmfDf = DataFrame.ReadJson(largeNdjsonPath);
                Assert.Equal(expectedRows, mmfDf.RowCount);
                Assert.Equal(0, mmfDf.GetColumn("id").Get(0));
                Assert.Equal(expectedRows - 1, mmfDf.GetColumn("id").Get(expectedRows - 1));
                Assert.Equal($"row_{expectedRows - 1:D6}_abcdefghij", mmfDf.GetColumn("msg").Get(expectedRows - 1));
            }
            finally
            {
                HybridIoDispatcher.MaxChunkSize = origMaxChunk;
                if (File.Exists(smallPath)) File.Delete(smallPath);
                if (File.Exists(largeNdjsonPath)) File.Delete(largeNdjsonPath);
            }
        }

        [Fact]
        public async Task StreamingSinks_MultiChunkWithCrossChunkNulls_ZeroRow_Overwrite_AndExternalSort()
        {
            string srcPath = Path.Combine(Path.GetTempPath(), $"polaris_sink_src_{Guid.NewGuid():N}.ndjson");
            string csvOut = Path.Combine(Path.GetTempPath(), $"polaris_sink_out_{Guid.NewGuid():N}.csv");
            string pqOut = Path.Combine(Path.GetTempPath(), $"polaris_sink_out_{Guid.NewGuid():N}.parquet");
            string ipcOut = Path.Combine(Path.GetTempPath(), $"polaris_sink_out_{Guid.NewGuid():N}.arrow");

            try
            {
                // Chunk 1 (rows 0..1): non-null; Chunk 2 (rows 2..3): contains nulls!
                // Using JsonReader chunkSize=2 inside ScanJson? Note ScanJson uses 10000; let's also test ExecutionEngine with multi-chunk stream directly + ScanCsv -> Sort -> Sink!
                await File.WriteAllTextAsync(srcPath,
                    "{\"k\":30,\"v\":\"c\"}\n" +
                    "{\"k\":10,\"v\":\"a\"}\n" +
                    "{\"k\":20,\"v\":null}\n");

                // Pre-populate oversized files to verify overwrite truncation in all 3 sinks
                await File.WriteAllBytesAsync(csvOut, new byte[8192]);
                await File.WriteAllBytesAsync(pqOut, new byte[8192]);
                await File.WriteAllBytesAsync(ipcOut, new byte[8192]);

                var lf = LazyFrame.ScanJson(srcPath).Sort("k");
                await lf.SinkCsv(csvOut);
                await lf.SinkParquet(pqOut);
                await lf.SinkIpc(ipcOut);

                var csvRead = await LazyFrame.ScanCsv(csvOut).Collect();
                Assert.Equal(3, csvRead.RowCount);
                Assert.Equal(new[] { 10, 20, 30 }, ((Int32Series)csvRead.GetColumn("k")).Memory.Span.ToArray());

                var pqRead = await LazyFrame.ScanParquet(pqOut).Collect();
                Assert.Equal(3, pqRead.RowCount);
                Assert.Equal(new[] { 10, 20, 30 }, ((Int32Series)pqRead.GetColumn("k")).Memory.Span.ToArray());
                Assert.True(pqRead.GetColumn("v").ValidityMask.IsNull(1));

                var ipcRead = DataFrame.FromArrowIpc(ipcOut);
                Assert.Equal(3, ipcRead.RowCount);
                Assert.Equal(new[] { 10, 20, 30 }, ((Int32Series)ipcRead.GetColumn("k")).Memory.Span.ToArray());
                Assert.True(ipcRead.GetColumn("v").ValidityMask.IsNull(1));

                // Direct multi-chunk cross-nullability test (Chunk 1 non-null, Chunk 2 nullable)
                async IAsyncEnumerable<DataFrame> CrossNullStream()
                {
                    yield return new DataFrame(new ISeries[]
                    {
                        new Int32Series("id", new[] { 1, 2 }),
                        new UInt16Series("u16", new ushort[] { 10, 20 }),
                        new Utf8StringSeries("txt", new[] { "x", "y" })
                    });
                    yield return new DataFrame(new ISeries[]
                    {
                        Int32Series.FromValues("id", new int?[] { 3, null }),
                        UInt16Series.FromValues("u16", new ushort?[] { null, 40 }),
                        Utf8StringSeries.FromStrings("txt", new string?[] { "z", null })
                    });
                    await Task.CompletedTask;
                }

                await ParquetWriter.WriteStreamAsync(CrossNullStream(), pqOut);
                var pqMulti = await LazyFrame.ScanParquet(pqOut).Collect();
                Assert.Equal(4, pqMulti.RowCount);
                Assert.True(pqMulti.GetColumn("id").ValidityMask.IsNull(3));
                Assert.True(pqMulti.GetColumn("u16").ValidityMask.IsNull(2));
                Assert.True(pqMulti.GetColumn("txt").ValidityMask.IsNull(3));

                await IpcWriter.WriteStreamAsync(CrossNullStream(), ipcOut);
                var ipcMulti = DataFrame.FromArrowIpc(ipcOut);
                Assert.Equal(4, ipcMulti.RowCount);
                Assert.True(ipcMulti.GetColumn("id").ValidityMask.IsNull(3));
                Assert.True(ipcMulti.GetColumn("u16").ValidityMask.IsNull(2));
                Assert.True(ipcMulti.GetColumn("txt").ValidityMask.IsNull(3));

                // Zero-row filtered SinkCsv / SinkParquet / SinkIpc
                var emptyFiltered = lf.Filter(Expr.Col("k") > 999);
                await emptyFiltered.SinkCsv(csvOut);
                await emptyFiltered.SinkParquet(pqOut);
                await emptyFiltered.SinkIpc(ipcOut);

                Assert.Equal(new[] { "k", "v" }, CsvReader.PeekHeaders(csvOut));
                var emptyPq = await LazyFrame.ScanParquet(pqOut).Collect();
                Assert.Equal(0, emptyPq.RowCount);
                Assert.Equal(2, emptyPq.Columns.Count);
                var emptyIpc = DataFrame.FromArrowIpc(ipcOut);
                Assert.Equal(0, emptyIpc.RowCount);
                Assert.Equal(2, emptyIpc.Columns.Count);

                // ExecutionEngine SinkCsvPlanNode / SinkParquetPlanNode / SinkIpcPlanNode dispatch
                var engine = new ExecutionEngine();
                await foreach (var _ in engine.ExecuteAsync(new SinkCsvPlanNode(lf.PlanNode, csvOut))) { }
                await foreach (var _ in engine.ExecuteAsync(new SinkParquetPlanNode(lf.PlanNode, pqOut))) { }
                await foreach (var _ in engine.ExecuteAsync(new SinkIpcPlanNode(lf.PlanNode, ipcOut))) { }
                Assert.Equal(3, (await LazyFrame.ScanParquet(pqOut).Collect()).RowCount);
            }
            finally
            {
                foreach (var p in new[] { srcPath, csvOut, pqOut, ipcOut })
                    if (File.Exists(p)) File.Delete(p);
            }
        }

        [Fact]
        public async Task Joins_MultiType_MultiKey_NullInequality_AndRenameCloneAcrossAllSeriesTypes()
        {
            // 1. String key joins with nulls (null != null)
            var lStr = new DataFrame(new ISeries[]
            {
                Utf8StringSeries.FromStrings("k", new string?[] { "a", null, "b", "c" }),
                new DecimalSeries("amt", new decimal?[] { 1.5m, 2.5m, 3.5m, 4.5m })
            });
            var rStr = new DataFrame(new ISeries[]
            {
                Utf8StringSeries.FromStrings("k", new string?[] { "b", null, "d" }),
                new BinarySeries("payload", new byte[]?[] { new byte[] { 1, 2 }, new byte[] { 3 }, new byte[] { 4 } })
            });

            var inner = lStr.Join(rStr, "k", JoinType.Inner);
            Assert.Equal(1, inner.RowCount);
            Assert.Equal("b", inner.GetColumn("k").Get(0));
            Assert.Equal(new byte[] { 1, 2 }, inner.GetColumn("payload").Get(0));

            var left = lStr.Join(rStr, "k", JoinType.Left);
            Assert.Equal(4, left.RowCount);
            Assert.True(left.GetColumn("payload").ValidityMask.IsNull(1)); // null key did not match null key!
            Assert.False(left.GetColumn("payload").ValidityMask.IsNull(2));

            var outer = lStr.Join(rStr, "k", JoinType.Outer);
            Assert.Equal(6, outer.RowCount); // a, null(L), b, c, null(R), d

            var semi = lStr.Join(rStr, "k", JoinType.Semi);
            Assert.Equal(1, semi.RowCount);
            Assert.Equal(2, semi.Columns.Count);

            var anti = lStr.Join(rStr, "k", JoinType.Anti);
            Assert.Equal(3, anti.RowCount); // a, null(L), c
            Assert.Equal(2, anti.Columns.Count);

            // 2. Multi-column leftOn / rightOn on DataFrame AND LazyFrame
            var ml = new DataFrame(new ISeries[]
            {
                new Int64Series("org", new[] { 1L, 1L, 2L }),
                Utf8StringSeries.FromStrings("code", new string?[] { "x", "y", "x" }),
                new Float64Series("val", new[] { 10.0, 20.0, 30.0 })
            });
            var mr = new DataFrame(new ISeries[]
            {
                new Int64Series("org_r", new[] { 1L, 2L }),
                Utf8StringSeries.FromStrings("code_r", new string?[] { "y", "x" }),
                new Float64Series("val", new[] { 200.0, 300.0 })
            });

            var multiEager = ml.Join(mr, new[] { "org", "code" }, new[] { "org_r", "code_r" }, JoinType.Inner);
            Assert.Equal(2, multiEager.RowCount);
            Assert.Equal(new[] { "org", "code", "val", "val_right" }, multiEager.ColumnNames);
            Assert.Equal(200.0, multiEager.GetColumn("val_right").Get(0));
            Assert.Equal(300.0, multiEager.GetColumn("val_right").Get(1));

            var multiLazy = await ml.Lazy().Join(mr.Lazy(), new[] { "org", "code" }, new[] { "org_r", "code_r" }, JoinType.Left).Collect();
            Assert.Equal(3, multiLazy.RowCount);
            Assert.True(multiLazy.GetColumn("val_right").ValidityMask.IsNull(0));
            Assert.Equal(200.0, multiLazy.GetColumn("val_right").Get(1));

            // 3. Key arity & type mismatch throw ArgumentException
            Assert.Throws<ArgumentException>(() => ml.Join(mr, new[] { "org" }, new[] { "org_r", "code_r" }));
            var wrongType = new DataFrame(new ISeries[] { new Int32Series("org_r", new[] { 1 }) });
            Assert.Throws<ArgumentException>(() => ml.Join(wrongType, new[] { "org" }, new[] { "org_r" }));

            // 4. RenameSeries & SeriesExtensions.Clone across ALL series types (including Array/Struct with Binary & long Utf8String) without mutating source Name
            var longStr = new Utf8StringSeries("orig_str", new[] { new string('z', 64), "short" });
            var binSrc = new BinarySeries("orig_bin", new byte[]?[] { new byte[] { 9, 8, 7 }, new byte[] { 6 } });
            var nestedArr = new ArraySeries("orig_arr", 1, binSrc.Clone());
            var nestedStruct = new StructSeries("orig_struct", new ISeries[]
            {
                longStr.Clone(),
                binSrc.Clone(),
                new ListSeries("lst", new Int32Series("off", new[] { 0, 1, 2 }), new Int32Series("lv", new[] { 11, 22 }))
            });

            var clonedArr = nestedArr.Clone();
            var clonedStruct = nestedStruct.Clone();
            Assert.Equal(new byte[] { 9, 8, 7 }, ((ArraySeries)clonedArr).Values.Get(0));
            Assert.Equal(new string('z', 64), ((StructSeries)clonedStruct).Fields[0].Get(0));

            // Join colliding columns of every type (forces RenameSeries -> _right without mutating right table)
            var leftAll = new DataFrame(new ISeries[]
            {
                new DateSeries("d", new[] { new DateTime(2025, 1, 1) }),
                new DecimalSeries("dec", new decimal?[] { 1m }),
                new BinarySeries("bin", new byte[]?[] { new byte[] { 1 } }),
                new TimeSeries("tm", new[] { TimeSpan.FromMinutes(5) }),
                EnumSeries.FromStrings("enm", new string?[] { "a" }, new[] { "a" }),
                CategoricalSeries.FromStrings("cat", new string?[] { "c" }),
                new NullSeries("nul", 1),
                new ObjectSeries("obj", new object?[] { "o" })
            });
            var rightAll = new DataFrame(new ISeries[]
            {
                new DateSeries("d", new[] { new DateTime(2025, 1, 1) }),
                new DecimalSeries("dec", new decimal?[] { 2m }),
                new BinarySeries("bin", new byte[]?[] { new byte[] { 2 } }),
                new TimeSeries("tm", new[] { TimeSpan.FromMinutes(10) }),
                EnumSeries.FromStrings("enm", new string?[] { "a" }, new[] { "a" }),
                CategoricalSeries.FromStrings("cat", new string?[] { "c" }),
                new NullSeries("nul", 1),
                new ObjectSeries("obj", new object?[] { "o2" })
            });

            var joinedAll = leftAll.Join(rightAll, "d", JoinType.Inner);
            Assert.Equal(1, joinedAll.RowCount);
            Assert.Equal("dec_right", joinedAll.GetColumn("dec_right").Name);
            Assert.Equal("bin_right", joinedAll.GetColumn("bin_right").Name);
            Assert.Equal("tm_right", joinedAll.GetColumn("tm_right").Name);
            Assert.Equal("enm_right", joinedAll.GetColumn("enm_right").Name);
            Assert.Equal("cat_right", joinedAll.GetColumn("cat_right").Name);
            Assert.Equal("nul_right", joinedAll.GetColumn("nul_right").Name);
            Assert.Equal("obj_right", joinedAll.GetColumn("obj_right").Name);
            Assert.Equal("dec", rightAll.Columns[1].Name);
        }

        [Fact]
        public async Task LazyGroupBy_DynamicRolling_SyncDeadlockSafety_AndLogicalPlanRoundTrip()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("k", new[] { 1, 1, 2, 2 }),
                new Int32Series("v", new[] { 10, 20, 30, 40 })
            });

            // 1. LazyGroupBy.Agg & AggGroups (previously crashed due to missing GroupByAggOp)
            var lgb = new LazyGroupBy(df.Lazy().Plan, new[] { "k" });
            var aggRes = await lgb.Agg(Expr.Col("v").Sum().Alias("sum_v")).Collect();
            Assert.Equal(2, aggRes.RowCount);
            Assert.Equal(30, aggRes.GetColumn("sum_v").Get(0));
            Assert.Equal(70, aggRes.GetColumn("sum_v").Get(1));

            var agGroupsRes = await lgb.AggGroups().Collect();
            Assert.Equal(2, agGroupsRes.RowCount);

            // 2. LazyFrame.GroupByDynamic & GroupByRolling with Agg(Expr[])
            var tsDf = new DataFrame(new ISeries[]
            {
                new Int64Series("ts", new[] { 0L, 1_800_000_000_000L, 3_600_000_000_000L }),
                new Int32Series("v", new[] { 5, 15, 25 })
            });
            var dynRes = await tsDf.Lazy().GroupByDynamic("ts", "1h").Agg(Expr.Col("v").Sum().Alias("s")).Collect();
            Assert.Equal(2, dynRes.RowCount);
            Assert.Equal(20, dynRes.GetColumn("s").Get(0));
            Assert.Equal(25, dynRes.GetColumn("s").Get(1));

            var rollRes = await tsDf.Lazy().GroupByRolling("ts", "1h").Agg(Expr.Col("v").Sum().Alias("s")).Collect();
            Assert.Equal(3, rollRes.RowCount);

            // 3. SynchronizationContext deadlock safety across DataFrame, GroupByBuilder, GroupByDynamicBuilder, GroupByRollingBuilder, ParquetWriter, ParquetReader
            string pqPath = Path.Combine(Path.GetTempPath(), $"polaris_syncctx_{Guid.NewGuid():N}.parquet");
            var prevCtx = SynchronizationContext.Current;
            try
            {
                SynchronizationContext.SetSynchronizationContext(new SingleThreadedDeadlockDetectingSyncContext());

                var selected = df.Select(Expr.Col("k"), (Expr.Col("v") * 2).Alias("v2"));
                var filtered = selected.Filter(Expr.Col("v2") > 20);
                var withCols = filtered.WithColumns((Expr.Col("v2") + 1).Alias("v3"));
                var sorted = withCols.Sort("v3", descending: true);
                var limited = sorted.Limit(2);
                Assert.Equal(2, limited.RowCount);

                var gbSync = df.GroupBy("k").Agg(Expr.Col("v").Sum().Alias("sum_v"));
                Assert.Equal(2, gbSync.RowCount);

                var dynSync = new GroupByDynamicBuilder(tsDf, "ts", "1h").Agg(Expr.Col("v").Sum().Alias("s"));
                Assert.Equal(2, dynSync.RowCount);

                var rollSync = new GroupByRollingBuilder(tsDf, "ts", "1h").Agg(Expr.Col("v").Sum().Alias("s"));
                Assert.Equal(3, rollSync.RowCount);

                df.WriteParquet(pqPath);
                var headers = ParquetReader.PeekHeaders(pqPath);
                Assert.Equal(new[] { "k", "v" }, headers);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prevCtx);
                if (File.Exists(pqPath)) File.Delete(pqPath);
            }

            // 4. LogicalPlanNode <-> Legacy Expression round-trip
            var complexLazy = df.Lazy()
                .Filter(e => Expr.Col("v") >= 10)
                .Join(df.Lazy(), new[] { "k" }, new[] { "k" }, JoinType.Inner)
                .Select(Expr.Col("k"), Expr.Col("v"))
                .Sort("v")
                .Limit(2);

            var legacyExpr = complexLazy.Plan;
            var roundTripped = LogicalPlanNode.FromLegacyExpression(legacyExpr);
            var opt = new QueryOptimizer();
            var optExpr = opt.Optimize(legacyExpr);
            Assert.NotNull(optExpr);
            Assert.True(CardinalityEstimator.Estimate(roundTripped) > 0);
        }

        [Fact]
        public void LeftAndOuterJoin_NullableAndNonNullArrayAndStructSeries_HandlesUnmatchedRowsCleanly()
        {
            // Direct ValidityMask.Take test for -1 sentinel with both HasNulls=false and HasNulls=true
            var maskNoNulls = new Memory.ValidityMask(3);
            var targetNoNulls = new Memory.ValidityMask(4);
            maskNoNulls.Take(targetNoNulls, new[] { 0, -1, 2, -1 });
            Assert.False(targetNoNulls.IsNull(0));
            Assert.True(targetNoNulls.IsNull(1));
            Assert.False(targetNoNulls.IsNull(2));
            Assert.True(targetNoNulls.IsNull(3));

            var maskWithNulls = new Memory.ValidityMask(3);
            maskWithNulls.SetNull(1);
            var targetWithNulls = new Memory.ValidityMask(4);
            maskWithNulls.Take(targetWithNulls, new[] { 0, 1, -1, 2 });
            Assert.False(targetWithNulls.IsNull(0));
            Assert.True(targetWithNulls.IsNull(1));
            Assert.True(targetWithNulls.IsNull(2));
            Assert.False(targetWithNulls.IsNull(3));

            // Nullable ArraySeries & StructSeries in Left and Outer joins
            var left = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1, 2, 4 }),
                new Utf8StringSeries("lval", new[] { "L1", "L2", "L4" })
            });

            var rightArrNullable = new ArraySeries("arr_null", 2, Int32Series.FromValues("av", new int?[] { 10, 20, 30, 40, 50, 60 }));
            rightArrNullable.ValidityMask.SetNull(1); // row id=3 is null array

            var rightArrNonNull = new ArraySeries("arr_nn", 2, Utf8StringSeries.FromStrings("sv", new string?[] { "a", "b", "c", "d", "e", "f" }));

            var rightStructNullable = new StructSeries("st_null", new ISeries[]
            {
                Int32Series.FromValues("x", new int?[] { 100, 200, 300 }),
                Utf8StringSeries.FromStrings("s", new string?[] { "s2", null, "s5" })
            });
            rightStructNullable.ValidityMask.SetNull(1);

            var rightStructNonNull = new StructSeries("st_nn", new ISeries[]
            {
                new Int32Series("k", new[] { 7, 8, 9 }),
                new Utf8StringSeries("t", new[] { "t2", "t3", "t5" })
            });

            var right = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 2, 3, 5 }),
                rightArrNullable,
                rightArrNonNull,
                rightStructNullable,
                rightStructNonNull
            });

            var leftJoined = left.Join(right, "id", JoinType.Left);
            Assert.Equal(3, leftJoined.RowCount);
            // Row 0 (id=1): unmatched -> all right columns null
            Assert.True(leftJoined.GetColumn("arr_null").ValidityMask.IsNull(0));
            Assert.True(leftJoined.GetColumn("arr_nn").ValidityMask.IsNull(0));
            Assert.True(leftJoined.GetColumn("st_null").ValidityMask.IsNull(0));
            Assert.True(leftJoined.GetColumn("st_nn").ValidityMask.IsNull(0));

            // Row 1 (id=2): matched -> exact values
            Assert.False(leftJoined.GetColumn("arr_null").ValidityMask.IsNull(1));
            Assert.Equal(new object?[] { 10, 20 }, (object?[])leftJoined.GetColumn("arr_null").Get(1)!);
            Assert.Equal(new object?[] { "a", "b" }, (object?[])leftJoined.GetColumn("arr_nn").Get(1)!);
            var st1 = Assert.IsAssignableFrom<IDictionary<string, object?>>(leftJoined.GetColumn("st_null").Get(1));
            Assert.Equal(100, st1["x"]);
            Assert.Equal("s2", st1["s"]);
            var stnn1 = Assert.IsAssignableFrom<IDictionary<string, object?>>(leftJoined.GetColumn("st_nn").Get(1));
            Assert.Equal(7, stnn1["k"]);
            Assert.Equal("t2", stnn1["t"]);

            // Row 2 (id=4): unmatched -> null
            Assert.True(leftJoined.GetColumn("arr_null").ValidityMask.IsNull(2));
            Assert.True(leftJoined.GetColumn("arr_nn").ValidityMask.IsNull(2));
            Assert.True(leftJoined.GetColumn("st_null").ValidityMask.IsNull(2));
            Assert.True(leftJoined.GetColumn("st_nn").ValidityMask.IsNull(2));

            // Outer join: id 2 matched, id 3 & 5 unmatched right, id 1 & 4 unmatched left
            var outerJoined = left.Join(right, "id", JoinType.Outer);
            Assert.Equal(5, outerJoined.RowCount);
            // Verify id=3 row has null arr_null/st_null (source null) and valid arr_nn/st_nn
            int row3Idx = -1;
            int row1Idx = -1;
            int row2Idx = -1;
            var idRightCol = outerJoined.GetColumn("id_right");
            var idLeftCol = outerJoined.GetColumn("id");
            for (int i = 0; i < outerJoined.RowCount; i++)
            {
                if (!idLeftCol.ValidityMask.IsNull(i) && Equals(idLeftCol.Get(i), 1)) row1Idx = i;
                if (!idLeftCol.ValidityMask.IsNull(i) && Equals(idLeftCol.Get(i), 2)) row2Idx = i;
                if (!idRightCol.ValidityMask.IsNull(i) && Equals(idRightCol.Get(i), 3)) row3Idx = i;
            }
            Assert.True(row1Idx >= 0 && row2Idx >= 0 && row3Idx >= 0);
            Assert.True(outerJoined.GetColumn("arr_null").ValidityMask.IsNull(row1Idx));
            Assert.True(outerJoined.GetColumn("st_nn").ValidityMask.IsNull(row1Idx));
            Assert.Equal(new object?[] { 10, 20 }, (object?[])outerJoined.GetColumn("arr_null").Get(row2Idx)!);
            Assert.True(outerJoined.GetColumn("arr_null").ValidityMask.IsNull(row3Idx));
            Assert.True(outerJoined.GetColumn("st_null").ValidityMask.IsNull(row3Idx));
            Assert.Equal(new object?[] { "c", "d" }, (object?[])outerJoined.GetColumn("arr_nn").Get(row3Idx)!);
        }

        [Fact]
        public async Task ParquetWriterAndReader_DateSeriesRoundtrip_UnixEpochPre1970NullsAndMultiChunkSink()
        {
            string pqPath = Path.Combine(Path.GetTempPath(), $"polaris_pq_date_{Guid.NewGuid():N}.parquet");
            string pqNonNullPath = Path.Combine(Path.GetTempPath(), $"polaris_pq_datenn_{Guid.NewGuid():N}.parquet");
            string pqSinkPath = Path.Combine(Path.GetTempPath(), $"polaris_pq_datesink_{Guid.NewGuid():N}.parquet");
            string ndjsonPath = Path.Combine(Path.GetTempPath(), $"polaris_date_src_{Guid.NewGuid():N}.ndjson");

            try
            {
                var epoch = new DateOnly(1970, 1, 1);
                int day1900 = new DateOnly(1900, 1, 1).DayNumber - epoch.DayNumber; // -25567
                int day2026 = new DateOnly(2026, 11, 27).DayNumber - epoch.DayNumber; // 20784

                // 1. Nullable DateSeries roundtrip (1970-01-01=0, 1969-12-31=-1, 1900-01-01=-25567, 2026-11-27=20784, null)
                var nullableDates = DateSeries.FromValues("dt", new int?[] { 0, -1, day1900, day2026, null });
                var dfNullable = new DataFrame(new ISeries[] { nullableDates });
                dfNullable.WriteParquet(pqPath);

                var readNullable = await LazyFrame.ScanParquet(pqPath).Collect();
                Assert.Equal(5, readNullable.RowCount);
                var dtRead = Assert.IsType<DateSeries>(readNullable.GetColumn("dt"));
                Assert.Equal(0, dtRead.Memory.Span[0]);
                Assert.Equal(-1, dtRead.Memory.Span[1]);
                Assert.Equal(day1900, dtRead.Memory.Span[2]);
                Assert.Equal(day2026, dtRead.Memory.Span[3]);
                Assert.True(dtRead.ValidityMask.IsNull(4));

                // 2. Non-null DateSeries roundtrip
                var nonNullDates = new DateSeries("dt", new[]
                {
                    new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2026, 11, 27, 0, 0, 0, DateTimeKind.Utc)
                });
                var dfNonNull = new DataFrame(new ISeries[] { nonNullDates });
                dfNonNull.WriteParquet(pqNonNullPath);

                var readNonNull = await LazyFrame.ScanParquet(pqNonNullPath).Collect();
                Assert.Equal(4, readNonNull.RowCount);
                var dtReadNn = Assert.IsType<DateSeries>(readNonNull.GetColumn("dt"));
                Assert.False(dtReadNn.ValidityMask.HasNulls);
                Assert.Equal(new[] { 0, -1, day1900, day2026 }, dtReadNn.Memory.Span.ToArray());

                // 3. Multi-chunk SinkParquet with DateSeries (cross-chunk nullability + pre-1970)
                var csvSb = new StringBuilder("d\n");
                csvSb.Append("1970-01-01\n1969-12-31\n1900-01-01\n2026-11-27\n");
                for (int i = 0; i < 6000; i++)
                {
                    csvSb.Append(i % 2 == 0 ? "1969-12-31\n" : "2026-11-27\n");
                }
                await File.WriteAllTextAsync(ndjsonPath, csvSb.ToString());

                await LazyFrame.ScanCsv(ndjsonPath)
                    .WithColumns(Expr.Col("d").Str().ToDate().Alias("dt"))
                    .Select(Expr.Col("dt"))
                    .SinkParquet(pqSinkPath);

                var readSink = await LazyFrame.ScanParquet(pqSinkPath).Collect();
                Assert.Equal(6004, readSink.RowCount);
                var dtSink = Assert.IsType<DateSeries>(readSink.GetColumn("dt"));
                Assert.Equal(0, dtSink.Memory.Span[0]);
                Assert.Equal(-1, dtSink.Memory.Span[1]);
                Assert.Equal(day1900, dtSink.Memory.Span[2]);
                Assert.Equal(day2026, dtSink.Memory.Span[3]);

                // 4. Multi-chunk WriteStreamAsync with non-null chunk 1 + nullable chunk 2
                async IAsyncEnumerable<DataFrame> DateBatches()
                {
                    yield return new DataFrame(new ISeries[]
                    {
                        new DateSeries("dt", new[]
                        {
                            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                            new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc)
                        })
                    });
                    yield return new DataFrame(new ISeries[]
                    {
                        DateSeries.FromValues("dt", new int?[] { day1900, null, day2026 })
                    });
                    await Task.CompletedTask;
                }

                await ParquetWriter.WriteStreamAsync(DateBatches(), pqSinkPath);
                var readMulti = await LazyFrame.ScanParquet(pqSinkPath).Collect();
                Assert.Equal(5, readMulti.RowCount);
                var dtMulti = Assert.IsType<DateSeries>(readMulti.GetColumn("dt"));
                Assert.Equal(0, dtMulti.Memory.Span[0]);
                Assert.Equal(-1, dtMulti.Memory.Span[1]);
                Assert.Equal(day1900, dtMulti.Memory.Span[2]);
                Assert.True(dtMulti.ValidityMask.IsNull(3));
                Assert.Equal(day2026, dtMulti.Memory.Span[4]);

                // 5. Nested StructSeries containing DateSeries + CsvWriter/JsonWriter DateSeries check
                var stDateDf = new DataFrame(new ISeries[]
                {
                    new StructSeries("st", new ISeries[]
                    {
                        DateSeries.FromValues("dt", new int?[] { 0, -1 })
                    })
                });
                stDateDf.WriteParquet(pqSinkPath);
                var stRead = await LazyFrame.ScanParquet(pqSinkPath).Collect();
                Assert.Equal("{\"dt\":\"1970-01-01\"}", stRead.GetColumn("st").Get(0));
                Assert.Equal("{\"dt\":\"1969-12-31\"}", stRead.GetColumn("st").Get(1));

                dfNonNull.WriteCsv(ndjsonPath);
                var csvText = await File.ReadAllTextAsync(ndjsonPath);
                Assert.Contains("1970-01-01", csvText);
                Assert.Contains("1969-12-31", csvText);
                Assert.Contains("1900-01-01", csvText);
                Assert.Contains("2026-11-27", csvText);

                dfNonNull.WriteJson(ndjsonPath);
                var jsonText = await File.ReadAllTextAsync(ndjsonPath);
                Assert.Contains("1970-01-01", jsonText);
                Assert.Contains("1969-12-31", jsonText);
                Assert.Contains("1900-01-01", jsonText);
                Assert.Contains("2026-11-27", jsonText);
            }
            finally
            {
                foreach (var p in new[] { pqPath, pqNonNullPath, pqSinkPath, ndjsonPath })
                    if (File.Exists(p)) File.Delete(p);
            }
        }

        [Fact]
        public void JoinAsof_MultiType_AndNullableRightKeys_HandlesNullsAndTypesCleanly()
        {
            // 1. Nullable right keys with interspersed nulls: right=[10, null, 30], left=[5, 10, 25, 35, null]
            var leftNull = new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("t", new int?[] { 5, 10, 25, 35, null }),
                new Int32Series("lv", new[] { 1, 2, 3, 4, 5 })
            });
            var rightNull = new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("t", new int?[] { 10, null, 30 }),
                new Utf8StringSeries("rv", new[] { "r10", "rnull", "r30" })
            });
            var asofNull = leftNull.Join(rightNull, "t", JoinType.AsOf);
            Assert.Equal(5, asofNull.RowCount);
            Assert.True(asofNull.GetColumn("rv").ValidityMask.IsNull(0)); // 5 < 10
            Assert.Equal("r10", asofNull.GetColumn("rv").Get(1));       // 10 -> 10
            Assert.Equal("r10", asofNull.GetColumn("rv").Get(2));       // 25 -> 10 (skips null)
            Assert.Equal("r30", asofNull.GetColumn("rv").Get(3));       // 35 -> 30 (was broken if null split binary search!)
            Assert.True(asofNull.GetColumn("rv").ValidityMask.IsNull(4)); // null -> unmatched

            // 2. Float64, DateSeries, Utf8StringSeries, BinarySeries AsOf joins
            var lMulti = new DataFrame(new ISeries[]
            {
                new Float64Series("f", new[] { 1.5, 2.5 }),
                new DateSeries("d", new[] { new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, 27, 0, 0, 0, DateTimeKind.Utc) }),
                new Utf8StringSeries("s", new[] { "b", "d" }),
                new BinarySeries("b", new byte[]?[] { new byte[] { 2 }, new byte[] { 5 } })
            });
            var rMulti = new DataFrame(new ISeries[]
            {
                new Float64Series("f", new[] { 1.0, 2.0 }),
                new DateSeries("d", new[] { new DateTime(1969, 12, 30, 0, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) }),
                new Utf8StringSeries("s", new[] { "a", "c" }),
                new BinarySeries("b", new byte[]?[] { new byte[] { 1 }, new byte[] { 3 } }),
                new Int32Series("tag", new[] { 100, 200 })
            });

            Assert.Equal(200, lMulti.JoinAsof(rMulti, "f").GetColumn("tag").Get(1));
            Assert.Equal(200, lMulti.JoinAsof(rMulti, "d").GetColumn("tag").Get(1));
            Assert.Equal(200, lMulti.JoinAsof(rMulti, "s").GetColumn("tag").Get(1));
            Assert.Equal(200, lMulti.JoinAsof(rMulti, "b").GetColumn("tag").Get(1));
        }

        private sealed class BufferSegment : ReadOnlySequenceSegment<byte>
        {
            public BufferSegment(ReadOnlyMemory<byte> memory)
            {
                Memory = memory;
            }

            public BufferSegment Append(ReadOnlyMemory<byte> memory)
            {
                var next = new BufferSegment(memory)
                {
                    RunningIndex = RunningIndex + Memory.Length
                };
                Next = next;
                return next;
            }
        }
    }
}
