using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Glacier.Polaris;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Data;
using Glacier.Polaris.IO;
using Glacier.Polaris.Memory;
using Xunit;

namespace Glacier.Polaris.Tests
{
    public class RemediationVerificationTests
    {
        [Fact]
        public async Task DataFrame_SortBy_And_SortByEager_SortSingleAndMultiColumn()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("grp", new[] { 2, 1, 2, 1 }),
                new Int32Series("val", new[] { 10, 40, 5, 20 }),
                Utf8StringSeries.FromStrings("label", new string?[] { "b1", "a1", "b0", "a0" })
            });

            var s1 = await df.SortBy("val");
            Assert.Equal(new[] { 5, 10, 20, 40 }, ((Int32Series)s1.GetColumn("val")).Memory.Span.ToArray());
            Assert.Equal("b0", s1.GetColumn("label").Get(0));

            var s2 = df.SortByEager("val", descending: true);
            Assert.Equal(new[] { 40, 20, 10, 5 }, ((Int32Series)s2.GetColumn("val")).Memory.Span.ToArray());

            var s3 = await df.SortBy(new[] { "grp", "val" }, new[] { false, true });
            Assert.Equal(new[] { 1, 1, 2, 2 }, ((Int32Series)s3.GetColumn("grp")).Memory.Span.ToArray());
            Assert.Equal(new[] { 40, 20, 10, 5 }, ((Int32Series)s3.GetColumn("val")).Memory.Span.ToArray());

            var s4 = df.SortByEager("grp", "val");
            Assert.Equal(new[] { 1, 1, 2, 2 }, ((Int32Series)s4.GetColumn("grp")).Memory.Span.ToArray());
            Assert.Equal(new[] { 20, 40, 5, 10 }, ((Int32Series)s4.GetColumn("val")).Memory.Span.ToArray());
        }

        [Fact]
        public async Task DataFrame_And_LazyFrame_Unique_MultiColumn_Subset_And_NullDistinction()
        {
            // Rows:
            // 0: (null, "x", 100)
            // 1: (0,    "x", 200) -> must NOT collide with (null, "x")
            // 2: (null, "x", 300) -> duplicate on subset ("a", "b")
            // 3: (0,    "",  400)
            // 4: (0,    null,500) -> must NOT collide with (0, "")
            var df = new DataFrame(new ISeries[]
            {
                Int32Series.FromValues("a", new int?[] { null, 0, null, 0, 0 }),
                Utf8StringSeries.FromStrings("b", new string?[] { "x", "x", "x", "", null }),
                new Int32Series("payload", new[] { 100, 200, 300, 400, 500 })
            });

            // Full-row unique: all 5 rows differ because payload differs
            var fullUnique = df.Unique();
            Assert.Equal(5, fullUnique.RowCount);

            // Subset ("a", "b") unique with maintainOrder=true: row 2 duplicates row 0, keeping 4 rows [0, 1, 3, 4]
            var subsetUnique = df.Unique(new[] { "a", "b" }, maintainOrder: true);
            Assert.Equal(4, subsetUnique.RowCount);
            Assert.Equal(new[] { 100, 200, 400, 500 }, ((Int32Series)subsetUnique.GetColumn("payload")).Memory.Span.ToArray());
            Assert.True(subsetUnique.GetColumn("a").ValidityMask.IsNull(0));
            Assert.False(subsetUnique.GetColumn("a").ValidityMask.IsNull(1));
            Assert.False(subsetUnique.GetColumn("b").ValidityMask.IsNull(2));
            Assert.True(subsetUnique.GetColumn("b").ValidityMask.IsNull(3));

            // LazyFrame.Unique with subset
            var lazyUnique = await df.Lazy().Unique(new[] { "a", "b" }, maintainOrder: true).Collect();
            Assert.Equal(4, lazyUnique.RowCount);
            Assert.Equal(new[] { 100, 200, 400, 500 }, ((Int32Series)lazyUnique.GetColumn("payload")).Memory.Span.ToArray());
        }

        [Fact]
        public void UniqueKernels_DistinguishNullFromZeroAndEmpty_RetainingFirstNull_AcrossTypes()
        {
            using var i32 = Int32Series.FromValues("i32", new int?[] { 5, null, 0, null, 0 });
            Assert.Equal(3, (int)UniqueKernels.NUnique(i32).Get(0)!);
            Assert.Equal(new[] { 0, 1, 2 }, UniqueKernels.UniqueIndices(i32));
            using var uI32 = UniqueKernels.Unique(i32);
            Assert.Equal(3, uI32.Length);
            Assert.Equal(5, uI32.Get(0));
            Assert.True(uI32.ValidityMask.IsNull(1));
            Assert.Equal(0, uI32.Get(2));

            using var f64 = Float64Series.FromValues("f64", new double?[] { null, 0.0, null, -0.0, 1.5 });
            Assert.Equal(3, (int)UniqueKernels.NUnique(f64).Get(0)!);
            using var uF64 = UniqueKernels.Unique(f64);
            Assert.Equal(3, uF64.Length);
            Assert.True(uF64.ValidityMask.IsNull(0));
            Assert.False(uF64.ValidityMask.IsNull(1));

            using var str = Utf8StringSeries.FromStrings("s", new string?[] { "", null, "", null, "abc" });
            Assert.Equal(3, (int)UniqueKernels.NUnique(str).Get(0)!);
            Assert.Equal(new[] { 0, 1, 4 }, UniqueKernels.UniqueIndices(str));
            using var uStr = UniqueKernels.Unique(str);
            Assert.Equal(3, uStr.Length);
            Assert.Equal("", uStr.Get(0));
            Assert.True(uStr.ValidityMask.IsNull(1));
            Assert.Equal("abc", uStr.Get(2));

            using var bl = BooleanSeries.FromValues("b", new bool?[] { false, null, false, true, null });
            Assert.Equal(3, (int)UniqueKernels.NUnique(bl).Get(0)!);
            Assert.Equal(new[] { 0, 1, 3 }, UniqueKernels.UniqueIndices(bl));

            using var i64 = Int64Series.FromValues("i64", new long?[] { null, 0L, null });
            Assert.Equal(2, (int)UniqueKernels.NUnique(i64).Get(0)!);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(63)]
        [InlineData(64)]
        [InlineData(65)]
        [InlineData(127)]
        [InlineData(129)]
        public void ValidityMask_TailBits_NeverInflateNullCount(int length)
        {
            var allNull = new ValidityMask(length, setAllValid: false);
            Assert.Equal(length, allNull.NullCount);
            Assert.Equal(length > 0, allNull.HasNulls);

            var mask = new ValidityMask(length, setAllValid: true);
            Assert.Equal(0, mask.NullCount);
            Assert.False(mask.HasNulls);

            mask.SetAllNull();
            Assert.Equal(length, mask.NullCount);

            mask.Not();
            Assert.Equal(0, mask.NullCount);
            Assert.False(mask.HasNulls);

            mask.Not();
            Assert.Equal(length, mask.NullCount);

            if (length > 0)
            {
                // Corrupt raw high bits via AsWritableSpan and verify NullCount stays bounded
                var raw = mask.AsWritableSpan();
                raw[^1] = 0UL;
                Assert.Equal(length, mask.NullCount);
                raw[^1] = ulong.MaxValue;
                Assert.InRange(mask.NullCount, 0, length);
            }
        }

        [Fact]
        public void MemoryAndArrowExports_SafeAfterSourceDisposal_AndSchemaRegistrySafe()
        {
            ReadOnlyMemory<byte> intBytes;
            ReadOnlyMemory<byte> nullBitmap;
            using (var s = Int32Series.FromValues("x", new int?[] { 10, null, 30 }))
            {
                intBytes = s.AsBytesMemory();
                nullBitmap = s.ValidityMask.GetNullBitmapMemory();
            }
            Assert.Equal(12, intBytes.Length);
            Assert.Single(nullBitmap.ToArray());

            Glacier.Formats.Arrow.ArrowColumn boolArrow;
            using (var bs = BooleanSeries.FromValues("flag", new bool?[] { true, null, false, true }))
            {
                boolArrow = bs.ToArrowColumn();
            }
            Assert.Equal(4, boolArrow.Length);
            Assert.Equal(1, boolArrow.NullCount);
            Assert.Equal(0b0000_1001, boolArrow.DataBuffer.Span[0] & 0b0000_1111);

            var objSeries = new ObjectSeries("obj", new object?[] { "hello", null, 42 });
            var objArrow = objSeries.ToArrowColumn();
            Assert.Equal(3, objArrow.Length);
            Assert.Equal(1, objArrow.NullCount);
            Assert.True(objArrow.DataBuffer.Length >= 7); // "hello" + "42" still valid after utf8 disposed!

            SchemaRegistry.RegisterSchema(new Dictionary<string, Type>
            {
                ["id"] = typeof(int),
                ["score"] = typeof(float)
            });
            Assert.Equal(typeof(float), SchemaRegistry.GetType("score"));
            Assert.True(SchemaRegistry.GetFastLookupTable().Length >= 2);
        }

        [Fact]
        public async Task QueryOptimizer_SoundnessGuards_PreventBrokenPlans()
        {
            var df = new DataFrame(new ISeries[]
            {
                new Int32Series("a", new[] { 1, 2, 3, 4 }),
                new Int32Series("b", new[] { 40, 30, 20, 10 }),
                new Int32Series("grp", new[] { 1, 1, 2, 2 })
            });

            // 1. Filter -> Select blocked on computed alias, executes cleanly without missing column crash
            var computedAliasResult = await df.Lazy()
                .Select((Expr.Col("a") * 10).Alias("scaled"))
                .Filter(e => Expr.Col("scaled") > 20)
                .Collect();
            Assert.Equal(new[] { 30, 40 }, ((Int32Series)computedAliasResult.GetColumn("scaled")).Memory.Span.ToArray());

            // 2. Filter -> Select pass-through alias rewrites column name soundly
            var passThroughAliasResult = await df.Lazy()
                .Select(Expr.Col("a").Alias("renamed_a"))
                .Filter(e => Expr.Col("renamed_a") >= 3)
                .Collect();
            Assert.Equal(new[] { 3, 4 }, ((Int32Series)passThroughAliasResult.GetColumn("renamed_a")).Memory.Span.ToArray());

            // 3. Limit -> Select blocked on aggregation (Sum over all 4 rows = 10, NOT sum of 1 row = 1)
            var aggLimitResult = await df.Lazy()
                .Select(Expr.Col("a").Sum().Alias("total"))
                .Limit(1)
                .Collect();
            Assert.Equal(1, aggLimitResult.RowCount);
            Assert.Equal(10, Convert.ToInt64(aggLimitResult.GetColumn("total").Get(0)));

            // 4. DataFrameOp leaf schema resolution + Left join preserves unmatched rows under constant filter
            var leftDf = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1, 2 }),
                new Int32Series("lval", new[] { 10, 20 })
            });
            var rightDf = new DataFrame(new ISeries[]
            {
                new Int32Series("id", new[] { 1 }),
                new Int32Series("rval", new[] { 99 })
            });

            var leftJoinFiltered = await leftDf.Lazy()
                .Join(rightDf.Lazy(), "id", JoinType.Left)
                .Filter(e => Expr.Col("lval") >= 10)
                .Collect();
            Assert.Equal(2, leftJoinFiltered.RowCount);
            Assert.True(leftJoinFiltered.GetColumn("rval").ValidityMask.IsNull(1));

            // 5. SortOp column not projected away before sort
            string csvPath = Path.Combine(Path.GetTempPath(), $"polaris_sort_proj_{Guid.NewGuid():N}.csv");
            try
            {
                await File.WriteAllTextAsync(csvPath, "keep,sort_key\nx,30\ny,10\nz,20\n");
                var sortedProj = await LazyFrame.ScanCsv(csvPath)
                    .Sort("sort_key")
                    .Select(Expr.Col("keep"))
                    .Collect();
                Assert.Single(sortedProj.Columns);
                Assert.Equal("y", sortedProj.GetColumn("keep").Get(0));
                Assert.Equal("z", sortedProj.GetColumn("keep").Get(1));
                Assert.Equal("x", sortedProj.GetColumn("keep").Get(2));
            }
            finally
            {
                if (File.Exists(csvPath)) File.Delete(csvPath);
            }

            // 6. Multi-way Inner Join preserves exact output column order
            var d1 = new DataFrame(new ISeries[] { new Int32Series("k", new[] { 1, 2 }), new Int32Series("c1", new[] { 10, 20 }) });
            var d2 = new DataFrame(new ISeries[] { new Int32Series("k", new[] { 1, 2, 3, 4 }), new Int32Series("c2", new[] { 100, 200, 300, 400 }) });
            var d3 = new DataFrame(new ISeries[] { new Int32Series("k", new[] { 2 }), new Int32Series("c3", new[] { 2000 }) });

            var joined3 = await d1.Lazy()
                .Join(d2.Lazy(), "k", JoinType.Inner)
                .Join(d3.Lazy(), "k", JoinType.Inner)
                .Collect();
            Assert.Equal(1, joined3.RowCount);
            Assert.Equal(new[] { "k", "c1", "c2", "c3" }, joined3.Columns.Select(c => c.Name).ToArray());
        }

        [Fact]
        public void GpuPolarisAccelerator_QuarantinesFakeAmdPath_AndFloat32PropagatesValidityMask()
        {
            Assert.False(GpuPolarisAccelerator.IsAmdAvailable);
            Assert.Throws<PlatformNotSupportedException>(() => GpuPolarisAccelerator.VectorSub(new float[] { 1f }, new float[] { 2f }, new float[1], GpuTarget.Amd));
            Assert.Throws<PlatformNotSupportedException>(() => GpuPolarisAccelerator.VectorFma(new float[] { 1f }, new float[] { 2f }, new float[] { 3f }, new float[1], GpuTarget.Amd));

            using var a = Float32Series.FromValues("a", new float?[] { 4f, null, 9f });
            using var b = Float32Series.FromValues("b", new float?[] { 2f, 5f, null });

            using var sumSeries = a.Add(b);
            Assert.False(sumSeries.ValidityMask.IsNull(0));
            Assert.Equal(6f, sumSeries.Memory.Span[0]);
            Assert.True(sumSeries.ValidityMask.IsNull(1));
            Assert.True(sumSeries.ValidityMask.IsNull(2));

            using var sqrtSeries = a.Sqrt();
            Assert.False(sqrtSeries.ValidityMask.IsNull(0));
            Assert.Equal(2f, sqrtSeries.Memory.Span[0]);
            Assert.True(sqrtSeries.ValidityMask.IsNull(1));
            Assert.False(sqrtSeries.ValidityMask.IsNull(2));
            Assert.Equal(3f, sqrtSeries.Memory.Span[2]);

            Assert.Equal(13f, a.Sum());
        }

        [Fact]
        public async Task JsonReader_MultiColumn_NdJson_WithoutTrailingNewline_JsonArray_Projection_AndExactChunkSize()
        {
            string ndjsonPath = Path.Combine(Path.GetTempPath(), $"polaris_multi_{Guid.NewGuid():N}.ndjson");
            string arrayPath = Path.Combine(Path.GetTempPath(), $"polaris_array_{Guid.NewGuid():N}.json");

            try
            {
                // NDJSON WITHOUT trailing newline, mixed types + nulls + nested skip + >int.MaxValue promotion
                string ndjson =
                    "{\"id\": 1, \"big\": 5000000000, \"score\": 1.5, \"active\": true, \"name\": \"alpha\", \"nested\": {\"x\": [1,2]}}\n" +
                    "{\"id\": 2, \"big\": null, \"score\": 2, \"active\": false, \"name\": null}\n" +
                    "{\"id\": null, \"big\": 42, \"score\": null, \"active\": null, \"name\": \"gamma\"}";
                await File.WriteAllTextAsync(ndjsonPath, ndjson);

                var df = DataFrame.ReadJson(ndjsonPath);
                Assert.Equal(3, df.RowCount);
                Assert.Equal(6, df.Columns.Count);
                Assert.Equal("{\"x\":[1,2]}", df.GetColumn("nested").Get(0));
                Assert.True(df.GetColumn("nested").ValidityMask.IsNull(1));

                var idCol = (Int32Series)df.GetColumn("id");
                Assert.Equal(1, idCol.Memory.Span[0]);
                Assert.Equal(2, idCol.Memory.Span[1]);
                Assert.True(idCol.ValidityMask.IsNull(2));

                var bigCol = (Int64Series)df.GetColumn("big");
                Assert.Equal(5_000_000_000L, bigCol.Memory.Span[0]);
                Assert.True(bigCol.ValidityMask.IsNull(1));
                Assert.Equal(42L, bigCol.Memory.Span[2]);

                var scoreCol = (Float64Series)df.GetColumn("score");
                Assert.Equal(1.5, scoreCol.Memory.Span[0]);
                Assert.Equal(2.0, scoreCol.Memory.Span[1]);
                Assert.True(scoreCol.ValidityMask.IsNull(2));

                var activeCol = (BooleanSeries)df.GetColumn("active");
                Assert.True(activeCol.Memory.Span[0]);
                Assert.False(activeCol.Memory.Span[1]);
                Assert.True(activeCol.ValidityMask.IsNull(2));

                var nameCol = (Utf8StringSeries)df.GetColumn("name");
                Assert.Equal("alpha", nameCol.GetString(0));
                Assert.True(nameCol.ValidityMask.IsNull(1));
                Assert.Equal("gamma", nameCol.GetString(2));

                // Streaming chunk slicing: chunkSize=2 over 3 rows yields batches of exact RowCount 2 and 1
                var batchSizes = new List<int>();
                var chunkReader = new JsonReader(ndjsonPath, chunkSize: 2, columns: new[] { "id", "name" });
                await foreach (var batch in chunkReader.ReadNdJsonAsync())
                {
                    batchSizes.Add(batch.RowCount);
                    Assert.Equal(2, batch.Columns.Count);
                }
                Assert.Equal(new[] { 2, 1 }, batchSizes);

                // JSON array [...] input + LazyFrame.ScanJson projection pushdown
                string jsonArray = "[{\"k\": 10, \"v\": \"ten\", \"extra\": 99}, {\"k\": 20, \"v\": null, \"extra\": 88}]";
                await File.WriteAllTextAsync(arrayPath, jsonArray);

                var arrayDf = await LazyFrame.ScanJson(arrayPath)
                    .Select(Expr.Col("v"), Expr.Col("k"))
                    .Collect();
                Assert.Equal(2, arrayDf.RowCount);
                Assert.Equal(new[] { "v", "k" }, arrayDf.Columns.Select(c => c.Name).ToArray());
                Assert.Equal("ten", arrayDf.GetColumn("v").Get(0));
                Assert.True(arrayDf.GetColumn("v").ValidityMask.IsNull(1));
                Assert.Equal(20, arrayDf.GetColumn("k").Get(1));
            }
            finally
            {
                if (File.Exists(ndjsonPath)) File.Delete(ndjsonPath);
                if (File.Exists(arrayPath)) File.Delete(arrayPath);
            }
        }
    }
}
