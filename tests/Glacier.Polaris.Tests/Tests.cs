using System;
using System.Linq.Expressions;
using Xunit;
using Glacier.Polaris;
using Glacier.Polaris.Compute;
using Glacier.Polaris.Memory;

namespace Glacier.Polaris.Tests
{
    public class PolarsTests
    {
        [Fact]
        public void Test_KleeneLogic_TruthTables()
        {
            // True | NA = True
            Assert.True((KleeneBool.True | KleeneBool.NA).IsTrue);
            // False | NA = NA
            Assert.True((KleeneBool.False | KleeneBool.NA).IsNA);
            // True & NA = NA
            Assert.True((KleeneBool.True & KleeneBool.NA).IsNA);
            // False & NA = False
            Assert.True((KleeneBool.False & KleeneBool.NA).IsFalse);
            // !NA = NA
            Assert.True((!KleeneBool.NA).IsNA);
        }

        [Fact]
        public void Test_ComputeKernels_Sum()
        {
            // Create a span of integers
            int[] data = new int[100];
            for (int i = 0; i < 100; i++) data[i] = 1; // sum should be 100

            long sum = ComputeKernels.Sum(data);
            Assert.Equal(100, sum);
        }

        [Fact]
        public void Test_ComputeKernels_BranchlessMask()
        {
            int[] data = { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
            int[] mask = {  1,  0,  1,  0,  1,  1,  0,  0,  1,   0 };
            int[] result = new int[10];

            ComputeKernels.BranchlessMask(data, mask, result);

            Assert.Equal(10, result[0]);
            Assert.Equal(0, result[1]);
            Assert.Equal(30, result[2]);
            Assert.Equal(0, result[3]);
            Assert.Equal(50, result[4]);
            Assert.Equal(60, result[5]);
            Assert.Equal(0, result[6]);
            Assert.Equal(0, result[7]);
            Assert.Equal(90, result[8]);
            Assert.Equal(0, result[9]);
        }

        [Fact]
        public void Test_LazyFrame_AST_Construction()
        {
            var df = new LazyFrame(Expression.Constant(new LazyFrame(Expression.Empty())));
            var filtered = df.Filter(e => Expr.Col("A") > 5);
            var selected = filtered.Select(e => Expr.Col("A"));

            Assert.NotNull(selected.Plan);
            Assert.True(selected.Plan is MethodCallExpression);
            
            // Check if Filter is in the expression tree
            var methodCall = (MethodCallExpression)selected.Plan;
            Assert.Equal("SelectOp", methodCall.Method.Name);
            
            var prevCall = (MethodCallExpression)methodCall.Arguments[0];
            Assert.Equal("FilterOp", prevCall.Method.Name);
        }

        [Fact]
        public void Test_QueryOptimizer_PredicatePushdown()
        {
            var df = LazyFrame.ScanCsv("test.csv");
            
            // Build AST: Filter(Select(ScanCsv, cols), pred)
            var plan = df.Select(e => Expr.Col("A")).Filter(e => Expr.Col("A") > 5).Plan;
            
            var optimizer = new QueryOptimizer();
            var optimizedPlan = optimizer.Optimize(plan);

            // Optimized should be: Select(Filter(ScanCsv, pred), cols)
            Assert.True(optimizedPlan is MethodCallExpression);
            var rootCall = (MethodCallExpression)optimizedPlan;
            Assert.Equal("SelectOp", rootCall.Method.Name);

            var childCall = (MethodCallExpression)rootCall.Arguments[0];
            Assert.Equal("FilterOp", childCall.Method.Name);
        }

        [Fact]
        public async System.Threading.Tasks.Task Test_ExecutionEngine_Evaluate()
        {
            // We need a dummy csv file for the reader
            System.IO.File.WriteAllText("dummy.csv", "Column1,B,C\n1,2.5,\"apple\"\n10,5.1,\"banana\"\n");

            var df = LazyFrame.ScanCsv("dummy.csv");
            // AST: (Column1 * 2) + Column1
            var plan = df.Select(e => (Expr.Col("Column1") * 2) + Expr.Col("Column1")).Filter(e => Expr.Col("Column1") > 5).Plan;
            
            var optimizer = new QueryOptimizer();
            var optimizedPlan = optimizer.Optimize(plan);

            var engine = new ExecutionEngine();
            int count = 0;

            await foreach (var batch in engine.ExecuteAsync(optimizedPlan))
            {
                Assert.NotNull(batch);
                count++;

                // The filter Expr.Col("Column1") > 5 should only keep the row with value 10
                // The select (Expr.Col("Column1") * 2) + Expr.Col("Column1") produces (10 * 2) + 10 = 30
                var sumSeries = (Glacier.Polaris.Data.Int32Series)batch.Columns[0];
                Assert.Equal(1, sumSeries.Length);
                Assert.Equal(30, sumSeries.Memory.Span[0]);
            }

            Assert.True(count > 0);
            System.IO.File.Delete("dummy.csv");
        }

        [Fact]
        public void Test_StronglyTypedSeries_Memory()
        {
            var series = new Glacier.Polaris.Data.Int32Series("A", 100);
            Assert.Equal(100, series.Length);
            Assert.Equal("A", series.Name);
            Assert.Equal(typeof(int), series.DataType);

            // Access span and modify
            var span = series.Memory.Span;
            span[0] = 42;
            Assert.Equal(42, span[0]);

            // Set mask
            series.ValidityMask.SetNull(0);
            Assert.False(series.ValidityMask.IsValid(0));

            series.Dispose();

            // After dispose, accessing memory should throw ObjectDisposedException
            Assert.Throws<ObjectDisposedException>(() => series.Memory);
        }

        [Fact]
        public void Test_ComputeHashes_SIMD()
        {
            // Placeholder for new tests
        }

        [Fact]
        public async System.Threading.Tasks.Task Test_ChunkScheduler_ParallelExecution()
        {
            int[] chunks = { 1, 2, 3, 4, 5, 6, 7, 8 };
            int sum = 0;

            // Generate an IAsyncEnumerable from the array
            async System.Collections.Generic.IAsyncEnumerable<int> GetChunks()
            {
                foreach (var chunk in chunks)
                {
                    yield return chunk;
                    await System.Threading.Tasks.Task.Yield();
                }
            }

            await ChunkScheduler.ProcessChunksAsync(GetChunks(), async (chunk, ct) =>
            {
                System.Threading.Interlocked.Add(ref sum, chunk);
                await System.Threading.Tasks.Task.CompletedTask;
            });

            Assert.Equal(36, sum);
        }

        [Fact]
        public void Test_SchemaRegistry_NonGCHeap()
        {
            var schema = new System.Collections.Generic.Dictionary<string, Type>
            {
                { "A", typeof(int) },
                { "B", typeof(double) }
            };

            Glacier.Polaris.Data.SchemaRegistry.RegisterSchema(schema);

            Assert.Equal(typeof(int), Glacier.Polaris.Data.SchemaRegistry.GetType("A"));
            Assert.Equal(typeof(double), Glacier.Polaris.Data.SchemaRegistry.GetType("B"));
            Assert.Null(Glacier.Polaris.Data.SchemaRegistry.GetType("C"));

            var fastTable = Glacier.Polaris.Data.SchemaRegistry.GetFastLookupTable();
            Assert.Equal(2, fastTable.Length);
            Assert.Contains(typeof(int), fastTable.ToArray());
        }

        [Fact]
        public async System.Threading.Tasks.Task Test_JsonReader_ReadNdJson()
        {
            string path = "test.ndjson";
            await System.IO.File.WriteAllTextAsync(path, "{\"val\": 10}\n{\"val\": 20}\n{\"val\": 30}\n");

            var reader = new Glacier.Polaris.IO.JsonReader(path);
            int total = 0;
            int count = 0;

            await foreach (var df in reader.ReadNdJsonAsync())
            {
                var series = (Glacier.Polaris.Data.Int32Series)df.Columns[0];
                var span = series.Memory.Span;
                // We know rowCount isn't exposed yet, but for the test we'll check first few
                total += span[0] + span[1] + span[2];
                count++;
            }

            Assert.Equal(60, total);
            Assert.True(count > 0);

            System.IO.File.Delete(path);
        }

        [Fact]
        public void Test_SortKernels()
        {
            int[] data = { 5, 2, 8, 1, 9 };
            Glacier.Polaris.Compute.SortKernels.Sort(data);
            Assert.Equal(new[] { 1, 2, 5, 8, 9 }, data);
        }

        [Fact]
        public void Test_JoinKernels_InnerJoin()
        {
            var left = new Glacier.Polaris.Data.Int32Series("L", 3);
            left.Memory.Span[0] = 1;
            left.Memory.Span[1] = 2;
            left.Memory.Span[2] = 3;

            var right = new Glacier.Polaris.Data.Int32Series("R", 3);
            right.Memory.Span[0] = 2;
            right.Memory.Span[1] = 3;
            right.Memory.Span[2] = 4;

            var result = Glacier.Polaris.Compute.JoinKernels.InnerJoin(left, right);

            // Matches should be on keys 2 and 3
            // Key 2: LeftIdx 1, RightIdx 0
            // Key 3: LeftIdx 2, RightIdx 1
            Assert.Equal(2, result.LeftIndices.Length);
            Assert.Contains(1, result.LeftIndices);
            Assert.Contains(2, result.LeftIndices);
        }

        [Fact]
        public async System.Threading.Tasks.Task Test_ColumnWiseArithmetic()
        {
            var left = new Glacier.Polaris.Data.Int32Series("A", 3);
            left.Memory.Span[0] = 1;
            left.Memory.Span[1] = 2;
            left.Memory.Span[2] = 3;

            var right = new Glacier.Polaris.Data.Int32Series("B", 3);
            right.Memory.Span[0] = 10;
            right.Memory.Span[1] = 20;
            right.Memory.Span[2] = 30;

            var df = new DataFrame(new ISeries[] { left, right });
            
            // df.Select(e => Expr.Col("A") + Expr.Col("B"))
            Glacier.Polaris.DataFrame? resultDf = null;
            await foreach (var batch in df.Lazy().Select(e => Expr.Col("A") + Expr.Col("B")).CollectAsync())
            {
                resultDf = batch;
                break;
            }

            Assert.NotNull(resultDf);
            var resultSeries = (Glacier.Polaris.Data.Int32Series)resultDf.Columns[0];
            Assert.Equal(11, resultSeries.Memory.Span[0]);
            Assert.Equal(22, resultSeries.Memory.Span[1]);
            Assert.Equal(33, resultSeries.Memory.Span[2]);
        }

        [Fact]
        public void Test_ValidityMask()
        {
            var mask = new ValidityMask(100);
            Assert.True(mask.IsValid(0));
            Assert.True(mask.IsValid(99));

            mask.SetNull(50);
            Assert.False(mask.IsValid(50));
            Assert.True(mask.IsValid(49));
            Assert.True(mask.IsValid(51));
        }
        [Fact]
        public void Test_Joins()
        {
            var leftKeys = new Glacier.Polaris.Data.Int32Series("Key", 3);
            leftKeys.Memory.Span[0] = 1;
            leftKeys.Memory.Span[1] = 2;
            leftKeys.Memory.Span[2] = 3;

            var leftVals = new Glacier.Polaris.Data.Float64Series("Value", 3);
            leftVals.Memory.Span[0] = 1.1;
            leftVals.Memory.Span[1] = 2.2;
            leftVals.Memory.Span[2] = 3.3;

            var leftDf = new DataFrame(new ISeries[] { leftKeys, leftVals });

            var rightKeys = new Glacier.Polaris.Data.Int32Series("Key", 2);
            rightKeys.Memory.Span[0] = 2;
            rightKeys.Memory.Span[1] = 4;

            var rightVals = new Glacier.Polaris.Data.Float64Series("Value", 2);
            rightVals.Memory.Span[0] = 20.0;
            rightVals.Memory.Span[1] = 40.0;

            var rightDf = new DataFrame(new ISeries[] { rightKeys, rightVals });

            // Inner Join
            var inner = leftDf.JoinInner(rightDf, "Key");
            Assert.Equal(1, inner.Columns[0].Length); // Only key '2' matches
            Assert.Equal(2, ((Glacier.Polaris.Data.Int32Series)inner.Columns[0]).Memory.Span[0]);

            // Left Join
            var left = leftDf.JoinLeft(rightDf, "Key");
            Assert.Equal(3, left.Columns[0].Length);
            var leftJoinedVals = (Glacier.Polaris.Data.Float64Series)left.Columns[2];
            Assert.Equal("Value_right", leftJoinedVals.Name);
            
            int leftNullCount = 0;
            for (int i = 0; i < 3; i++) if (leftJoinedVals.ValidityMask.IsNull(i)) leftNullCount++;
            Assert.Equal(2, leftNullCount);

            // Outer Join
            var outer = leftDf.JoinOuter(rightDf, "Key");
            Assert.Equal(4, outer.Columns[0].Length); // 1, 2, 3, (null for left key on 4)
            var outerLeftKeys = (Glacier.Polaris.Data.Int32Series)outer.Columns[0];
            int outerNullKeyCount = 0;
            for (int i = 0; i < 4; i++) if (outerLeftKeys.ValidityMask.IsNull(i)) outerNullKeyCount++;
            Assert.Equal(1, outerNullKeyCount);
            
            // Cross Join
            var cross = leftDf.JoinCross(rightDf);
            Assert.Equal(6, cross.Columns[0].Length);
        }
    }
}
