using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Buffers;
using Glacier.Polaris;
using Glacier.Polaris.Data;
using Glacier.Polaris.Compute;

namespace Glacier.Polaris.Benchmarks
{
    /// <summary>
    /// Comprehensive benchmark suite, mirroring Python Polars benchmarks
    /// (benchmarks/python_bench_full.py) for direct comparison.
    /// </summary>
    public class FullSuiteBenchmark
    {
        private const int SmallN = 1_000_000;
        private const int LargeN = 10_000_000;
        private const int Seed = 42;

        private static Int32Series s_i32Small = null!;
        private static Int32Series s_i32Large = null!;
        private static Float64Series s_f64Small = null!;
        private static Float64Series s_f64Large = null!;
        private static Utf8StringSeries s_stringSmall = null!;
        private static Int32Series s_keySmall = null!;
        private static Int32Series s_keyLarge = null!;

        public static void Run()
        {
            Console.WriteLine("=== Pre-creating benchmark data... ===");
            PrecreateData();

            Console.WriteLine($"\nGlacier.Polaris Benchmarks");
            Console.WriteLine($"Seed: {Seed}, SmallN: {SmallN:N0}, LargeN: {LargeN:N0}");
            Console.WriteLine(new string('=', 80));

            RunAll();

            Console.WriteLine($"\n{new string('=', 80)}");
            Console.WriteLine("Done.");
        }

        private static void PrecreateData()
        {
            var rng = new Random(Seed);

            var i32data = new int[LargeN];
            for (int i = 0; i < LargeN; i++) i32data[i] = rng.Next();
            s_i32Large = new Int32Series("a", i32data);

            rng = new Random(Seed);
            i32data = new int[SmallN];
            for (int i = 0; i < SmallN; i++) i32data[i] = rng.Next();
            s_i32Small = new Int32Series("a", i32data);

            rng = new Random(Seed);
            var f64data = new double[LargeN];
            for (int i = 0; i < LargeN; i++) f64data[i] = rng.NextDouble();
            s_f64Large = new Float64Series("a", f64data);

            rng = new Random(Seed);
            f64data = new double[SmallN];
            for (int i = 0; i < SmallN; i++) f64data[i] = rng.NextDouble();
            s_f64Small = new Float64Series("a", f64data);

            var cats = new[] { "apple", "banana", "cherry", "date", "elderberry", "fig", "grape" };
            rng = new Random(Seed);
            var strData = new string[SmallN];
            for (int i = 0; i < SmallN; i++) strData[i] = cats[rng.Next(cats.Length)];
            s_stringSmall = new Utf8StringSeries("name", strData);

            rng = new Random(Seed);
            var keyData = new int[SmallN];
            for (int i = 0; i < SmallN; i++) keyData[i] = rng.Next(1000);
            s_keySmall = new Int32Series("key", keyData);

            rng = new Random(Seed);
            keyData = new int[LargeN];
            for (int i = 0; i < LargeN; i++) keyData[i] = rng.Next(1000);
            s_keyLarge = new Int32Series("key", keyData);

            Console.WriteLine("  Pre-creation complete.");
        }

        private static double Time(string label, Action action)
        {
            action(); // warmup
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < 3; i++) action();
            sw.Stop();
            double avgMs = sw.Elapsed.TotalMilliseconds / 3;
            Console.WriteLine($"  {label,-55} {avgMs,10:F2} ms avg");
            return avgMs;
        }

        private static void RunAll()
        {
            // ── Creation ───────────────────────────────────
            Time("Creation_Int32(N=1M)",  () => new Int32Series("a", SmallN));
            Time("Creation_Int32(N=10M)", () => new Int32Series("a", LargeN));
            Time("Creation_Float64(N=1M)",  () => new Float64Series("a", SmallN));
            Time("Creation_Float64(N=10M)", () => new Float64Series("a", LargeN));

            // ── Sort (ArgSort - Radix) ────────────────────
            Time("ArgSort_Int32(N=1M)",  () => SortKernels.ArgSort(s_i32Small.Memory.Span));
            Time("ArgSort_Int32(N=10M)", () => SortKernels.ArgSort(s_i32Large.Memory.Span));
            Time("ArgSort_Float64(N=1M)",  () => SortKernels.ArgSort(s_f64Small.Memory.Span));
            Time("ArgSort_Float64(N=10M)", () => SortKernels.ArgSort(s_f64Large.Memory.Span));
            // System sort comparison
            Time("ArgSortSys_Int32(N=1M)",  () => SortKernels.ArgSortSystem(s_i32Small.Memory.Span));
            Time("ArgSortSys_Int32(N=10M)", () => SortKernels.ArgSortSystem(s_i32Large.Memory.Span));
            Time("ArgSortSys_Float64(N=1M)",  () => SortKernels.ArgSortSystem(s_f64Small.Memory.Span));
            Time("ArgSortSys_Float64(N=10M)", () => SortKernels.ArgSortSystem(s_f64Large.Memory.Span));
            // Full sort + take (matching Python semantics)
            Time("SortFull_Int32(N=1M)", () => {
                var idx = SortKernels.ArgSort(s_i32Small.Memory.Span);
                var sorted = new int[SmallN];
                for (int i = 0; i < SmallN; i++) sorted[i] = s_i32Small.Memory.Span[idx[i]];
                GC.KeepAlive(sorted);
            });
            Time("SortFull_Float64(N=1M)", () => {
                var idx = SortKernels.ArgSort(s_f64Small.Memory.Span);
                var sorted = new double[SmallN];
                for (int i = 0; i < SmallN; i++) sorted[i] = s_f64Small.Memory.Span[idx[i]];
                GC.KeepAlive(sorted);
            });

            // ── Filter (SIMD) ─────────────────────────────
            Time("Filter_Int32(N=1M)", () => {
                var threshold = int.MaxValue / 2;
                var (indices, count) = FilterKernels.Filter(s_i32Small.Memory.Span, threshold, FilterOperation.GreaterThan);
                ArrayPool<int>.Shared.Return(indices);
            });
            Time("Filter_Int32(N=10M)", () => {
                var threshold = int.MaxValue / 2;
                var (indices, count) = FilterKernels.Filter(s_i32Large.Memory.Span, threshold, FilterOperation.GreaterThan);
                ArrayPool<int>.Shared.Return(indices);
            });
            Time("Filter_String_EQ(N=1M)", () => {
                var result = new int[SmallN];
                StringKernels.Equals(s_stringSmall.DataBytes.Span, s_stringSmall.Offsets.Span,
                    System.Text.Encoding.UTF8.GetBytes("banana"), result.AsSpan());
                GC.KeepAlive(result);
            });

            // ── Aggregations ──────────────────────────────
            Time("Sum_Agg(N=1M)",  () => AggregationKernels.Sum(s_i32Small));
            Time("Sum_Agg(N=10M)", () => AggregationKernels.Sum(s_i32Large));
            Time("Mean_Agg(N=1M)",  () => AggregationKernels.Mean(s_f64Small));
            Time("Mean_Agg(N=10M)", () => AggregationKernels.Mean(s_f64Large));
            Time("Std_Agg(N=1M)",  () => AggregationKernels.Std(s_f64Small));
            Time("Std_Agg(N=10M)", () => AggregationKernels.Std(s_f64Large));

            // ── GroupBy ────────────────────────────────────
            Time("GroupBy_Single_Int32Sum(N=1M)", () => {
                var agg = GroupByKernels.GroupBySumInt32(s_keySmall, s_i32Small);
                GC.KeepAlive(agg);
            });
            Time("GroupBy_Single_F64Mean(N=1M)", () => {
                var agg = GroupByKernels.GroupByMeanF64(s_keySmall, s_f64Small);
                GC.KeepAlive(agg);
            });
            Time("GroupBy_Single_Int32Sum(N=10M)", () => {
                var agg = GroupByKernels.GroupBySumInt32(s_keyLarge, s_i32Large);
                GC.KeepAlive(agg);
            });
            Time("GroupBy_MultiAgg_F64(N=1M)", () => {
                var groups = GroupByKernels.GroupByMultiAggF64Fast(s_keySmall, s_f64Small);
                GC.KeepAlive(groups);
            });
            // Hash-based fast paths
            Time("GroupBy_Hash_Int32Sum(N=1M)", () => {
                var agg = GroupByKernels.GroupBySumInt32Fast(s_keySmall, s_i32Small);
                GC.KeepAlive(agg);
            });
            Time("GroupBy_Hash_Int32Sum(N=10M)", () => {
                var agg = GroupByKernels.GroupBySumInt32Fast(s_keyLarge, s_i32Large);
                GC.KeepAlive(agg);
            });
            Time("GroupBy_Hash_F64Mean(N=1M)", () => {
                var agg = GroupByKernels.GroupByMeanF64Fast(s_keySmall, s_f64Small);
                GC.KeepAlive(agg);
            });

            // ── Join ──────────────────────────────────────
            Time("Join_Inner_SmallRight(N=1M)", () => {
                var right = new Int32Series("r", Enumerable.Range(0, 1000).ToArray());
                var result = JoinKernels.InnerJoin(s_keySmall, right);
                GC.KeepAlive(result);
            });
            Time("Join_Inner_SmallRight(N=10M)", () => {
                var right = new Int32Series("r", Enumerable.Range(0, 1000).ToArray());
                var result = JoinKernels.InnerJoin(s_keyLarge, right);
                GC.KeepAlive(result);
            });

            // ── Rolling ───────────────────────────────────
            Time("RollingMean(N=1M)",  () => WindowKernels.RollingMean(s_f64Small, 5));
            Time("RollingMean(N=10M)", () => WindowKernels.RollingMean(s_f64Large, 5));
            Time("RollingStd(N=1M)",   () => WindowKernels.RollingStd(s_f64Small, 5));
            Time("RollingSum(N=1M)",   () => WindowKernels.RollingSum(s_f64Small, 5));

            // ── Expanding ─────────────────────────────────
            Time("ExpandingSum(N=1M)", () => WindowKernels.ExpandingSum(s_f64Small));
            Time("ExpandingStd(N=1M)", () => WindowKernels.ExpandingStd(s_f64Small));

            // ── EWM ───────────────────────────────────────
            Time("EWMMean(N=1M)", () => WindowKernels.EWMMean(s_f64Small, 0.5));

            // ── Unique ────────────────────────────────────
            Time("Unique(N=1M)", () => {
                var result = UniqueKernels.Unique(s_i32Small);
                GC.KeepAlive(result);
            });

            // ── String ops ────────────────────────────────
            Time("String_ToUpper(N=1M)", () => {
                var result = StringKernels.ToUppercase(s_stringSmall);
                GC.KeepAlive(result);
            });
            Time("String_Contains(N=1M)", () => {
                var result = new int[SmallN];
                StringKernels.Contains(s_stringSmall.DataBytes.Span, s_stringSmall.Offsets.Span, "a", result.AsSpan());
                GC.KeepAlive(result);
            });
            Time("String_Regex(N=1M)", () => {
                var result = new int[SmallN];
                StringKernels.RegexMatch(s_stringSmall.DataBytes.Span, s_stringSmall.Offsets.Span, "a.*a", result.AsSpan());
                GC.KeepAlive(result);
            });

            // ── Pivot ─────────────────────────────────────
            // Pre-create pivot data once
            DataFrame? pivotDf = null;
            {
                var rng = new Random(42);
                var n = 100_000;
                var groups = new int[n];
                var cats = new string[n];
                var vals = new double[n];
                for (int i = 0; i < n; i++)
                {
                    groups[i] = rng.Next(1000);
                    cats[i] = rng.Next(3) switch { 0 => "cat_A", 1 => "cat_B", _ => "cat_C" };
                    vals[i] = rng.NextDouble();
                }
                pivotDf = new DataFrame(new ISeries[] {
                    new Int32Series("date", groups),
                    new Utf8StringSeries("cat", cats),
                    new Float64Series("val", vals)
                });
            }
            Time("Pivot(N=100k)", () => {
                var result = PivotKernels.Pivot(pivotDf!, new[] { "date" }, "cat", "val", "sum");
                GC.KeepAlive(result);
            });

            // ── FillNull ──────────────────────────────────
            // Pre-create fillNull data once
            {
                var data = new double[SmallN];
                var rng = new Random(42);
                for (int i = 0; i < SmallN; i++) data[i] = rng.NextDouble();
                var series = new Float64Series("a", data);
                for (int i = 0; i < SmallN; i += 10) series.ValidityMask.SetNull(i);
                Time("FillNull_Forward(N=1M)", () => {
                    var result = FillNullKernels.FillNull(series, FillStrategy.Forward);
                    GC.KeepAlive(result);
                });
            }
            {
                var data = new double[LargeN];
                var rng = new Random(42);
                for (int i = 0; i < LargeN; i++) data[i] = rng.NextDouble();
                var series = new Float64Series("a", data);
                for (int i = 0; i < LargeN; i += 10) series.ValidityMask.SetNull(i);
                Time("FillNull_Forward(N=10M)", () => {
                    var result = FillNullKernels.FillNull(series, FillStrategy.Forward);
                    GC.KeepAlive(result);
                });
            }

            // ── Left Join ─────────────────────────────────
            Time("Join_Left(N=1M)", () => {
                var right = new Int32Series("r", Enumerable.Range(0, 1100).ToArray());
                var result = JoinKernels.LeftJoin(s_keySmall, right);
                GC.KeepAlive(result);
            });

            // ── Expanding on larger data ─────────────────
            Time("ExpandingSum(N=10M)", () => WindowKernels.ExpandingSum(s_f64Large));
            Time("ExpandingStd(N=10M)", () => WindowKernels.ExpandingStd(s_f64Large));
            Time("EWMMean(N=10M)", () => WindowKernels.EWMMean(s_f64Large, 0.5));
        }
    }
}
