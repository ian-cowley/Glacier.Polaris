#!/usr/bin/env python3
"""
Comprehensive Polars Python Benchmark Suite
Mirrors the C# Glacier.Polaris BenchmarkDotNet suite for side-by-side comparison.

Usage: python benchmarks/python_bench_full.py [--size 1M|10M] [--output results.json]

Output: Prints results table to stdout. Optionally saves JSON for analysis.
"""

import polars as pl
import time
import numpy as np
import sys
import os

# ── Configuration ──────────────────────────────────────────────────────────

SMALL_N = 1_000_000
LARGE_N = 10_000_000
SEED = 42

rng = np.random.default_rng(SEED)


def fmt_time(seconds: float) -> str:
    return f"{seconds*1000:.2f} ms"


def fmt_rows(rows: int, seconds: float) -> str:
    if seconds <= 0:
        return "N/A"
    return f"{rows / seconds:,.0f}"


# ── Benchmark Functions ───────────────────────────────────────────────────

def bench_creation_int32(n: int) -> float:
    """Creation: allocate and fill Int32 series"""
    start = time.perf_counter()
    s = pl.Series("a", rng.integers(0, 2**31 - 1, n, dtype=np.int32))
    end = time.perf_counter()
    _ = s  # prevent optimization
    return end - start


def bench_creation_float64(n: int) -> float:
    """Creation: allocate and fill Float64 series"""
    start = time.perf_counter()
    s = pl.Series("a", rng.random(n))
    end = time.perf_counter()
    _ = s
    return end - start


def bench_creation_string(n: int) -> float:
    """Creation: allocate string series"""
    categories = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"]
    data = rng.choice(categories, n)
    start = time.perf_counter()
    s = pl.Series("name", data)
    end = time.perf_counter()
    _ = s
    return end - start


def bench_sort_int32(n: int) -> float:
    """Sort Int32 series (full sort)"""
    df = pl.DataFrame({"a": rng.integers(0, 2**31 - 1, n, dtype=np.int32)})
    start = time.perf_counter()
    df.sort("a")
    end = time.perf_counter()
    return end - start


def bench_sort_float64(n: int) -> float:
    """Sort Float64 series"""
    df = pl.DataFrame({"a": rng.random(n)})
    start = time.perf_counter()
    df.sort("a")
    end = time.perf_counter()
    return end - start


def bench_filter_int32(n: int, threshold_percentile: float = 0.5) -> float:
    """Filter Int32 series with predicate"""
    df = pl.DataFrame({"a": rng.integers(0, 2**31 - 1, n, dtype=np.int32)})
    threshold = int(2**31 * threshold_percentile)
    start = time.perf_counter()
    df.filter(pl.col("a") > threshold)
    end = time.perf_counter()
    return end - start


def bench_filter_string(n: int) -> float:
    """Filter string series with predicate"""
    categories = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"]
    df = pl.DataFrame({"name": rng.choice(categories, n)})
    start = time.perf_counter()
    df.filter(pl.col("name") == "banana")
    end = time.perf_counter()
    return end - start


def bench_sum_agg(n: int) -> float:
    """Sum aggregation over Int32 series"""
    df = pl.DataFrame({"a": rng.integers(0, 2**31 - 1, n, dtype=np.int32)})
    start = time.perf_counter()
    result = df.select(pl.col("a").sum())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_mean_agg(n: int) -> float:
    """Mean aggregation"""
    df = pl.DataFrame({"a": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("a").mean())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_std_agg(n: int) -> float:
    """Std aggregation"""
    df = pl.DataFrame({"a": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("a").std())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_groupby_single(n: int, n_groups: int = 1000) -> float:
    """GroupBy single key, single aggregation"""
    df = pl.DataFrame({
        "key": rng.integers(0, n_groups, n, dtype=np.int32),
        "val": rng.random(n),
    })
    start = time.perf_counter()
    result = df.group_by("key").agg(pl.col("val").sum())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_groupby_multiagg(n: int, n_groups: int = 1000) -> float:
    """GroupBy single key, multiple aggregations"""
    df = pl.DataFrame({
        "key": rng.integers(0, n_groups, n, dtype=np.int32),
        "val": rng.random(n),
    })
    start = time.perf_counter()
    result = df.group_by("key").agg(
        pl.col("val").sum().alias("sum"),
        pl.col("val").mean().alias("mean"),
        pl.col("val").min().alias("min"),
        pl.col("val").max().alias("max"),
        pl.col("val").count().alias("count"),
    )
    end = time.perf_counter()
    _ = result
    return end - start


def bench_inner_join_small_right(n: int) -> float:
    """Inner join with small right table"""
    left = pl.DataFrame({
        "key": rng.integers(0, n // 1000, n, dtype=np.int32),
        "val": rng.random(n),
    })
    right = pl.DataFrame({
        "key": np.arange(n // 1000, dtype=np.int32),
        "meta": rng.random(n // 1000),
    })
    start = time.perf_counter()
    result = left.join(right, on="key", how="inner")
    end = time.perf_counter()
    _ = result
    return end - start


def bench_inner_join_medium(n: int) -> float:
    """Inner join with medium right table"""
    left = pl.DataFrame({
        "key": rng.integers(0, n // 10, n, dtype=np.int32),
        "val": rng.random(n),
    })
    right = pl.DataFrame({
        "key": np.arange(n // 10, dtype=np.int32),
        "meta": rng.random(n // 10),
    })
    start = time.perf_counter()
    result = left.join(right, on="key", how="inner")
    end = time.perf_counter()
    _ = result
    return end - start


def bench_left_join(n: int) -> float:
    """Left join"""
    left = pl.DataFrame({
        "key": rng.integers(0, n // 10, n, dtype=np.int32),
        "val": rng.random(n),
    })
    right = pl.DataFrame({
        "key": np.arange(n // 10 + 1000, dtype=np.int32),
        "meta": rng.random(n // 10 + 1000),
    })
    start = time.perf_counter()
    result = left.join(right, on="key", how="left")
    end = time.perf_counter()
    _ = result
    return end - start


def bench_rolling_mean(n: int, window: int = 5) -> float:
    """Rolling mean"""
    df = pl.DataFrame({"val": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("val").rolling_mean(window_size=window))
    end = time.perf_counter()
    _ = result
    return end - start


def bench_rolling_std(n: int, window: int = 5) -> float:
    """Rolling std"""
    df = pl.DataFrame({"val": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("val").rolling_std(window_size=window))
    end = time.perf_counter()
    _ = result
    return end - start


def bench_expanding_sum(n: int) -> float:
    """Expanding (cumulative) sum"""
    df = pl.DataFrame({"val": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("val").cum_sum())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_expanding_std(n: int) -> float:
    """Expanding (cumulative) std"""
    df = pl.DataFrame({"val": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("val").cum_std())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_ewm_mean(n: int, alpha: float = 0.5) -> float:
    """Exponentially weighted mean"""
    df = pl.DataFrame({"val": rng.random(n)})
    start = time.perf_counter()
    result = df.select(pl.col("val").ewm_mean(alpha=alpha, adjust=False))
    end = time.perf_counter()
    _ = result
    return end - start


def bench_unique(n: int) -> float:
    """Unique values"""
    df = pl.DataFrame({"a": rng.integers(0, n // 2, n, dtype=np.int32)})
    start = time.perf_counter()
    result = df.unique()
    end = time.perf_counter()
    _ = result
    return end - start


def bench_string_toupper(n: int) -> float:
    """String to upper case"""
    rng_local = np.random.default_rng(42)
    data = [f"QuickBrownFoxJumpedOverTheLazyDog_{i}" for i in range(n)]
    df = pl.DataFrame({"s": data})
    start = time.perf_counter()
    result = df.select(pl.col("s").str.to_uppercase())
    end = time.perf_counter()
    _ = result
    return end - start


def bench_string_contains(n: int) -> float:
    """String contains"""
    rng_local = np.random.default_rng(42)
    data = rng_local.choice(
        ["apple", "banana", "cherry", "application", "bandana", "cherry blossom"],
        n
    )
    df = pl.DataFrame({"s": data})
    start = time.perf_counter()
    result = df.select(pl.col("s").str.contains("a.*a"))
    end = time.perf_counter()
    _ = result
    return end - start


def bench_string_regex_match(n: int) -> float:
    """String regex match"""
    rng_local = np.random.default_rng(42)
    data = rng_local.choice(
        ["apple", "banana", "cherry", "application", "bandana", "alphabet"],
        n
    )
    df = pl.DataFrame({"s": data})
    start = time.perf_counter()
    result = df.select(pl.col("s").str.contains("a.*a"))
    end = time.perf_counter()
    _ = result
    return end - start


def bench_pivot(n: int) -> float:
    """Pivot operation"""
    n_groups = min(n // 100, 10000)
    categories = ["cat_A", "cat_B", "cat_C"]
    df = pl.DataFrame({
        "date": rng.integers(0, n_groups, n, dtype=np.int32),
        "cat": rng.choice(categories, n),
        "val": rng.random(n),
    })
    start = time.perf_counter()
    result = df.pivot(index="date", on="cat", values="val", aggregate_function="sum")
    end = time.perf_counter()
    _ = result
    return end - start


def bench_pivot_mean(n: int) -> float:
    """Pivot with mean aggregation"""
    categories = ["A", "B", "C"]
    df = pl.DataFrame({
        "group": rng.choice(["x", "y"], n),
        "cat": rng.choice(categories, n),
        "val": rng.random(n),
    })
    start = time.perf_counter()
    result = df.pivot(index="group", on="cat", values="val", aggregate_function="mean")
    end = time.perf_counter()
    _ = result
    return end - start


def bench_arrow_roundtrip(n: int) -> float:
    """Arrow round-trip (DataFrame -> RecordBatch -> DataFrame)"""
    import pyarrow as pa
    df = pl.DataFrame({
        "a": rng.integers(0, 2**31 - 1, n, dtype=np.int32),
        "b": rng.random(n),
        "c": rng.choice(["hello", "world", "python"], n),
    })
    start = time.perf_counter()
    table = df.to_arrow()
    result = pl.from_arrow(table)
    end = time.perf_counter()
    _ = result
    return end - start


def bench_fill_null_forward(n: int, null_fraction: float = 0.1) -> float:
    """Forward fill nulls — uses NaN->null conversion because Polars treats NaN != null"""
    data = rng.random(n)
    null_mask = rng.random(n) < null_fraction
    data[null_mask] = np.nan
    # Polars treats NaN as a valid float, not null. Convert NaN to actual nulls first.
    df = pl.DataFrame({"a": pl.Series("a", data).fill_nan(None)})
    start = time.perf_counter()
    result = df.select(pl.col("a").fill_null(strategy="forward"))
    end = time.perf_counter()
    _ = result
    return end - start


# ── Results ───────────────────────────────────────────────────────────────

BENCHMARKS = [
    ("Creation_Int32", bench_creation_int32, [SMALL_N, LARGE_N]),
    ("Creation_Float64", bench_creation_float64, [SMALL_N, LARGE_N]),
    ("Creation_String", bench_creation_string, [500_000, SMALL_N]),
    ("Sort_Int32", bench_sort_int32, [SMALL_N, LARGE_N]),
    ("Sort_Float64", bench_sort_float64, [SMALL_N, LARGE_N]),
    ("Filter_Int32", bench_filter_int32, [SMALL_N, LARGE_N]),
    ("Filter_String", bench_filter_string, [SMALL_N, LARGE_N]),
    ("Sum_Agg", bench_sum_agg, [SMALL_N, LARGE_N]),
    ("Mean_Agg", bench_mean_agg, [SMALL_N, LARGE_N]),
    ("Std_Agg", bench_std_agg, [SMALL_N, LARGE_N]),
    ("GroupBy_Single", bench_groupby_single, [SMALL_N, LARGE_N]),
    ("GroupBy_MultiAgg", bench_groupby_multiagg, [SMALL_N, LARGE_N]),
    ("Join_Inner_SmallRight", bench_inner_join_small_right, [SMALL_N, LARGE_N]),
    ("Join_Inner_Medium", bench_inner_join_medium, [SMALL_N, LARGE_N]),
    ("Join_Left", bench_left_join, [SMALL_N, LARGE_N]),
    ("RollingMean", bench_rolling_mean, [SMALL_N, LARGE_N]),
    ("RollingStd", bench_rolling_std, [SMALL_N, LARGE_N]),
    ("ExpandingSum", bench_expanding_sum, [SMALL_N, LARGE_N]),
    ("ExpandingStd", bench_expanding_std, [SMALL_N, LARGE_N]),
    ("EWMMean", bench_ewm_mean, [SMALL_N]),
    ("Unique", bench_unique, [SMALL_N, LARGE_N]),
    ("String_ToUpper", bench_string_toupper, [500_000]),
    ("String_Contains", bench_string_contains, [500_000]),
    ("String_Regex", bench_string_regex_match, [500_000]),
    ("Pivot", bench_pivot, [100_000, SMALL_N]),
    ("Pivot_Mean", bench_pivot_mean, [100_000, SMALL_N]),
    ("Arrow_Roundtrip", bench_arrow_roundtrip, [SMALL_N]),
    ("FillNull_Forward", bench_fill_null_forward, [SMALL_N, LARGE_N]),
]


def run_benchmarks():
    print(f"{'='*90}")
    print(f"  Python Polars Benchmark Suite")
    print(f"{'='*90}")
    print(f"  Seed: {SEED}")
    print(f"  Small N: {SMALL_N:,}")
    print(f"  Large N: {LARGE_N:,}")
    print(f"  Polars version: {pl.__version__}")
    import pyarrow as pa
    print(f"  PyArrow version: {pa.__version__}")
    print(f"{'='*90}")
    
    results = {}
    
    for name, func, sizes in BENCHMARKS:
        for n in sizes:
            bench_name = f"{name}(N={n:,})"
            
            # Warmup run (for JIT compilation)
            try:
                _ = func(n)
            except Exception as e:
                print(f"  ⚠ Warmup failed for {bench_name}: {e}")
            
            # Timed runs
            times = []
            n_runs = 3
            for _ in range(n_runs):
                try:
                    gc_before = __import__('gc').isenabled()
                    t = func(n)
                    times.append(t)
                except Exception as e:
                    print(f"  ✗ {bench_name}: FAILED - {e}")
                    break
            
            if times:
                avg_time = sum(times) / len(times)
                min_time = min(times)
                results[bench_name] = {
                    "avg_ms": avg_time * 1000,
                    "min_ms": min_time * 1000,
                    "throughput_rows_per_sec": fmt_rows(n, min_time),
                }
                print(f"  {bench_name:>50s}: {fmt_time(min_time)} avg={fmt_time(avg_time)}")
    
    return results


if __name__ == "__main__":
    print(f"\nPython {sys.version}")
    
    # Run all benchmarks
    r = run_benchmarks()
    
    print(f"\n{'='*90}")
    print(f"  Summary Table")
    print(f"{'='*90}")
    print(f"  {'Benchmark':<45s} {'Time (ms)':<15s} {'Throughput (rows/s)':<20s}")
    print(f"  {'-'*45} {'-'*15} {'-'*20}")
    
    for name, data in sorted(r.items()):
        print(f"  {name:<45s} {data['min_ms']:<15.2f} {data['throughput_rows_per_sec']:<20s}")
    
    # Optionally save to JSON
    if "--output" in sys.argv:
        import json
        idx = sys.argv.index("--output")
        out_path = sys.argv[idx + 1]
        with open(out_path, "w") as f:
            json.dump(r, f, indent=2)
        print(f"\nResults saved to {out_path}")
