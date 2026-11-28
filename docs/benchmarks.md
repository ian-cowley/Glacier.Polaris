# Glacier.Polaris Benchmarks

All measurements below were captured directly from `FullSuiteBenchmark.Run()` (warmup pass followed by 3 measured iterations per operation with explicit GC collection between cases). Zero extrapolated or external competitor estimates are included.

## Test Environment

- **CPU:** AMD Ryzen AI 9 HX 370 w/ Radeon 890M (12 physical cores / 24 logical threads, Zen 5, AVX-512)
- **GPU:** NVIDIA GeForce RTX 4060 Laptop GPU (`IsNvidiaAvailable = True`)
- **Memory & OS:** 31 GB RAM, Microsoft Windows NT `10.0.26300.0`
- **Runtime:** `.NET 10.0.15` (`.NET SDK 11.0.100`)
- **Command:**

```bash
dotnet run --project benchmarks/Glacier.Polaris.Benchmarks/Glacier.Polaris.Benchmarks.csproj -c Release
```

## Core Kernel Throughput (`Seed = 42`)

| Operation | N = 1,000,000 | N = 10,000,000 | Notes |
| :--- | ---: | ---: | :--- |
| `Creation_Int32` | 0.06 ms | 2.63 ms | Unmanaged aligned allocation |
| `Creation_Float64` | 0.09 ms | 5.23 ms | Unmanaged aligned allocation |
| `ArgSort_Int32` (8-bit LSD radix) | 6.91 ms | 59.28 ms | Permutation index output |
| `ArgSortSys_Int32` (`Array.Sort` baseline) | 60.52 ms | 606.29 ms | Comparison sort baseline |
| `ArgSort_Float64` (8-bit LSD radix) | 8.20 ms | 75.54 ms | Permutation index output |
| `ArgSortSys_Float64` (`Array.Sort` baseline) | 72.83 ms | 607.48 ms | Comparison sort baseline |
| `SortFull_Int32` / `SortFull_Float64` | 26.23 ms / 51.78 ms | — | `ArgSort` + gather permutation |
| `Arithmetic_Add_Int32` | 0.17 ms | 3.00 ms | SIMD vectorized add |
| `Arithmetic_Add_Float64` | 0.25 ms | 4.10 ms | SIMD vectorized add |
| `Filter_Int32` | 0.70 ms | 1.58 ms | SIMD comparison + bitmask compress |
| `Filter_String_EQ` | 0.60 ms | — | UTF-8 offset equality filter |
| `Sum_Agg` | 0.07 ms | 0.52 ms | Vectorized sum reduction |
| `Mean_Agg` | 0.08 ms | 1.11 ms | Vectorized mean reduction |
| `Std_Agg` | 0.13 ms | 1.91 ms | Two-pass variance/std |
| `GroupBy_Single_Int32Sum` | 0.94 ms | 2.02 ms | 1,000 keys |
| `GroupBy_Single_F64Mean` | 1.18 ms | — | 1,000 keys |
| `GroupBy_MultiAgg_F64` | 1.93 ms | — | Combined sum/mean/count pass |
| `GroupBy_Hash_Int32Sum` | 0.59 ms | 2.07 ms | Open-addressed hash fast path |
| `GroupBy_Hash_F64Mean` | 0.65 ms | — | Open-addressed hash fast path |
| `Join_Inner_SmallRight` | 0.78 ms | 5.17 ms | 1,000 unique right keys |
| `Join_Left` | 0.64 ms | — | 1,100 right keys (`N=1M`) |
| `RollingMean` (window = 5) | 0.78 ms | 9.45 ms | Sliding window mean |
| `RollingStd` / `RollingSum` (window = 5) | 1.10 ms / 0.56 ms | — | Sliding window |
| `ExpandingSum` | 0.79 ms | 9.24 ms | Prefix scan |
| `ExpandingStd` | 0.96 ms | 7.41 ms | Welford / prefix std |
| `EWMMean` ($\alpha = 0.5$) | 1.44 ms | 12.84 ms | Exponentially weighted moving mean |
| `Unique` (`Int32`) | 9.02 ms | — | Null-aware hash deduplication |
| `String_ToUpper` | 6.83 ms | — | UTF-8 string case conversion |
| `String_Contains` | 9.03 ms | — | Substring scan |
| `String_Regex` (`a.*a`) | 2.05 ms | — | Regex evaluation |
| `Pivot` (`N=100k`, 1,000 groups x 3 cats) | 26.18 ms | — | Sum aggregation pivot |
| `FillNull_Forward` | 0.56 ms | 4.46 ms | Bitmap forward fill (10% nulls) |

## File I/O Round-Trip (`N = 100,000` Rows, 3 Columns: `Int32`, `Float64`, `Utf8String`)

| Format | Write | Read / `Scan...Collect()` |
| :--- | ---: | ---: |
| CSV (`WriteCsv` / `LazyFrame.ScanCsv`) | 54.20 ms | 24.48 ms |
| Parquet (`WriteParquet` / `LazyFrame.ScanParquet`) | 34.95 ms | 7.31 ms |
| NDJSON (`WriteJson` / `DataFrame.ReadJson`) | 228.47 ms | 69.86 ms |
| Compact Columnar IPC v1 (`WriteIpc` / `FromArrowIpc`) | 5.97 ms | 0.74 ms |

## FP32 Vector Math (`N = 1,000,000` `float` = 4 MB, Host Arrays)

| Operation | `GpuTarget.Cpu` (AVX-512) | `GpuTarget.Auto` (CUDA RTX 4060 incl. PCIe H2D/D2H) |
| :--- | ---: | ---: |
| `GpuPolarisAccelerator.VectorAdd` | **0.41 ms** | 1.09 ms |
| `GpuPolarisAccelerator.VectorSum` | **0.06 ms** | 0.42 ms |

For host-resident 4 MB vectors, PCIe host-to-device copy overhead exceeds kernel compute time, so `GpuTarget.Cpu` is faster. Use `GpuTarget.Auto` / `GpuTarget.Nvidia` only when compute intensity amortizes transfer cost.
