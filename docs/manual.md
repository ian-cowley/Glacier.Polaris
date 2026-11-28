# Glacier.Polaris User Manual

## Contents

1. [Getting Started & Data Types](#1-getting-started--data-types)
2. [Eager vs. Lazy Execution](#2-eager-vs-lazy-execution)
3. [Reading, Streaming & Writing Data](#3-reading-streaming--writing-data)
4. [Expressions, Aggregations & Window Functions](#4-expressions-aggregations--window-functions)
5. [Joins, Reshaping & Data Cleaning](#5-joins-reshaping--data-cleaning)
6. [Out-of-Core Columns & External Sorting](#6-out-of-core-columns--external-sorting)
7. [GPU Vector Math Acceleration](#7-gpu-vector-math-acceleration)
8. [Analytics Helpers](#8-analytics-helpers)
9. [Building & Testing](#9-building--testing)

### Topic Manuals

- [Expressions, Series Extensions & I/O Helpers](manual/expressions-and-namespaces.md) — `SeriesExtensions`, string/temporal/list/binary/struct/predicate/math kernels, `StringCache`, `SqlDialect`, and `GlacierStorageBridge`
- [Direct Compute Kernels & Unmanaged Memory](manual/compute-kernels.md) — low-level SIMD compute kernel catalog (`FilterKernels`, `SortKernels`, `GroupByKernels`, `JoinKernels`, `WindowKernels`, `UniqueKernels`) and unmanaged memory owners

---

## 1. Getting Started & Data Types

Install the NuGet package:

```bash
dotnet add package Glacier.Polaris
```

A `DataFrame` is an ordered collection of typed columns (`ISeries`), where each column stores values in contiguous unmanaged or pooled buffers rather than boxed `object[]` arrays.

Supported column series types:

| Category | Series Types |
| :--- | :--- |
| **Signed Integers** | `Int8Series`, `Int16Series`, `Int32Series`, `Int64Series` |
| **Unsigned Integers** | `UInt8Series`, `UInt16Series`, `UInt32Series`, `UInt64Series` |
| **Floating-Point & Decimal** | `Float32Series`, `Float64Series`, `DecimalSeries` |
| **Boolean & Null Semantics** | `BooleanSeries` (`ValidityMask` + three-valued `KleeneBool` logic) |
| **Text & Binary** | `Utf8StringSeries`, `BinarySeries` |
| **Temporal** | `DateSeries`, `DatetimeSeries`, `DurationSeries`, `TimeOfDaySeries` |
| **Nested & Dictionary** | `CategoricalSeries`, `EnumSeries`, `ListSeries`, `StructSeries`, `ArraySeries` |

Construct a `DataFrame` directly from nullable arrays or raw arrays and dispose it when finished so unmanaged buffers return immediately:

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;

using var df = new DataFrame(new ISeries[]
{
    Int32Series.FromValues("id", new int?[] { 1, 2, null, 4 }),
    Utf8StringSeries.FromStrings("region", new string?[] { "eu", "us", "eu", null }),
    new Float64Series("amount", new[] { 12.5, 40.0, 18.0, 99.5 })
});
```

Missing entries are stored in a separate bit-packed `ValidityMask`, so `null` stays distinct from `0`, `0.0`, `false`, and `""` across sorting, grouping, filtering, and deduplication.

---

## 2. Eager vs. Lazy Execution

- **Eager mode (`DataFrame`):** Calls run immediately against in-memory columns (`df.SortByEager`, `df.Unique`, `df.GroupBy`, `df.Slice`). Use eager mode for interactive inspection or short single-step transforms.
- **Lazy mode (`LazyFrame`):** Enters a query plan via `df.Lazy()` or `LazyFrame.ScanCsv` / `ScanParquet` / `ScanJson` / `ScanSql`. Calls build a logical tree that `QueryOptimizer` rewrites (pushing filters, required column subsets, and row limits toward the data source, and reordering small inner joins) before executing when you call `await query.Collect()`, `CollectStreaming()`, or a `Sink*` method.

```csharp
using Glacier.Polaris;

DataFrame summary = await df.Lazy()
    .Filter(e => Expr.Col("amount") >= 15.0)
    .GroupBy("region")
    .Agg(Expr.Col("amount").Sum().Alias("total_amount"))
    .Sort("total_amount", descending: true)
    .Collect();
```

---

## 3. Reading, Streaming & Writing Data

### File Readers & Scans

| Source / Format | Lazy Scan | Eager Read | Notes |
| :--- | :--- | :--- | :--- |
| **CSV** | `LazyFrame.ScanCsv(path)` | `CsvReader` | Handles quoted fields, multi-line fields, escaped `""`, and filter/column/limit pushdown |
| **Parquet** | `LazyFrame.ScanParquet(path)` | `ParquetReader` | Multi-threaded row-group reading with column pruning |
| **NDJSON / JSON Array** | `LazyFrame.ScanJson(path, columns)` | `DataFrame.ReadJson(path, columns)` | Accepts both newline-delimited JSON and top-level `[...]` arrays |
| **ADO.NET SQL** | `LazyFrame.ScanSql(connection, sql)` | `DataFrame.FromSqlReader(reader)` | Reads standard `IDbConnection` / `IDataReader` streams |
| **Compact Columnar IPC v1** | — | `DataFrame.FromArrowIpc(pathOrStream)` | Fast binary column framing via `Glacier.Formats.Arrow` (custom protocol, not Apache Arrow FlatBuffers wire-compatible) |

```csharp
using Glacier.Polaris;

// Read selected columns from NDJSON or a top-level JSON array
using DataFrame eventsDf = DataFrame.ReadJson("events.ndjson", columns: new[] { "id", "score" });

// Stream a lazy CSV scan in fixed-size 65,536-row batches
await foreach (DataFrame batch in LazyFrame.ScanCsv("large_log.csv")
    .Filter(e => Expr.Col("status") == 500)
    .Select(Expr.Col("timestamp"), Expr.Col("path"))
    .CollectStreaming(batchSize: 65_536))
{
    using (batch)
    {
        Console.WriteLine($"Batch rows: {batch.RowCount}");
    }
}
```

### Writers & Streaming Sinks

On an existing `DataFrame`:
- `df.WriteCsv(filePath, separator: ',', includeHeader: true, includeBom: false)`
- `df.WriteParquet(filePath)`
- `df.WriteJson(filePath, ndjson: true)`
- `df.WriteIpc(filePath)` / `df.WriteIpcStream(filePath)` / `df.ToArrowIpc(stream, leaveOpen: false)`

On a `LazyFrame`, stream directly to disk without materializing the full result in memory:
- `await query.SinkCsv("out.csv");`
- `await query.SinkParquet("out.parquet");`
- `await query.SinkIpc("out.glipc");`

---

## 4. Expressions, Aggregations & Window Functions

Build expressions with `Expr.Col("name")`, `Expr.Lit(value)`, arithmetic/comparison operators, and conditional logic (`Expr.When(cond).Then(val).Otherwise(fallback)`).

### Aggregations (`GroupBy(...).Agg(...)` or `Select(...)`)

- `Sum()`, `Mean()`, `Min()`, `Max()`, `Std()`, `Var()`, `Median()`, `Count()`, `NUnique()`, `Quantile(double q)`

### Rolling, Expanding & Dynamic Windows

- **Rolling:** `RollingMean(window)`, `RollingSum(window)`, `RollingMin(window)`, `RollingMax(window)`, `RollingStd(window)`
- **Expanding & EWM:** `ExpandingMean()`, `ExpandingSum()`, `ExpandingMin()`, `EWMMean(double alpha)`
- **Lag / Shift:** `Shift(int n)` (on `Expr` and `LazyFrame`)
- **Time/Index Grouping on `LazyFrame`:** `GroupByDynamic(indexColumn, every, period?, offset?, closed: "left", startBy: "window")` and `GroupByRolling(indexColumn, period, offset?, closed: "right")`

---

## 5. Joins, Reshaping & Data Cleaning

### Joins

`LazyFrame.Join` supports single-key, multi-key, and separate left/right key signatures:

- `Join(other, on: "id", type: JoinType.Inner)`
- `Join(other, on: new[] { "org", "id" }, type: JoinType.Left)`
- `Join(other, leftOn: new[] { "cust_id" }, rightOn: new[] { "id" }, type: JoinType.Outer)`

Available `JoinType` modes: `Inner`, `Left`, `Outer`, `Cross`, `AsOf`, `Semi`, `Anti`.

### Reshaping & Null Handling

- **Reshape:** `Pivot(index, pivot, values, aggregate: "sum")`, `Unpivot(on, index)`, `Transpose(include_header, header_name, column_names)`, `Explode(column)`, `Unnest(params string[] columns)`, `Rename(Dictionary<string, string>)`, `WithRowIndex(name: "index")`.
- **Sorting & Slicing:** `Sort(column, descending)` / `Sort(columnNames, descending)` (stable 8-bit LSD radix sort across `Int32` and `Float64`), `Limit(n)` / `Head(n)` / `Tail(n)` / `Fetch(n)` / `Slice(offset, length)`.
- **Deduplication & Missing Values:**
  - `Unique(subset: ..., maintainOrder: true)` / `Distinct(...)` — treats `null` as a distinct value rather than conflating it with `0` or `""`.
  - `DropNulls(subset: ..., anyNull: true)` and `FillNan(value)`.
  - `FillNull(strategy)` with `FillStrategy`: `Forward`, `Backward`, `Min`, `Max`, `Mean`, `Zero`, `One`.

---

## 6. Out-of-Core Columns & External Sorting

Back numeric columns directly by files on disk using `FromMmf` so SIMD filters and aggregations operate over OS virtual memory mappings:

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;

using var idCol = Int32Series.FromMmf("ids", "ids.col", 1_000_000);
using var valCol = Float64Series.FromMmf("vals", "vals.col", 1_000_000);

Span<int> ids = idCol.Memory.Span;
Span<double> vals = valCol.Memory.Span;
for (int i = 0; i < 1_000_000; i++)
{
    ids[i] = i;
    vals[i] = i * 1.5;
}

using var df = new DataFrame(new ISeries[] { idCol, valCol });
using DataFrame filtered = await df.Lazy()
    .Filter(e => Expr.Col("ids") > 500_000)
    .Select(Expr.Col("vals"))
    .Collect();
```

When a sort exceeds `ExternalMergeSort.DefaultSpillBudget`, Polaris spills sorted runs to temporary disk files and merges them with a K-way priority tournament.

---

## 7. GPU Vector Math Acceleration

`GpuPolarisAccelerator` accelerates `Float32Series` vector math and reductions using either multi-threaded AVX-512/AVX2 CPU kernels or NVIDIA CUDA driver kernels (`nvcuda.dll` / `libcuda.so.1`):

- Binary ops: `VectorAdd`, `VectorSub`, `VectorMul`, `VectorDiv`, `VectorFma` (`GpuTarget.Auto` dispatches to NVIDIA CUDA when `IsNvidiaAvailable` and `n >= 65_536`).
- Unary math: `VectorExp`, `VectorLog`, `VectorSqrt`, `VectorSigmoid` (`n >= 65_536` for `Auto`).
- Reduction: `VectorSum` (`n >= 131_072` for `Auto`).

```csharp
using Glacier.Polaris.Compute;

float[] a = new float[1_000_000];
float[] b = new float[1_000_000];
float[] dst = new float[1_000_000];

GpuPolarisAccelerator.VectorAdd(a, b, dst, GpuTarget.Cpu);
float total = GpuPolarisAccelerator.VectorSum(dst, GpuTarget.Cpu);
```

Because uncached host spans pay PCIe transfer time on each call, `GpuTarget.Cpu` is faster than `GpuTarget.Auto` at 1M floats (`0.41 ms` vs `1.09 ms` for `VectorAdd` on Ryzen AI 9 HX 370 vs RTX 4060). `GpuTarget.Amd` throws `PlatformNotSupportedException` and `GpuTarget.DualGpu` throws `NotSupportedException`. Call `GpuPolarisAccelerator.ReleaseGpuResources()` on shutdown if you want eager CUDA context cleanup.

---

## 8. Analytics Helpers

Polaris exposes single-pass columnar analytics helpers on series and compute kernels: `Histogram` (single-pass binning), `KDE` (kernel density estimation), Shannon `Entropy`, and hash `ValueCounts`.

---

## 9. Building & Testing

```bash
dotnet build Glacier.Polaris.slnx -c Release
dotnet test Glacier.Polaris.slnx -c Release --no-build
```
