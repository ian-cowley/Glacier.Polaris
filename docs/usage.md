# Glacier.Polaris Usage Guide

This guide covers extended I/O patterns, memory-mapped out-of-core columns, analytics kernels, GPU math offload, and local build/test commands for `Glacier.Polaris`.

## Column Types & Execution Modes

Every column in a `DataFrame` stores contiguous elements in a typed `Series<T>`:

- **Numeric & Boolean:** `Int8Series`–`Int64Series`, `UInt8Series`–`UInt64Series`, `Float32Series`, `Float64Series`, `DecimalSeries`, `BooleanSeries`
- **Text, Binary & Temporal:** `Utf8StringSeries`, `BinarySeries`, `DateSeries`, `DatetimeSeries`, `DurationSeries`, `TimeOfDaySeries`
- **Nested & Dictionary:** `CategoricalSeries`, `EnumSeries`, `ListSeries`, `StructSeries`, `ArraySeries`

Two execution modes are available:

- **Eager (`DataFrame`):** Executes operations (`Filter`, `SortByEager`, `GroupBy`, `Join`, `Unique`) immediately on materialized columns.
- **Lazy (`LazyFrame`):** Records operators in a plan tree via `df.Lazy()` or `LazyFrame.ScanCsv` / `ScanParquet` / `ScanJson`, optimizes the plan in `QueryOptimizer`, and materializes results on `await query.Collect()` or sinks (`SinkCsv`, `SinkParquet`, `SinkIpc`).

## Streaming NDJSON & JSON Arrays

`JsonReader` (`DataFrame.ReadJson` and `LazyFrame.ScanJson`) parses both newline-delimited JSON (`NDJSON`, with or without a trailing newline) and top-level JSON arrays (`[...]`) using `Utf8JsonReader`, promoting schemas cleanly across chunks (`int` -> `long` -> `double` -> `string`) and applying column projection before materialization.

```csharp
using Glacier.Polaris;

// Eager load with column projection
using DataFrame eventsDf = DataFrame.ReadJson("events.ndjson", columns: new[] { "id", "timestamp", "score" });

// Streaming lazy pipeline
using DataFrame topScores = await LazyFrame.ScanJson("events.ndjson")
    .Filter(e => Expr.Col("score") >= 0.9)
    .Select(Expr.Col("id"), Expr.Col("score"))
    .Collect();
```

## Memory-Mapped Out-of-Core Columns (`FromMmf`)

For datasets larger than RAM or to reuse columns persisted as raw typed files, back series with `MemoryMappedFile` mappings via `FromMmf`:

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
    .Filter(Expr.Col("ids") > Expr.Lit(500_000))
    .Select(Expr.Col("vals"))
    .Collect();

Console.WriteLine($"Matched rows: {filtered.RowCount}");
```

## Glacier Compact Columnar IPC v1 & Parquet

`DataFrame` integrates directly with `Glacier.Formats.Arrow.ArrowRecordBatch` and Parquet:

- `WriteCsv` / `LazyFrame.ScanCsv`
- `WriteParquet` / `LazyFrame.ScanParquet`
- `WriteIpc` / `WriteIpcStream` / `FromArrowIpc` / `ToGlacierRecordBatch` / `FromGlacierRecordBatch`

Glacier Compact Columnar IPC v1 uses a compact custom framing protocol (`8`-byte continuation prefix + `16`-byte message header + `36`-byte `ArrowColumnDescriptor`). It is not wire-compatible with Apache Arrow FlatBuffers IPC.

## Hardware-Accelerated FP32 Vector Math

`GpuPolarisAccelerator` accelerates `Float32Series` elementwise operations (`VectorAdd`, `VectorSub`, `VectorMul`, `VectorDiv`, `VectorFma`, `VectorExp`, `VectorLog`, `VectorSqrt`, `VectorSigmoid`) and `VectorSum` reductions. Null masks propagate across operations and null lanes are zeroed before reductions.

```csharp
using Glacier.Polaris.Compute;

var a = new float[1_000_000];
var b = new float[1_000_000];
var dst = new float[1_000_000];

// Routes to NVIDIA CUDA (when n >= 65536 and nvcuda.dll is present) or AVX-512 / AVX2 CPU SIMD
GpuPolarisAccelerator.VectorAdd(a, b, dst, GpuTarget.Auto);
float sum = GpuPolarisAccelerator.VectorSum(dst, GpuTarget.Cpu);
```

Because `GpuTarget.Auto` pays host-to-device PCIe copy latency per uncached call (`n >= 65536` for vector ops, `n >= 131072` for `VectorSum`), `GpuTarget.Cpu` is often faster on host-resident 1M-element arrays (0.41 ms vs 1.09 ms for `VectorAdd` on Ryzen AI 9 HX 370 vs RTX 4060). Unsupported targets (`GpuTarget.Amd` and `GpuTarget.DualGpu`) fail explicitly with `PlatformNotSupportedException` / `NotSupportedException`.

## Analytics Kernels

The compute layer also exposes single-pass columnar analytics helpers (`Histogram`, `KDE` kernel density estimation, Shannon `Entropy`, and hash `ValueCounts`) alongside rolling (`RollingMean`, `RollingStd`, `RollingSum`), expanding (`ExpandingSum`, `ExpandingStd`), and exponentially weighted (`EWMMean`) window operations.

## Build & Test

```bash
dotnet build Glacier.Polaris.slnx -c Release
dotnet test Glacier.Polaris.slnx -c Release --no-build
```
