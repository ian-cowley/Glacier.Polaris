![Glacier.Polaris Banner](assets/banner.jpg)

# Glacier.Polaris

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![.NET 10](https://img.shields.io/badge/.NET-10.0-purple.svg)](https://dotnet.microsoft.com/)
[![Native AOT](https://img.shields.io/badge/Native%20AOT-Ready-brightgreen.svg)](https://learn.microsoft.com/dotnet/core/deploying/native-aot/)
[![NuGet Version](https://img.shields.io/nuget/v/Glacier.Polaris.svg)](https://www.nuget.org/packages/Glacier.Polaris/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Glacier.Polaris.svg)](https://www.nuget.org/packages/Glacier.Polaris/)

`Glacier.Polaris` is a pure C#, Native AOT-compatible (`IsAotCompatible=true`) columnar DataFrame and lazy relational query engine for .NET 10. It stores typed columns in 64-byte aligned unmanaged buffers (`NativeMemoryOwner<T>`), tracks missing values with Arrow-style little-endian bit-packed `ValidityMask` bitmaps and three-valued `KleeneBool` logic, and executes queries via `Vector512` / `Vector256` CPU SIMD kernels with optional NVIDIA CUDA offload (`GpuPolarisAccelerator`).

*(Framing caveat: Glacier Compact Columnar IPC v1 uses `Glacier.Formats.Arrow` compact binary framing and is not wire-compatible with Apache Arrow FlatBuffers IPC. GPU offload supports NVIDIA CUDA via `nvcuda.dll` with automatic CPU SIMD fallback; `GpuTarget.Amd` throws `PlatformNotSupportedException` and `GpuTarget.DualGpu` throws `NotSupportedException`.)*

Read the background write-up: [I built a native C# DataFrame engine to rival Python Polars](https://dev.to/iancowley/i-built-a-native-c-dataframe-engine-to-rival-python-polars-its-actually-faster-on-some-things-opg).

## Install

```bash
dotnet add package Glacier.Polaris
```

## Quick Start

### Lazy Query With Filter & Projection Pushdown

```csharp
using Glacier.Polaris;

DataFrame result = await LazyFrame.ScanCsv("employees.csv")
    .Filter(e => Expr.Col("Salary") > 50_000)
    .WithColumns(
        (Expr.Col("Salary") * 1.1).Alias("SalaryWithBonus"),
        Expr.When(Expr.Col("Salary") > 80_000)
            .Then("Senior")
            .Otherwise("Junior")
            .Alias("Level")
    )
    .Select(Expr.Col("Name"), Expr.Col("SalaryWithBonus"), Expr.Col("Level"))
    .Collect();
```

### Eager Sort, Null-Aware Deduplication & GroupBy

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;

using var df = new DataFrame(new ISeries[]
{
    Int32Series.FromValues("StoreId", new int?[] { 2, 1, 2, 1, 1 }),
    Utf8StringSeries.FromStrings("Category", new string?[] { "A", "B", "A", "B", null }),
    new Int32Series("Sales", new[] { 100, 250, 100, 400, 50 })
});

DataFrame unique = df.Unique(subset: new[] { "StoreId", "Category" }, maintainOrder: true);
DataFrame sorted = df.SortByEager(new[] { "StoreId", "Sales" }, new[] { false, true });
DataFrame grouped = df.GroupBy("StoreId").Agg(("Sales", "sum"), ("Sales", "count"));
```

## Key Capabilities

- **Native AOT & Warning-Free Execution:** Evaluates `Expression<Func<Expr, Expr>>` lambdas through `ExprLambdaEvaluator` (no `LambdaExpression.Compile()`, `Activator.CreateInstance`, or reflection-based JSON serialization).
- **Columnar Types & Null Semantics:** Supports `Int8`–`Int64`, `UInt8`–`UInt64`, `Float32`, `Float64`, `Decimal`, `Boolean`, `Utf8String`, `Binary`, `Date`, `Datetime`, `Duration`, `Time`, `Categorical`, `Enum`, `List`, `Struct`, and `Array`, with `ValidityMask` tail-bit masking (`0 <= NullCount <= Length`) and three-valued `KleeneBool` algebra.
- **Query Optimizer & Out-of-Core Operations:** Applies `Filter`, `Select` (projection), and `Limit` pushdown plus cardinality-aware inner-join reordering, alongside memory-mapped columns (`FromMmf`), K-way external merge sort, and stable multi-column LSD radix sorting.
- **Streaming I/O:** Reads and writes CSV, Parquet, NDJSON / JSON arrays (`Utf8JsonReader`), and Compact Columnar IPC v1, with streaming `SinkCsv`, `SinkParquet`, and `SinkIpc` targets.

## Measured Benchmarks

Measured in Release on **AMD Ryzen AI 9 HX 370 (12C/24T Zen 5, AVX-512), 31 GB RAM, Windows 11, .NET 10.0.15 (.NET SDK 11.0.100)**:

| Workload | Scope | Mean Latency | Notes |
| :--- | :--- | ---: | :--- |
| `ArgSort_Int32` (8-bit LSD radix) | 1M / 10M rows | **6.91 ms** / **59.28 ms** | vs `ArgSortSys_Int32` (`Array.Sort`): 60.52 ms / 606.29 ms |
| `Filter_Int32` (AVX-512 bitmask) | 1M / 10M rows | **0.70 ms** / **1.58 ms** | — |
| `GroupBy_Hash_Int32Sum` (1,000 keys) | 1M / 10M rows | **0.59 ms** / **2.07 ms** | — |
| `Join_Inner_SmallRight` (1,000 right keys) | 1M / 10M rows | **0.78 ms** / **5.17 ms** | Left join (`N=1M`): **0.64 ms** |
| `RollingMean` (window = 5) | 1M / 10M rows | **0.78 ms** / **9.45 ms** | — |
| Compact IPC v1 Write / Read | 100k x 3 cols | **5.97 ms** / **0.74 ms** | Parquet write/collect: **34.95 ms** / **7.31 ms** |
| `GpuPolarisAccelerator.VectorAdd` | 1M `float` | **0.41 ms** (CPU) / **1.09 ms** (CUDA `Auto`) | Uncached 4 MB PCIe transfer makes CPU SIMD faster at 1M |

Reproduce locally:

```bash
dotnet run --project benchmarks/Glacier.Polaris.Benchmarks/Glacier.Polaris.Benchmarks.csproj -c Release
```

## Documentation

- [Usage Guide](docs/usage.md) — streaming JSON, memory-mapped columns, analytics, joins, and build/test commands
- [Architecture](docs/architecture.md) — memory ownership, `ValidityMask` invariants, Native AOT design, and `QueryOptimizer` rules
- [Benchmarks](docs/benchmarks.md) — full 62-operation benchmark output and I/O + GPU measurements
- [Technical Audit Report](docs/REPORT.md) — ecosystem hardening & correctness report
- [Changelog](CHANGELOG.md) — release notes

## License & Credits

Licensed under the [MIT License](LICENSE).

Developed by Ian Cowley, Antigravity (Google DeepMind), Claude and Cline/DeepSeek.
