# Glacier.Polaris Manual

Glacier.Polaris is a high-performance, strongly-typed, memory-efficient C# .NET 10 DataFrame library. It is designed to process massive datasets at speed by leveraging column-oriented layouts, SIMD vectorization, lazy query plans, predicate/projection pushdown, and custom memory management including zero-copy Memory-Mapped column storage.

---

## 1. Core Architecture

Glacier.Polaris utilizes a columnar architecture where each column is stored contiguously in memory as a `Series<T>`.

### A. Column Types
- **Primitive Series**: `Int32Series`, `Int64Series`, `Float64Series`, `Float32Series`, etc.
- **String Series**: `Utf8StringSeries` for UTF-8 encoded text.
- **Specialized Series**: `DateTimeSeries`, `TimeOfDaySeries`, `EnumSeries`.

### B. Eager vs. Lazy Execution
- **Eager**: Compute operations immediately on the DataFrame (e.g. `df.Filter(...)`, `df.Sort(...)`).
- **Lazy**: Build a query graph with expressions and compile it to an optimized physical execution plan. Initiated via `df.Lazy()`, modified with relational operators, and executed with `.Collect()`.

---

## 2. Query Optimization (`QueryOptimizer`)

When running in Lazy mode, query plans pass through an optimization pipeline before execution:
1. **Predicate Pushdown**: Filters are pushed down as close to the storage layer as possible, minimizing the rows loaded and copied.
2. **Projection Pushdown**: Column selections are analyzed, ensuring only the referenced columns are materialized.
3. **Common Subexpression Elimination (CSE)**: Identical expressions computed multiple times within a query are computed once and reused.
4. **Join Reordering**: Reorders join trees to minimize intermediate results.

---

## 3. Memory & Out-of-Core Management

Glacier.Polaris provides multiple levels of memory management to avoid GC overhead and support out-of-core calculations:

### A. Managed Array Pool (`MemoryOwnerColumn<T>`)
- Columns rent underlying arrays from `ArrayPool<T>.Shared`.
- Minimizes Garbage Collector pressure and allocations during query execution.
- Resources are released back to the pool once the Series is disposed.

### B. Memory-Mapped Columns (`MmfMemoryOwnerColumn<T>`)
- Backs columns directly by files on disk using the OS memory manager via `MemoryMappedFile` and a custom `System.Buffers.MemoryManager<T>`.
- **Zero-Copy Performance**: Exposes raw file pointers as safe, standard `Memory<T>` and `Span<T>`. Allows all SIMD kernels, filters, and aggregations to run directly on the unmanaged file mapping with zero copy.
- **Out-of-Core Execution**: Enables querying datasets much larger than physical RAM without memory exhaustion.

#### Example: Memory-Mapped Column Ingestion & Querying

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;

// 1. Instantiate memory-mapped columns
using var idCol = Int32Series.FromMmf("ids", "ids.col", 1_000_000);
using var valCol = Float64Series.FromMmf("vals", "vals.col", 1_000_000);

// Populate data (zero-copy writes directly to disk)
var idSpan = idCol.Memory.Span;
var valSpan = valCol.Memory.Span;
for (int i = 0; i < 1_000_000; i++)
{
    idSpan[i] = i;
    valSpan[i] = i * 1.5;
}

// 2. Load columns into DataFrame
var df = new DataFrame(new ISeries[] { idCol, valCol });

// 3. Perform optimized lazy query (Predicate Pushdown + Column selection)
var result = await df.Lazy()
    .Filter(Expr.Col("ids") > Expr.Lit(500_000))
    .Select(Expr.Col("vals"))
    .Collect();

Console.WriteLine($"Result Row Count: {result.RowCount}");
```

---

## 4. Analytical Capabilities

Built-in compute kernels support advanced analytics directly on the columnar data structures:
- **Histogram**: Fast single-pass binning for value distributions.
- **KDE (Kernel Density Estimation)**: SIMD-accelerated continuous density approximation.
- **Entropy**: Calculate mathematical Shannon entropy over distributions.
- **ValueCounts**: Fast hash-aggregate calculations.

---

## 5. Apache Arrow & Parquet Integration

- **ToArrowArray()**: Converts Polaris columns directly into standard `Apache.Arrow` arrays for zero-copy sharing with Arrow-compatible tools.
- **Arrow IPC / Parquet**: Read and write dataframes to standard file formats with high throughput.
