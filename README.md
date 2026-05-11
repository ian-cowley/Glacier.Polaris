# Glacier.Polaris

Glacier.Polaris is a high-performance, strongly-typed DataFrame library for C# .NET 10, heavily inspired by the Rust-based Polars library. It provides a robust, zero-copy, memory-efficient data processing engine using modern C# features such as `Memory<T>`, `Span<T>`, hardware intrinsics (SIMD), and a lazy execution engine.

## What is it?

Glacier.Polaris provides an expressive API for data manipulation, cleaning, and analysis in .NET. It's built to handle large datasets efficiently.

### Key Features

*   **Strongly Typed DataFrames & Series**: Uses generics to avoid boxing/unboxing overhead, mapping directly to C# primitive types (e.g., `Int32Series`, `Float64Series`).
*   **Zero-Copy Memory Model**: Leverages `Memory<T>` and `Span<T>` for in-memory operations, meaning data is shared, sliced, and passed around without unnecessary cloning.
*   **SIMD Vectorized Operations**: `ComputeKernels` process data in chunks using CPU vector instructions, drastically speeding up aggregations, filtering, and mathematical operations.
*   **Lazy Execution Engine**: Computations are built into an Abstract Syntax Tree (AST) using `LazyFrame`. They are only executed when needed (e.g., via `CollectAsync()`), allowing for comprehensive query optimization.
*   **Query Optimization**: Features like Predicate Pushdown push filters closer to the data source (like reading a CSV), minimizing memory usage and processing time.
*   **Native Nullability (Kleene Logic)**: Uses three-state Boolean logic (`True`, `False`, `NA`) via structures like `ValidityMask` and `KleeneBool` to handle missing data natively without requiring nullable value types (`int?`), keeping memory contiguous.

## How it works

The core of the library is divided into a few key areas:

1.  **Memory Management**: Custom allocators and structures like `MemoryOwnerColumn` and `ValidityMask` manage dense arrays of data. This allows for C-like memory control while remaining safe within the .NET ecosystem.
2.  **Compute Kernels**: Highly optimized static methods (e.g., `ComputeKernels.Sum`, `ComputeKernels.BranchlessMask`) perform vectorized operations on `Span<T>`.
3.  **Lazy Evaluation**: Using the `Expr` class, users build up an expression tree. The `LazyFrame` holds this execution plan. When data is requested, the `QueryOptimizer` rewrites the AST (e.g., pushing filters down), and the `ExecutionEngine` runs the optimized plan, often returning results in asynchronous batches.

## Use Cases

1.  **High-Performance Data Ingestion and Transformation (ETL)**: Quickly loading large CSVs or Parquet files, applying complex filters, aggregating, and writing them back out.
2.  **Financial Time Series Analysis**: Utilizing the `WindowKernels` and temporal functions for rolling averages and statistical calculations over time-based data.
3.  **Data Science & Machine Learning Preprocessing**: Cleaning missing data using Kleene logic, normalizing features using SIMD vectorized math, and shaping data before feeding it into ML models like ML.NET securely and quickly.

## Examples

### 1. Basic Compute Kernel (Vectorized Math)

```csharp
using Glacier.Polaris.Compute;
using System;

// Create a large span of integers
int[] data = new int[1000];
Array.Fill(data, 5);

// Highly optimized sum using SIMD (if available on the target architecture)
long sum = ComputeKernels.Sum(data); // = 5000
```

### 2. Handling Missing Data (Kleene Logic)

```csharp
using Glacier.Polaris.Compute;

// Three-state logic prevents exceptions and allows null-aware boolean algebra
KleeneBool condition1 = KleeneBool.True;
KleeneBool condition2 = KleeneBool.NA;

// True OR NA = True
KleeneBool result = condition1 | condition2; 
bool isTrue = result.IsTrue; // true
```

### 3. Lazy Evaluation and Predicate Pushdown

This example demonstrates how filters are pushed down before the select operation, minimizing the data processed.

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Compute;

// 1. Point to a file (data isn't loaded yet)
var df = LazyFrame.ScanCsv("large_dataset.csv");

// 2. Build the query plan
var query = df
    .Select(e => Expr.Col("Age") * 2) 
    .Filter(e => Expr.Col("Age") > 18);

// Behind the scenes, the QueryOptimizer rewrites this to:
// Select(Filter(ScanCsv("large_dataset.csv"), Age > 18), Age * 2)

// 3. Execute the optimized plan
// (assuming an ExecuteAsync method to materialize the dataframe)
var engine = new ExecutionEngine();
await foreach (var batch in engine.ExecuteAsync(query.Plan))
{
     // Process batches of data efficiently
}
```

### 4. Working with Strongly Typed Series and Memory

```csharp
using Glacier.Polaris.Data;
using System;

// Create a series explicitly typing it. Avoids boxing overhead.
using var series = new Int32Series("UserIds", 100);

// Access raw memory via Span for C-like performance
Span<int> rawMemory = series.Memory.Span;
rawMemory[0] = 1001;

// Efficient validty mask setting (marking index 1 as null)
series.ValidityMask.SetNull(1);
bool isFirstNull = series.ValidityMask.IsValid(0); // true
bool isSecondNull = series.ValidityMask.IsValid(1); // false

// Because instances implement IDisposable, using statements ensure memory is returned
```

### 5. Advanced Feature: Joining and Grouping

`Glacier.Polaris` provides rich features for common relational operations, such as multi-column joining and grouping, providing comprehensive aggregation similar to SQL or Pandas.

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;

// Let's create two dataframes simulating a relational operation
var df1 = new DataFrame(new ISeries[] {
    new Int32Series("Id", new[] { 1, 2, 3 }),
    new Int32Series("Value", new[] { 10, 20, 30 })
});

var df2 = new DataFrame(new ISeries[] {
    new Int32Series("Id", new[] { 1, 2, 4 }),
    new Int32Series("Multiplier", new[] { 2, 3, 4 })
});

// Perform an inner join on the "Id" column
var joined = df1.Join(df2, "Id", JoinType.Inner);
// Resulting rows for (Id, Value, Multiplier): (1, 10, 2), (2, 20, 3)

// Multi-column groupby and aggregation
var salesDf = new DataFrame(new ISeries[] {
    new Int32Series("StoreId", new[] { 1, 1, 2, 2 }),
    new Int32Series("Quarter", new[] { 1, 1, 1, 2 }),
    new Int32Series("Sales", new[] { 100, 200, 300, 400 })
});

// Group by StoreId and Quarter, calculate sum and count of Sales
var grouped = salesDf.GroupBy("StoreId", "Quarter").Agg(
    ("Sales", "sum"),
    ("Sales", "count")
);
```

### 6. Expressions and Conditional Functions

Construct complex new columns natively using lazy evaluation via expressive `.WithColumns()` manipulations and conditionals like `Expr.When().Then().Otherwise()`.

```csharp
using Glacier.Polaris;

var df = LazyFrame.ScanCsv("employees.csv");

// Add computed columns, incorporating conditional logic
var transformed = await df
    .WithColumns(
        (Expr.Col("Salary") * 1.1).Alias("SalaryWithBonus"),
        Expr.When(Expr.Col("Salary") > 80000)
            .Then("Senior")
            .Otherwise("Junior")
            .Alias("Level")
    )
    .Collect();
```

### 7. Time-Series and Temporal Operations

Polars excels at time-series analysis. The library provides specialized structures like `DateSeries`, `DatetimeSeries`, and `DurationSeries`, backed by advanced join capabilities such as `JoinAsof` (As-of Joins).

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;
using System;

var dates = new DateTime[] {
    new DateTime(2021, 1, 1, 12, 30, 45, DateTimeKind.Utc),
    new DateTime(2022, 5, 15, 8, 15, 0, DateTimeKind.Utc)
};

var df = new DataFrame(new ISeries[] {
    new DateSeries("TradeDate", dates)
});

// Extract temporal features using the .Dt() namespace
var temporalFeatures = await df.Lazy()
    .Select(
        Expr.Col("TradeDate").Dt().Year().Alias("Year"),
        Expr.Col("TradeDate").Dt().Month().Alias("Month"),
        Expr.Col("TradeDate").Dt().Day().Alias("Day")
    )
    .Collect();

// 'As-Of' Joins are essential for merging financial tick data where timestamps don't perfectly align.
// It joins the closest timestamp backward (or forward/nearest).
// var result = trades.JoinAsof(quotes, on: "time", strategy: AsofStrategy.Backward);
```

### 8. Reshaping Data: Pivot and Melt

Transform data layouts seamlessly using `Pivot` (wide format) and `Melt` (long format).

```csharp
using Glacier.Polaris;
using Glacier.Polaris.Data;

var salesDf = new DataFrame(new ISeries[] {
    new Utf8StringSeries("Region", new[] { "North", "South", "North", "South" }),
    new Utf8StringSeries("Product", new[] { "A", "A", "B", "B" }),
    new Int32Series("Sales", new[] { 100, 200, 150, 250 })
});

// Pivot: Create columns for each 'Product' and aggregate 'Sales'
var pivoted = salesDf.Pivot(index: "Region", pivot: "Product", values: "Sales", agg: "sum");

// Melt: Convert wide pivoted data back to long format
var melted = pivoted.Melt(idVars: new[] { "Region" }, valueVars: new[] { "A", "B" }, variableName: "Product", valueName: "Sales");
