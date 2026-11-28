# Expressions, Series Extensions & I/O Helpers

This topic manual documents the specialized series extensions, domain kernels, shared categorical string cache, and SQL/IPC bridge helpers in `Glacier.Polaris`. For core `DataFrame` / `LazyFrame` workflows, see the [Main User Manual](../manual.md).

## 1. Series Extension Methods (`SeriesExtensions`)

When working directly with individual `ISeries` columns outside a `LazyFrame`, `Glacier.Polaris.SeriesExtensions` provides direct reduction extensions:

- `series.Min()`, `series.Max()`, `series.Sum()`, `series.Mean()`, `series.Std()`, `series.Var()`, `series.Median()`
- `series.Count()`, `series.NUnique()`, `series.Unique()`, `series.Quantile(double quantile)`
- `series.All()` (boolean / Kleene logical conjunction)

## 2. Specialized Domain Kernels

### Text & Pattern Kernels (`StringKernels`)
- **Matching & Search (`ReadOnlySpan<byte>` / offsets):** `StringKernels.Equals`, `Contains`, `StartsWith`, `EndsWith`, `RegexMatch`
- **Parsing & Structured Extraction (`Utf8StringSeries`):** `Split(source, separator, maxSplits)`, `ParseDate(source, format)`, `ParseDatetime(source, format)`, `Extract(source, pattern)`, `ExtractAll(source, pattern)`, `JsonDecode(source)` (returns `StructSeries`)
- **Transforms:** `ToUppercase`, `ToLowercase`, `ToTitlecase`, `Lengths`, `Take`, `TakeWithNulls`

### Temporal Kernels (`TemporalKernels`)
Extract calendar and clock components directly from temporal columns (`DateSeries`, `DatetimeSeries`):
- `ExtractYear`, `ExtractMonth`, `ExtractDay`, `ExtractHour`, `ExtractMinute`, `ExtractSecond`

### List, Binary, Struct & Predicate Kernels
- **`ListKernels`:** `Sum`, `Mean`, `Min` over `ListSeries` (or raw offset + value spans).
- **`BinaryKernels`:** `StartsWith`, `EndsWith`, `Contains`, `Lengths`, `Encode(BinarySeries, string encoding)`, `Decode(Utf8StringSeries, string encoding)`.
- **Struct manipulation:** `RenameFields(StructSeries, string[] newNames)`, `ProjectFields(StructSeries, IEnumerable<string> fieldNames)`, `WithFields(StructSeries, ISeries[] newFields)`, `JsonEncode(StructSeries)`.
- **`PredicateKernels`:** `IsIn(ISeries, IEnumerable)` / `IsIn(ISeries, ISeries)`, `IsBetween(ISeries, lower, upper, closed: "both")`, `IsNan`, `IsNotNan`.
- **`MathKernels`:** `Sqrt`, `Log`, `Log10`, `Exp`, `Sin`, `Cos`.

## 3. Shared Categorical String Cache (`StringCache`)

Enabling the global string cache lets multiple `CategoricalSeries` instances built independently share integer symbol codes across joins and concatenations:

```csharp
using Glacier.Polaris;

StringCache.Enable();
uint codeA = StringCache.GetOrCreate("eu-west");
string? value = StringCache.GetString(codeA);
string[] reverseMap = StringCache.GetRevMap();
StringCache.Disable();
```

## 4. Specialized Series Types & Registry

- **`NullSeries`, `TimeSeries`, `ObjectSeries`:** Specialized `ISeries` implementations for all-null placeholders, nanosecond/millisecond timestamps, and arbitrary object fallbacks alongside standard numeric and string series.
- **`SchemaRegistry`:** Thread-safe metadata registry mapping CLR column types cleanly across query planning and serialization.

## 5. SQL Dialect & Glacier Storage IPC Bridge

### SQL Source Helper (`Glacier.Polaris.IO.SqlDialect`)
- `SqlDialect.Create(dialect, connectionString)` opens an ADO.NET `DbConnection`.
- `ApplyProjection(dialect, query, columns)` and `ApplyLimit(dialect, query, limit)` rewrite SQL read queries cleanly for `LazyFrame.ScanSql`.

### Compact Columnar IPC Bridge (`Glacier.Polaris.IO.GlacierStorageBridge`)
Convert directly between `DataFrame` and `Glacier.Formats.Arrow.ArrowRecordBatch`:

- `df.ToGlacierRecordBatch()` / `DataFrame.FromGlacierRecordBatch(batch)`
- `df.ToArrowIpc(Stream destinationStream, bool leaveOpen = false)` / `df.WriteGlacierArrowIpc(...)`
- `DataFrame.FromArrowIpc(Stream sourceStream, bool leaveOpen = false)`
