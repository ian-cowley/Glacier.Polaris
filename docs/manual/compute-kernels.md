# Direct Compute Kernels & Unmanaged Memory

Most applications interact with Polaris through `LazyFrame` and `DataFrame`. When writing tight hot loops or custom storage adapters, you can call the lower-level static kernels and unmanaged memory owners under `Glacier.Polaris.Compute` and `Glacier.Polaris.Memory` directly.

## 1. Compute Kernel Catalog (`Glacier.Polaris.Compute`)

| Kernel Class | Primary Operations |
| :--- | :--- |
| **`AggregationKernels`** | `Min`, `Max`, `All`, `Any`, `Count`, `First`, `Sum`, `Mean`, `Std`, `Var`, `Median`, `Quantile` |
| **`AnalyticalKernels`** | `Histogram(ISeries, int bins)`, `Kde(ISeries, double bandwidth, int gridPoints = 100)` |
| **`ArithmeticKernels`** | Span-based `Add`, `Subtract`, `Multiply`, `Divide`, `FusedMultiplyAdd` routing across CPU SIMD and `GpuPolarisAccelerator` |
| **`ArrayKernels`** | `Shift`, `Diff`, `Abs`, `Clip`, `DropNulls`, `GatherEvery` |
| **`ComputeKernels`** | `MapElements`, `Sum` (`int` / `long`), `BranchlessMask`, `Take` |
| **`ConditionalKernels` / `ComparisonKernels`** | Elementwise conditional selection (`Select(condition, thenResult, otherwiseResult)`) and vectorized comparisons |
| **`FillNullKernels`** | `FillNull(ISeries, FillStrategy)`, `FillWithValue(ISeries, ISeries)` |
| **`FilterKernels` & `PredicatePushdownEngine`** | SIMD comparisons (`FilterOperation`), compressed-block bitmap filtering (`EvaluateCompressedBlock`, `EvaluateCompressedBlockToBitmap`, `EvaluateCompressedBlockDouble`), and zone-map pruning (`InspectZoneMap`, `BlockPruneResult`, `PushdownPruneResult`, `IPredicatePushdownEngine`) |
| **`GroupByKernels`** | Multi-key grouping (`GroupBy`, `GroupBySortBased`), rolling group generation (`GenerateRollingGroups`), specialized fast paths (`GroupBySumInt32`, `GroupBySumF64`, `GroupByMeanF64`, `GroupByMultiAggF64Fast`), and `Aggregate` |
| **`HashKernels`** | Vectorized 64-bit column hashing (`UInt64Series Hash(ISeries)`) |
| **`JoinKernels`** | Exact `InnerJoin`, `LeftJoin`, `OuterJoin`, `CrossJoin`, `SemiJoin`, `AntiJoin` returning `JoinResult` |
| **`MeltKernels` & `PivotKernels`** | Unpivoting (`Melt`) and pivoting (`Pivot`) on `DataFrame` |
| **`SortKernels`** | 8-bit LSD radix `Sort` and `ArgSort` (`Int32` and `Float64`), `ArgSortSystem`, `FastArgSortInt32`, `MultiColumnSort`, `TopK` |
| **`TransposeKernels`** | Matrix-style `DataFrame` transpose (`Transpose`) |
| **`UniqueKernels`** | Null-aware deduplication (`UniqueIndices`, `UniqueRowIndices`, `NUnique`, `ApproxNUnique`, `CreateSeriesForTake`, `CloneWithName`) |
| **`WindowKernels`** | `RollingMean`, `RollingSum`, `RollingStd`, `RollingMin`, `RollingMax`, `ExpandingSum`, `ExpandingMean`, `ExpandingMin`, `ExpandingMax`, `ExpandingStd`, `ExpandingCount`, `EWMMean`, `EWMStd`, `BroadcastOver` |

Related execution utilities in the core namespace include `ExecutionEngine` (physical plan execution), `ChunkScheduler` (adaptive multi-core work partitioning), and `CardinalityEstimator`.

## 2. Unmanaged Column Owners (`Glacier.Polaris.Memory`)

- **`NativeMemoryOwner<T>`:** Allocates 64-byte cache-line-aligned native memory wrapped as `IMemoryOwner<T>` / `MemoryManager<T>` so spans slice directly into unmanaged RAM with zero GC heap pinning.
- **`MemoryOwnerColumn<T>`:** Pooled buffer owner backed by `ArrayPool<T>.Shared`, returned to the pool on disposal.
- **`MmfMemoryOwnerColumn<T>`:** OS `MemoryMappedFile` owner backing `Series<T>.FromMmf(...)` for datasets larger than physical RAM.
- **`UnmanagedMemoryManager<T>` & `VectorStateBuffer`:** Pointer-backed `MemoryManager<T>` wrapper and state buffers used by compute kernels.
