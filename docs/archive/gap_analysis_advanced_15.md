# Gap Analysis: Advanced / Niche Features (Section 15) — Updated

> **Date**: 2026-05-11 (revised)
> **Scope**: Verification of advanced features against **current** Glacier.Polaris source code
> **Status**: All previously "missing" advanced features are now confirmed implemented ✅

## Summary

A prior version of this document (based on a comparison against Glacier.Polaris_OLD) concluded that many
features were either "OLD only" or "truly missing". A re-audit of the current codebase shows **all of them
are now implemented**. The table below reflects the current state.

| Feature | Status | C# Location |
|---------|--------|-------------|
| Streaming execution | ✅ Implemented | `LazyFrame.Collect(streaming: true)` → `IAsyncEnumerable<DataFrame>` |
| Dynamic groupby | ✅ Implemented | `GroupByDynamicBuilder.cs` + `GroupByKernels.GenerateDynamicGroups` |
| Rolling groupby | ✅ Implemented | `GroupByRollingBuilder.cs` + `GroupByKernels.GenerateRollingGroups` |
| `map_groups()` | ✅ Implemented | `GroupByBuilder.MapGroups(Func<DataFrame, DataFrame>)` |
| `map_elements()` | ✅ Implemented | `Expr.MapElements()` → `ComputeKernels.MapElements` |
| `map()` / `apply()` | ✅ Implemented | `DataFrame.Map(Func<DataFrame, DataFrame>)` / `LazyFrame.Map(...)` |
| KDE / histogram | ✅ Implemented | `AnalyticalKernels.Kde()` + `AnalyticalKernels.Histogram()` |
| `approx_n_unique()` | ✅ Implemented | `Expr.ApproxNUnique()` → `UniqueKernels.ApproxNUnique` |
| `entropy()` | ✅ Implemented | `Expr.Entropy()` → `AggregationKernels.Entropy` |
| `value_counts()` | ✅ Implemented | `Expr.ValueCounts()` → `UniqueKernels.ValueCounts` |
| `shrink_to_fit()` | ✅ Implemented | `DataFrame.ShrinkToFit()` (no-op; columns already single-chunk) |
| `rechunk()` | ✅ Implemented | `LazyFrame.Rechunk()` (identity; already contiguous) |
| `clear()` | ✅ Implemented | `DataFrame.Clear()` → returns empty DataFrame with same schema |
| `is_first()` | ✅ Implemented | `Expr.IsFirst()` → `UniqueKernels.IsFirst` |
| `hash()` | ✅ Implemented | `Expr.Hash()` → `HashKernels.Hash` (UInt64 output) |
| `reinterpret()` | ✅ Implemented | `Expr.Reinterpret()` → `Compute.ArrayKernels.Reinterpret` (bit-cast), Tier14_Reinterpret parity test added |

---

## Result

**All 16 previously-listed "missing or OLD-only" features are now present and fully parity-tested in the current project.**

---

## Previous (Incorrect) Analysis — For Reference Only

The prior version of this file incorrectly stated:

- **"OLD code only"**: `group_by_dynamic`, `rolling`, `map/apply`, `shrink_to_fit`, `rechunk`, `clear`
- **"Truly missing"**: Streaming execution, `map_groups`, KDE/histogram, `reinterpret`

These conclusions were based on comparing against a stale snapshot. The current `Glacier.Polaris` project
has all of these fully implemented and tested.
