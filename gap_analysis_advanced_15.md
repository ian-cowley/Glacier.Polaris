# Gap Analysis: Advanced / Niche Features (Section 15 Deep-Dive)

> **Date**: 2026-05-11
> **Scope**: Verification of the 16 "Unimplemented" features listed in Section 15 of gap_analysis.md
> **Method**: Cross-compared OLD (Glacier.Polaris_OLD) vs NEW (Glacier.Polaris) source code

## Summary

Of the 16 features listed as "Unimplemented" in Section 15:

| Status | Count | Features |
|--------|-------|----------|
| ✅ Already in **NEW** code | 6 | `approx_n_unique`, `entropy`, `value_counts`, `map_elements`, `is_first`, `hash` |
| ✅ Exists in **OLD** code (NOT ported) | 6 | `group_by_dynamic`, `rolling`, `map/apply`, `shrink_to_fit`, `rechunk`, `clear` |
| ❌ Truly missing (neither) | 4 | Streaming execution, `map_groups`, KDE/histogram, `reinterpret` |

**Result**: The gap analysis needs correction. **12 of 16** "unimplemented" features are actually implemented somewhere. **6 features** from the OLD project need to be ported to the NEW project.

---

## Detailed Findings

### ✅ Already in NEW Project (gap_analysis.md is outdated)

These are already implemented in Glacier.Polaris but the gap_analysis.md lists them as missing:

| Feature | Location in NEW Code |
|---------|---------------------|
| **`approx_n_unique()`** | `Expr.ApproxNUnique()` → `UniqueKernels.ApproxNUnique()` |
| **`entropy()`** | `Expr.Entropy()` → `AggregationKernels.Entropy()` |
| **`value_counts()`** | `Expr.ValueCounts()` → `UniqueKernels.ValueCounts()` |
| **`map_elements()`** | `Expr.MapElements()` → `ComputeKernels.MapElements()` |
| **`is_first()`** | `Expr.IsFirst()` → `UniqueKernels.IsFirst()` |
| **`hash()`** | `Expr.Hash()` → `HashKernels.Hash()` |

**Fix**: Remove these from the "Unimplemented" list or move them to their correct sections.

---

### ✅ In OLD Code Only — Needs Porting to NEW

These features exist in `Glacier.Polaris_OLD` but were **not ported** to the new `Glacier.Polaris`:

| # | Feature | OLD Location | NEW Status |
|---|---------|-------------|------------|
| 1 | **`group_by_dynamic()`** | `LazyFrame.GroupByDynamic()` + `GroupByDynamicBuilder.cs` + `GroupByKernels.GenerateDynamicGroups()` | ❌ Missing |
| 2 | **`rolling()` (Rolling GroupBy)** | `LazyFrame.GroupByRolling()` + `GroupByRollingBuilder.cs` + `GroupByKernels.GenerateRollingGroups()` | ❌ Missing |
| 3 | **`map()` / `apply()`** | `DataFrame.Map()`/`Apply()`, `LazyFrame.Map()`/`Apply()` | ❌ Missing |
| 4 | **`shrink_to_fit()`** | `DataFrame.ShrinkToFit()`, `LazyFrame.ShrinkToFit()` | ❌ Missing |
| 5 | **`rechunk()`** | `LazyFrame.Rechunk()` | ❌ Missing |
| 6 | **`clear()`** | `LazyFrame.Clear()` | ❌ Missing |

**Key dependencies to port** (files that need to be brought over):
- `Common/Duration.cs` — Duration string parser (e.g., `"1h30m"` → nanoseconds)
- `GroupByDynamicBuilder.cs` — Eager group_by_dynamic builder
- `GroupByRollingBuilder.cs` — Eager rolling group builder
- In `Compute/GroupByKernels.cs`:
  - `GenerateDynamicGroups()` — Window-based group generation
  - `GenerateRollingGroups()` — Sliding window group generation
- In `LazyFrame.cs`: `GroupByDynamic()`, `GroupByRolling()`, `Clear()`, `ShrinkToFit()`, `Rechunk()`
- In `DataFrame.cs`: `Map()`, `Apply()`, `ShrinkToFit()`
- In `QueryOptimizer.cs` (ExecutionEngine):
  - `AggOp` handler for `GroupByDynamicOp` and `GroupByRollingOp`
  - `ClearOp`, `ShrinkToFitOp` apply methods

---

### ❌ Truly Missing (Neither Project)

These are genuine gaps that don't exist in either project:

| Feature | Notes |
|---------|-------|
| **Streaming execution** (`lf.collect(streaming=True)`) | Requires fundamental architecture changes (chunked streaming execution) |
| **`map_groups()`** | No equivalent in either project |
| **KDE / histogram** | No analytical kernels exist in either project |
| **`reinterpret()`** | No ReinterpretKernels in either project |

---

## Porting Plan (Priority Order)

### High Priority — Minimal Effort, High Impact

These can be ported quickly:

1. **`Duration.cs`** — Small utility class (60 lines). No dependencies.
2. **`ShrinkToFit()` / `Rechunk()` / `Clear()`** — Each is ~10 lines; `Clear()` already has placeholder `ClearOp` in NEW execution engine
3. **`Map()` / `Apply()`** — Simple wrapper methods

### Medium Priority — Moderate Effort

4. **`GroupByDynamic` / `GroupByRolling`** — Requires porting:
   - `Duration.cs` (prerequisite)
   - `GenerateDynamicGroups()` + `GenerateRollingGroups()` from OLD `GroupByKernels.cs`
   - `GroupByDynamicBuilder.cs` + `GroupByRollingBuilder.cs`
   - `LazyFrame.GroupByDynamicOp` / `GroupByRollingOp` + execution engine handlers
   - Estimated: ~500 lines total

### Low Priority — Not Urgent

5. Streaming execution, `map_groups()`, KDE/histogram, `reinterpret()` — Architecture-level work

---

## Updated Section 15 (Corrected)

```
## 15. Advanced / Niche Features

| Feature | Python API | C# Status | Section |
|---------|-----------|-----------|---------|
| Streaming execution | `lf.collect(streaming=True)` | ❌ Missing | - |
| Dynamic groupby | `group_by_dynamic()` | 🟡 OLD only | GroupByDynamicBuilder.cs |
| Rolling groupby | `rolling()` | 🟡 OLD only | GroupByRollingBuilder.cs |
| Map groups | `map_groups()` | ❌ Missing | - |
| Map elements | `map_elements()` | ✅ In Expr.MapElements() | ComputeKernels |
| Map / apply | `map()` / `apply()` | 🟡 OLD only | DataFrame/MapApply |
| KDE / histogram | `df.plot.kde()`, `df.hist()` | ❌ Missing | - |
| `approx_n_unique()` | `col.approx_n_unique()` | ✅ In Expr.ApproxNUnique() | UniqueKernels |
| `entropy()` | `col.entropy()` | ✅ In Expr.Entropy() | AggregationKernels |
| `value_counts()` | `col.value_counts()` | ✅ In Expr.ValueCounts() | UniqueKernels |
| `shrink_to_fit()` | In-memory optimization | 🟡 OLD only | DataFrame/ShrinkToFit |
| `rechunk()` | Contiguous memory | 🟡 OLD only | LazyFrame.Rechunk |
| `clear()` | Returns empty DataFrame | 🟡 OLD only | LazyFrame.Clear |
| `is_first()` | First-occurrence check | ✅ In Expr.IsFirst() | UniqueKernels |
| `hash()` | Row hashing | ✅ In Expr.Hash() | HashKernels |
| `reinterpret()` | Bit reinterpretation | ❌ Missing | - |
```

**Legend Updated**: ✅ = Implemented, 🟡 = OLD only (needs porting), ❌ = Truly missing
