#!/usr/bin/env python3
"""
Parity Test Golden Data Generator
----------------------------------
Generates canonical expected outputs from Python Polars for every feature.
C# tests compare against these golden files to ensure result parity.

Usage: python generate_parity_data.py
Output: golden/*.json files
"""

import polars as pl
import json
import os
import datetime

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")
os.makedirs(GOLDEN_DIR, exist_ok=True)

def save(name, data):
    """Save data as JSON golden file."""
    # Convert Polars objects to serializable format
    def serialize(obj):
        if isinstance(obj, pl.Series):
            return obj.to_list()
        if isinstance(obj, pl.DataFrame):
            return obj.to_dict(as_series=False)
        if isinstance(obj, dict):
            return {k: serialize(v) for k, v in obj.items()}
        if isinstance(obj, (datetime.datetime, datetime.timedelta, datetime.date)):
            return str(obj)
        if hasattr(obj, 'item'):  # numpy/polars scalar
            return obj.item()
        return obj

    serialized = serialize(data)
    path = os.path.join(GOLDEN_DIR, f"{name}.json")
    with open(path, "w") as f:
        json.dump(serialized, f, indent=2, default=str)
    print(f"  ✓ Saved {path}")
    return serialized

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ======================================================================
# TIER 1: Core Data & Arithmetic
# ======================================================================

section("TIER 1: Core Data & Arithmetic")

def gen_tier1_arithmetic():
    """Basic arithmetic operations"""
    df = pl.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": [10, 20, 30, 40, 50]
    })
    
    # Addition
    result = df.select((pl.col("a") + pl.col("b")).alias("add"))
    save("tier1_add", result)
    
    # Subtraction
    result = df.select((pl.col("b") - pl.col("a")).alias("sub"))
    save("tier1_sub", result)
    
    # Multiplication
    result = df.select((pl.col("a") * pl.col("b")).alias("mul"))
    save("tier1_mul", result)
    
    # Division
    result = df.select((pl.col("b") / pl.col("a")).alias("div"))
    save("tier1_div", result)
    
    # Float arithmetic
    df_float = pl.DataFrame({"x": [1.5, 2.5, 3.5], "y": [0.5, 1.0, 1.5]})
    result = df_float.select((pl.col("x") + pl.col("y")).alias("add"))
    save("tier1_float_add", result)
    
    # Mix int + float
    result = df.select((pl.col("a") + 1.5).alias("int_plus_float"))
    save("tier1_mix_types", result)
    
    # Literal operations
    result = df.select((pl.col("a") + 10).alias("plus_literal"))
    save("tier1_literal_add", result)
    
    result = df.select((pl.col("a") * 2).alias("times_literal"))
    save("tier1_literal_mul", result)

gen_tier1_arithmetic()


def gen_tier1_comparison():
    """Comparison operators"""
    df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
    
    # Equal
    result = df.select((pl.col("a") == 3).alias("eq"))
    save("tier1_cmp_eq", result)
    
    # Not equal
    result = df.select((pl.col("a") != 3).alias("ne"))
    save("tier1_cmp_ne", result)
    
    # Greater than
    result = df.select((pl.col("a") > 3).alias("gt"))
    save("tier1_cmp_gt", result)
    
    # Greater or equal
    result = df.select((pl.col("a") >= 3).alias("ge"))
    save("tier1_cmp_ge", result)
    
    # Less than
    result = df.select((pl.col("a") < 3).alias("lt"))
    save("tier1_cmp_lt", result)
    
    # Less or equal
    result = df.select((pl.col("a") <= 3).alias("le"))
    save("tier1_cmp_le", result)

gen_tier1_comparison()


def gen_tier1_extended_types():
    """Extended numeric types (Int8, Int16, Int64, UInt*, Float32)"""
    # Int8
    df = pl.DataFrame({"a": pl.Series("a", [1, 2, 3], dtype=pl.Int8)})
    result = df.select((pl.col("a") + 10).alias("plus"))
    save("tier1_int8", result)
    
    # Int16
    df = pl.DataFrame({"a": pl.Series("a", [100, 200, 300], dtype=pl.Int16)})
    result = df.select((pl.col("a") * 2).alias("times"))
    save("tier1_int16", result)
    
    # Int64
    df = pl.DataFrame({"a": pl.Series("a", [1000000, 2000000, 3000000], dtype=pl.Int64)})
    result = df.select((pl.col("a") + 500000).alias("plus"))
    save("tier1_int64", result)
    
    # UInt8
    df = pl.DataFrame({"a": pl.Series("a", [10, 20, 30], dtype=pl.UInt8)})
    result = df.select((pl.col("a") * 3).alias("times"))
    save("tier1_uint8", result)
    
    # UInt16
    df = pl.DataFrame({"a": pl.Series("a", [100, 200, 300], dtype=pl.UInt16)})
    result = df.select((pl.col("a") + 50).alias("plus"))
    save("tier1_uint16", result)
    
    # UInt32
    df = pl.DataFrame({"a": pl.Series("a", [1000, 2000, 3000], dtype=pl.UInt32)})
    result = df.select((pl.col("a") * 4).alias("times"))
    save("tier1_uint32", result)
    
    # UInt64
    df = pl.DataFrame({"a": pl.Series("a", [50000, 60000, 70000], dtype=pl.UInt64)})
    result = df.select((pl.col("a") + 10000).alias("plus"))
    save("tier1_uint64", result)
    
    # Float32
    df = pl.DataFrame({"a": pl.Series("a", [1.5, 2.5, 3.5], dtype=pl.Float32)})
    result = df.select((pl.col("a") * 2.0).alias("times"))
    save("tier1_float32", result)

gen_tier1_extended_types()


def gen_tier1_boolean_logic():
    """Boolean series operations"""
    df = pl.DataFrame({
        "x": [True, True, False, False],
        "y": [True, False, True, False]
    })
    
    result = df.select(
        (pl.col("x") & pl.col("y")).alias("and"),
        (pl.col("x") | pl.col("y")).alias("or"),
    )
    save("tier1_boolean", result)

gen_tier1_boolean_logic()


def gen_tier1_basic_agg():
    """Basic aggregations (sum, min, max, mean, std, var)"""
    df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
    
    # Note: These produce single-row results (scalar aggregations)
    agg_result = df.select(
        pl.col("a").sum().alias("sum"),
        pl.col("a").min().alias("min"),
        pl.col("a").max().alias("max"),
        pl.col("a").mean().alias("mean"),
        pl.col("a").std().alias("std"),
        pl.col("a").var().alias("var"),
        pl.col("a").count().alias("count"),
        pl.col("a").n_unique().alias("n_unique"),
    )
    save("tier1_basic_agg", agg_result)
    
    # Quantile
    q_result = df.select(
        pl.col("a").quantile(0.25).alias("q25"),
        pl.col("a").quantile(0.5).alias("q50"),
        pl.col("a").quantile(0.75).alias("q75"),
    )
    save("tier1_quantile", q_result)

gen_tier1_basic_agg()


def gen_tier1_null_propagation():
    """Null propagation through arithmetic operations"""
    df = pl.DataFrame({"a": pl.Series("a", [1, None, 3])})
    
    # Addition with null
    result = df.select((pl.col("a") + 10).alias("plus"))
    save("tier1_null_add", result)
    
    # Comparison with null
    result = df.select(
        (pl.col("a") > 2).alias("gt"),
    )
    save("tier1_null_cmp", result)
    
    # Negation
    result = df.select(
        (-pl.col("a")).alias("neg"),
    )
    save("tier1_null_neg", result)

gen_tier1_null_propagation()


# ======================================================================
# TIER 2: Data Manipulation
# ======================================================================

section("TIER 2: Data Manipulation")

def gen_tier2_select():
    """Select operations"""
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9]
    })
    
    # Single column
    result = df.select("a")
    save("tier2_select_single", result)
    
    # Multiple columns
    result = df.select("a", "b")
    save("tier2_select_multi", result)
    
    # Expression with alias
    result = df.select((pl.col("a") * 2).alias("a_double"))
    save("tier2_select_expr", result)

gen_tier2_select()


def gen_tier2_filter():
    """Filter operations"""
    df = pl.DataFrame({
        "a": [1, 2, 3, 4, 5],
        "b": ["x", "y", "z", "w", "v"]
    })
    
    # Simple predicate
    result = df.filter(pl.col("a") > 2)
    save("tier2_filter_gt", result)
    
    # Combined conditions (and)
    result = df.filter((pl.col("a") > 1) & (pl.col("a") < 5))
    save("tier2_filter_and", result)
    
    # Null filter
    df_nulls = pl.DataFrame({"a": pl.Series("a", [1, None, 3, None, 5])})
    result = df_nulls.filter(pl.col("a").is_not_null())
    save("tier2_filter_not_null", result)
    
    result = df_nulls.filter(pl.col("a").is_null())
    save("tier2_filter_is_null", result)
    
    # Filter on string
    result = df.filter(pl.col("b") == "z")
    save("tier2_filter_string", result)

gen_tier2_filter()


def gen_tier2_sort():
    """Sort operations"""
    df = pl.DataFrame({
        "cat": ["A", "A", "B", "B"],
        "val": [30, 10, 40, 20]
    })
    
    # Single column ascending
    result = df.sort("val")
    save("tier2_sort_asc", result)
    
    # Single column descending
    result = df.sort("val", descending=True)
    save("tier2_sort_desc", result)
    
    # Multi-column mixed direction
    result = df.sort(["cat", "val"], descending=[False, True])
    save("tier2_sort_multi", result)

gen_tier2_sort()


def gen_tier2_limit():
    """Limit operations"""
    df = pl.DataFrame({"a": [10, 20, 30, 40, 50]})
    
    result = df.limit(3)
    save("tier2_limit_3", result)
    
    result = df.limit(0)
    save("tier2_limit_0", result)
    
    # Limit more than available
    result = df.limit(10)
    save("tier2_limit_overflow", result)

gen_tier2_limit()


def gen_tier2_with_columns():
    """WithColumns operations"""
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [10, 20, 30]
    })
    
    # Add new column
    result = df.with_columns(
        (pl.col("a") * 2).alias("c")
    )
    save("tier2_withcols_add", result)
    
    # Overwrite existing column
    result = df.with_columns(
        (pl.col("a") + 100).alias("a")
    )
    save("tier2_withcols_overwrite", result)
    
    # Multiple new columns
    result = df.with_columns(
        (pl.col("a") * 10).alias("a10"),
        (pl.col("b") / 2).alias("b_half"),
    )
    save("tier2_withcols_multi", result)

gen_tier2_with_columns()


# ======================================================================
# TIER 3: Joins
# ======================================================================

section("TIER 3: Joins")

def gen_tier3_joins():
    """Join operations"""
    left = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })
    right = pl.DataFrame({
        "id": [2, 3, 4],
        "salary": [50000, 60000, 70000]
    })
    
    # Inner join
    result = left.join(right, on="id", how="inner")
    save("tier3_join_inner", result)
    
    # Left join
    result = left.join(right, on="id", how="left")
    save("tier3_join_left", result)
    
    # Outer join
    result = left.join(right, on="id", how="full")
    save("tier3_join_outer", result)
    
    # Cross join
    left_cross = pl.DataFrame({"id": [1, 2]})
    right_cross = pl.DataFrame({"val": ["a", "b"]})
    result = left_cross.join(right_cross, how="cross")
    save("tier3_join_cross", result)
    
    # Semi join
    result = left.join(right, on="id", how="semi")
    save("tier3_join_semi", result)
    
    # Anti join
    result = left.join(right, on="id", how="anti")
    save("tier3_join_anti", result)

gen_tier3_joins()


def gen_tier3_asof_join():
    """AsOf join"""
    left = pl.DataFrame({
        "time": [10, 20, 30],
        "val_l": [1, 2, 3]
    }).sort("time")
    right = pl.DataFrame({
        "time": [5, 15, 25],
        "val_r": [50, 150, 250]
    }).sort("time")
    
    result = left.join_asof(right, on="time")
    save("tier3_asof_join", result)

gen_tier3_asof_join()


# ======================================================================
# TIER 4: GroupBy & Aggregation
# ======================================================================

section("TIER 4: GroupBy & Aggregation")

def gen_tier4_groupby():
    """GroupBy operations"""
    df = pl.DataFrame({
        "a": [1, 1, 2, 2, 2],
        "b": [10, 20, 30, 40, 50]
    })
    
    # Single aggregation
    result = df.group_by("a").agg(pl.col("b").sum().alias("sum_b"))
    save("tier4_groupby_sum", result)
    
    # Multiple aggregations
    result = df.group_by("a").agg(
        pl.col("b").sum().alias("sum_b"),
        pl.col("b").mean().alias("mean_b"),
        pl.col("b").min().alias("min_b"),
        pl.col("b").max().alias("max_b"),
        pl.col("b").count().alias("count_b"),
        pl.col("b").n_unique().alias("nunique_b"),
    )
    save("tier4_groupby_multi", result)
    
    # Agg with arithmetic
    result = df.group_by("a").agg(
        (pl.col("b").sum() * 2.0).alias("sum_double"),
    )
    save("tier4_groupby_arithmetic", result)
    
    # Quantile
    result = df.group_by("a").agg(
        pl.col("b").quantile(0.5).alias("q50_b"),
        pl.col("b").std().alias("std_b"),
        pl.col("b").var().alias("var_b"),
    )
    save("tier4_groupby_stats", result)

gen_tier4_groupby()


# ======================================================================
# TIER 5: Reshaping
# ======================================================================

section("TIER 5: Reshaping")

def gen_tier5_reshaping():
    """Reshaping operations"""
    
    # --- Pivot ---
    df = pl.DataFrame({
        "date": [1, 1, 2, 2],
        "store": [101, 102, 101, 102],
        "cat": ["A", "A", "B", "B"],
        "val": [1, 2, 3, 4]
    })
    result = df.pivot(index=["date", "store"], on="cat", values="val", aggregate_function="sum")
    save("tier5_pivot_multi_idx", result)
    
    # Simple pivot
    df = pl.DataFrame({
        "date": [1, 1, 2],
        "cat": ["A", "B", "A"],
        "val": [10, 20, 5]
    })
    result = df.pivot(index="date", on="cat", values="val", aggregate_function="sum")
    save("tier5_pivot_simple", result)
    
    # Pivot with mean
    df = pl.DataFrame({
        "cat": ["A", "A", "B", "B"],
        "val": [10, 20, 30, 40],
        "group": ["x", "x", "x", "x"]
    })
    result = df.pivot(index="group", on="cat", values="val", aggregate_function="mean")
    save("tier5_pivot_mean", result)
    
    # --- Unpivot (formerly Melt) ---
    df = pl.DataFrame({
        "id": [1, 2],
        "A": [10, 20],
        "B": [30, 40]
    })
    result = df.unpivot(index=["id"], on=["A", "B"])
    save("tier5_melt", result)
    
    # --- Transpose ---
    df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = df.transpose()
    save("tier5_transpose", result)
    
    # Transpose with header
    result = df.transpose(include_header=True, header_name="field")
    save("tier5_transpose_header", result)
    
    # Transpose single row
    df = pl.DataFrame({"a": [7], "b": [8], "c": [9]})
    result = df.transpose()
    save("tier5_transpose_single_row", result)

gen_tier5_reshaping()


# ======================================================================
# TIER 6: String Operations
# ======================================================================

section("TIER 6: String Operations")

def gen_tier6_strings():
    """String operations"""
    df = pl.DataFrame({"s": ["hello", "world", "rust", "Python"]})
    
    # Lengths
    result = df.select(pl.col("s").str.len_bytes().alias("len"))
    save("tier6_str_lengths", result)
    
    # Contains
    result = df.select(pl.col("s").str.contains("o").alias("has_o"))
    save("tier6_str_contains", result)
    
    # StartsWith
    result = df.select(pl.col("s").str.starts_with("he").alias("starts_he"))
    save("tier6_str_startswith", result)
    
    # EndsWith
    result = df.select(pl.col("s").str.ends_with("d").alias("ends_d"))
    save("tier6_str_endswith", result)
    
    # ToUpper
    result = df.select(pl.col("s").str.to_uppercase().alias("upper"))
    save("tier6_str_toupper", result)
    
    # ToLower
    result = df.select(pl.col("s").str.to_lowercase().alias("lower"))
    save("tier6_str_tolower", result)

gen_tier6_strings()


# ======================================================================
# TIER 7: Binary Operations
# ======================================================================

section("TIER 7: Binary Operations")

def gen_tier7_binary():
    """Binary operations"""
    df = pl.DataFrame({
        "data": [b"hello world", b"foo bar", b"world!"]
    })
    
    # Lengths
    result = df.select(pl.col("data").bin.size().alias("len"))
    save("tier7_bin_lengths", result)
    
    # Contains
    result = df.select(pl.col("data").bin.contains(b"world").alias("has_world"))
    save("tier7_bin_contains", result)
    
    # StartsWith
    result = df.select(pl.col("data").bin.starts_with(b"foo").alias("starts_foo"))
    save("tier7_bin_startswith", result)
    
    # EndsWith
    result = df.select(pl.col("data").bin.ends_with(b"!").alias("ends_bang"))
    save("tier7_bin_endswith", result)

gen_tier7_binary()


# ======================================================================
# TIER 8: Temporal Operations
# ======================================================================

section("TIER 8: Temporal Operations")

def gen_tier8_temporal():
    """Temporal operations"""
    
    # Date operations
    df = pl.DataFrame({
        "date": [datetime.date(2026, 5, 2), datetime.date(2025, 12, 25), datetime.date(2024, 3, 15)]
    })
    result = df.select(
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.day().alias("day"),
    )
    save("tier8_date_extract", result)
    
    # Datetime operations
    df = pl.DataFrame({
        "dt": [
            datetime.datetime(2026, 5, 2, 10, 30, 45, 123456, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 12, 25, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ]
    })
    result = df.select(
        pl.col("dt").dt.year().alias("year"),
        pl.col("dt").dt.month().alias("month"),
        pl.col("dt").dt.day().alias("day"),
        pl.col("dt").dt.hour().alias("hour"),
        pl.col("dt").dt.minute().alias("minute"),
        pl.col("dt").dt.second().alias("second"),
    )
    save("tier8_datetime_extract", result)
    
    # Temporal arithmetic
    dt1 = datetime.datetime(2026, 5, 2, 10, 0, 0, tzinfo=datetime.timezone.utc)
    dt2 = datetime.datetime(2026, 5, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
    df = pl.DataFrame({"dt1": [dt1], "dt2": [dt2]})
    result = df.select(
        (pl.col("dt1") - pl.col("dt2")).alias("diff")
    ).with_columns(
        pl.col("diff").dt.total_days().alias("days"),
        pl.col("diff").dt.total_hours().alias("hours"),
        pl.col("diff").dt.total_seconds().alias("seconds"),
    )
    save("tier8_temporal_diff", result)

gen_tier8_temporal()


# ======================================================================
# TIER 9: List & Struct Operations
# ======================================================================

section("TIER 9: List & Struct Operations")

def gen_tier9_list_struct():
    """List and Struct operations"""
    
    # --- List operations ---
    df = pl.DataFrame({
        "l": [[1, 2, 3], [4, 5], [6]]
    })
    result = df.select(
        pl.col("l").list.len().alias("len"),
        pl.col("l").list.sum().alias("sum"),
        pl.col("l").list.mean().alias("mean"),
        pl.col("l").list.min().alias("min"),
        pl.col("l").list.max().alias("max"),
    )
    save("tier9_list_agg", result)
    
    # List get
    result = df.select(
        pl.col("l").list.get(0).alias("first"),
        pl.col("l").list.get(-1).alias("last"),
    )
    save("tier9_list_get", result)
    
    # List join
    df_str = pl.DataFrame({"l": [["a", "b", "c"], ["d", "e"]]})
    result = df_str.select(
        pl.col("l").list.join("-").alias("joined"),
    )
    save("tier9_list_join", result)
    
    # List contains
    df = pl.DataFrame({"l": [[1, 2, 3], [4, 1], [6]]})
    result = df.select(
        pl.col("l").list.contains(1).alias("has_1"),
        pl.col("l").list.contains(4).alias("has_4"),
    )
    save("tier9_list_contains", result)
    
    # List unique
    df = pl.DataFrame({"l": [[1, 1, 2], [3, 4, 3, 4], []]})
    result = df.select(
        pl.col("l").list.unique().alias("unique"),
    )
    save("tier9_list_unique", result)
    
    # --- Struct operations ---
    df = pl.DataFrame({
        "s": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
    })
    result = df.select(
        pl.col("s").struct.field("a").alias("a_field"),
        pl.col("s").struct.field("b").alias("b_field"),
    )
    save("tier9_struct_field", result)

gen_tier9_list_struct()


# ======================================================================
# TIER 10: Advanced Features (Null Handling, When/Then, Window)
# ======================================================================

section("TIER 10: Advanced Features")

def gen_tier10_advanced():
    """Advanced features"""
    
    # --- FillNull strategies ---
    s = pl.Series("a", [1, None, 3, None, None])
    df = pl.DataFrame({"a": s})
    
    result = df.select(pl.col("a").fill_null(strategy="forward").alias("forward"))
    save("tier10_fill_forward", result)
    
    result = df.select(pl.col("a").fill_null(strategy="backward").alias("backward"))
    save("tier10_fill_backward", result)
    
    result = df.select(pl.col("a").fill_null(strategy="min").alias("min"))
    save("tier10_fill_min", result)
    
    result = df.select(pl.col("a").fill_null(strategy="max").alias("max"))
    save("tier10_fill_max", result)
    
    result = df.select(pl.col("a").fill_null(strategy="mean").alias("mean"))
    save("tier10_fill_mean", result)
    
    result = df.select(pl.col("a").fill_null(strategy="zero").alias("zero"))
    save("tier10_fill_zero", result)
    
    result = df.select(pl.col("a").fill_null(strategy="one").alias("one"))
    save("tier10_fill_one", result)
    
    # Fill null with literal
    result = df.select(pl.col("a").fill_null(99).alias("filled"))
    save("tier10_fill_literal", result)
    
    # When/Then/Otherwise
    df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
    result = df.select(
        pl.when(pl.col("a") > 3)
        .then(pl.lit("big"))
        .otherwise(pl.lit("small"))
        .alias("size")
    )
    save("tier10_when_then", result)
    
    # Window function (Over)
    df = pl.DataFrame({
        "group": ["A", "A", "B", "B"],
        "val": [10, 20, 30, 40]
    })
    result = df.select(
        (pl.col("val") / pl.col("val").sum().over("group")).alias("fraction")
    )
    save("tier10_window", result)
    
    # Expanding operations
    df = pl.DataFrame({"val": [1.0, 2.0, 3.0, 4.0]})
    result = df.select(
        pl.col("val").cum_sum().alias("cumsum"),
        pl.col("val").cum_min().alias("cummin"),
        pl.col("val").cum_max().alias("cummax"),
    )
    save("tier10_expanding", result)
    
    # Rolling operations
    df = pl.DataFrame({"val": [1.0, 2.0, 3.0, 4.0, 5.0]})
    result = df.select(
        pl.col("val").rolling_mean(window_size=3).alias("rolling_mean"),
        pl.col("val").rolling_sum(window_size=3).alias("rolling_sum"),
        pl.col("val").rolling_min(window_size=3).alias("rolling_min"),
        pl.col("val").rolling_max(window_size=3).alias("rolling_max"),
    )
    save("tier10_rolling", result)
    
    # EWM (Exponentially Weighted Mean)
    df = pl.DataFrame({"val": [10.0, 20.0, 30.0]})
    result = df.select(
        pl.col("val").ewm_mean(alpha=0.5, adjust=False).alias("ewm"),
    )
    save("tier10_ewm", result)
    
    # Unique
    df = pl.DataFrame({"a": [1, 2, 2, 3, 3, 3]})
    result = df.unique()
    save("tier10_unique", result)
    
    # IsNull / IsNotNull
    df = pl.DataFrame({"a": pl.Series("a", [1, None, 3])})
    result = df.select(
        pl.col("a").is_null().alias("is_null"),
        pl.col("a").is_not_null().alias("is_not_null"),
    )
    save("tier10_isnull", result)
    
    # Cast to categorical
    df = pl.DataFrame({"s": ["apple", "banana", "apple", "cherry"]})
    result = df.select(
        pl.col("s").cast(pl.Categorical).alias("cat"),
    )
    save("tier10_cast_categorical", result)

gen_tier10_advanced()


# ======================================================================
# TIER 12: I/O & Interop (Arrow round-trip)
# ======================================================================

section("TIER 12: I/O & Interop")

def gen_tier12_arrow():
    """Arrow interop - verify PyArrow round-trip preserves all types"""
    import pyarrow as pa
    
    # Int32 round-trip
    arr = pa.array([1, 2, 3], type=pa.int32())
    roundtrip = pa.table({"a": arr})
    result = pl.from_arrow(roundtrip)
    save("tier12_arrow_int32", result)
    
    # Float64 round-trip
    arr = pa.array([1.1, 2.2, 3.3], type=pa.float64())
    roundtrip = pa.table({"f": arr})
    result = pl.from_arrow(roundtrip)
    save("tier12_arrow_float64", result)
    
    # String round-trip
    arr = pa.array(["hello", "world", "test"], type=pa.utf8())
    roundtrip = pa.table({"s": arr})
    result = pl.from_arrow(roundtrip)
    save("tier12_arrow_string", result)
    
    # Boolean round-trip
    arr = pa.array([True, False, True], type=pa.bool_())
    roundtrip = pa.table({"b": arr})
    result = pl.from_arrow(roundtrip)
    save("tier12_arrow_bool", result)
    
    # Nulls preserved
    arr = pa.array([1, None, 3], type=pa.int32())
    roundtrip = pa.table({"a": arr})
    result = pl.from_arrow(roundtrip)
    save("tier12_arrow_nulls", result)

gen_tier12_arrow()


# ======================================================================
# TIER 13: Advanced / Missing Parity Tests
# ======================================================================

section("TIER 13: Advanced / Missing Parity Tests")

def gen_tier13_advanced():
    """ArraySeries, Implode, ExpandingMean, ScanParquet"""

    # --- ArraySeries (Pl.Array / fixed-size list) ---
    # pl.Array(pl.Int32, 2) means each row is a fixed-size array of 2 ints
    df = pl.DataFrame({
        "name": ["a", "b", "c"],
        "vals": pl.Series([
            [1, 2],
            [3, 4],
            [5, 6],
        ], dtype=pl.Array(pl.Int32, 2)),
    })
    save("tier13_array", df)

    # --- Implode (group values into list) ---
    df = pl.DataFrame({
        "g": ["x", "x", "y", "y", "y"],
        "v": [1, 2, 3, 4, 5],
    })
    result = df.group_by("g").agg(pl.col("v").implode().alias("imploded"))
    save("tier13_implode", result)

    # --- ExpandingMean (cumulative mean) ---
    # Polars doesn't have cum_mean() directly; compute via cum_sum / cum_count
    df = pl.DataFrame({"val": [10.0, 20.0, 30.0, 40.0]})
    result = df.select(
        (pl.col("val").cum_sum() / pl.col("val").cum_count()).alias("cummean"),
    )
    save("tier13_expanding_mean", result)

    # --- ScanParquet (parquet round-trip) ---
    import pyarrow as pa
    import pyarrow.parquet as pq
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [1.5, 2.5, 3.5],
        "c": ["x", "y", "z"],
    })
    table = df.to_arrow()
    pq_path = os.path.join(GOLDEN_DIR, "tier13_parquet.parquet")
    pq.write_table(table, pq_path)
    # Read back to verify
    result = pl.read_parquet(pq_path)
    save("tier13_parquet", result)
    print(f"  ✓ Saved {pq_path}")

gen_tier13_advanced()


# ======================================================================
# TIER 14: SQL Scan & DataFrame Operations
# ======================================================================

section("TIER 14: SQL Scan & DataFrame Operations")

def gen_tier14_sql_scan():
    """SQL scan parity - reads from a table via SQL and verifies all columns/types."""
    # This simulates what LazyFrame.ScanSql does.
    # Python Polars SQLContext approach for generating golden data:
    try:
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE employees (
                id      INTEGER  NOT NULL,
                name    TEXT     NOT NULL,
                salary  REAL     NOT NULL,
                active  INTEGER  NOT NULL
            )
        """)
        conn.execute("INSERT INTO employees VALUES (1, 'Alice', 95000.0, 1)")
        conn.execute("INSERT INTO employees VALUES (2, 'Bob', 82000.0, 0)")
        conn.execute("INSERT INTO employees VALUES (3, 'Carol', 110000.0, 1)")
        conn.execute("INSERT INTO employees VALUES (4, 'Dave', 74000.0, 0)")
        conn.execute("INSERT INTO employees VALUES (5, 'Eve', 130000.0, 1)")

        # Use Polars SQLContext to simulate ScanSql behavior
        from polars import SQLContext
        ctx = SQLContext(register_globals=True, eager=True)
        ctx.register("employees", pl.read_database("SELECT * FROM employees", conn))
        result = ctx.execute("SELECT * FROM employees", eager=True)
        save("tier14_sql_scan", result)
        print("  (Generated from sqlite3 via Polars SQLContext)")
    except Exception as e:
        print(f"  ⚠ SQLContext generation failed: {e}")
        print("  Falling back to static data...")
        # Fallback: static data matching the employees table
        result = pl.DataFrame({
            "id": pl.Series("id", [1, 2, 3, 4, 5], dtype=pl.Int64),
            "name": pl.Series("name", ["Alice", "Bob", "Carol", "Dave", "Eve"], dtype=pl.Utf8),
            "salary": pl.Series("salary", [95000.0, 82000.0, 110000.0, 74000.0, 130000.0], dtype=pl.Float64),
            "active": pl.Series("active", [1, 0, 1, 0, 1], dtype=pl.Int64),
        })
        save("tier14_sql_scan", result)

gen_tier14_sql_scan()


print(f"\n{'='*60}")
print(f"  All golden files generated in {GOLDEN_DIR}")
print(f"{'='*60}")


