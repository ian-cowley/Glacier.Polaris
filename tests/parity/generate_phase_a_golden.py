import polars as pl
import json
import os
import base64

GOLDEN_DIR = "tests/parity/golden"
os.makedirs(GOLDEN_DIR, exist_ok=True)

def write_golden(df, name):
    """Write Polars DataFrame to golden JSON file in columnar format."""
    path = os.path.join(GOLDEN_DIR, f"{name}.json")
    rows = json.loads(df.write_json())
    cols = {}
    for row in rows:
        for col, val in row.items():
            if col not in cols:
                cols[col] = []
            cols[col].append(val)
    with open(path, "w") as f:
        json.dump(cols, f, indent=2)
    print(f"Generated {name}.json")

# === 1: Decimal Series ===
# Polars doesn't have Decimal as a native type in older versions, but has pl.Decimal
df = pl.DataFrame({
    "val": pl.Series([1.5, 2.75, 3.125, None], dtype=pl.Decimal(38, 9)),
})
result = df.select(
    pl.col("val").alias("decimal_vals"),
)
write_golden(result, "tier14_decimal")

# === 2: Enum Series ===
# Polars Enum requires a set of categories
categories = ["cat", "dog", "bird"]
df = pl.DataFrame({
    "pet": pl.Series(["cat", "dog", "cat", "bird", None], dtype=pl.Enum(categories)),
})
result = df.select(
    pl.col("pet").alias("enum_pet"),
)
write_golden(result, "tier14_enum")

# === 3: Object Series ===
# Polars Object type is available via pl.Object
df = pl.DataFrame({
    "obj": pl.Series([1, "hello", 3.14, None], dtype=pl.Object),
})
result = df.select(
    pl.col("obj").alias("object_vals"),
)
write_golden(result, "tier14_object")

# === 4: Null Series ===
df = pl.DataFrame({
    "null_col": pl.Series([None, None, None], dtype=pl.Null),
})
write_golden(df, "tier14_null")

# === 5: Binary Decode ===
# Start with hex-encoded strings and decode them
df = pl.DataFrame({
    "encoded": ["68656c6c6f", "666f6f20626172", "776f726c6421"],
})
result = df.select(
    pl.col("encoded").str.decode("hex").alias("decoded_hex"),
    pl.col("encoded").str.decode("base64").alias("decoded_b64"),
)
write_golden(result, "tier7_bin_decode")

# === 6: dt.truncate() ===
df = pl.DataFrame({
    "dt": pl.date_range(
        start=pl.datetime(2026, 1, 1, 10, 30, 0),
        end=pl.datetime(2026, 1, 1, 13, 0, 0),
        interval="1h",
        eager=True,
    ),
})
result = df.select(
    pl.col("dt").alias("original"),
    pl.col("dt").dt.truncate("1d").alias("trunc_day"),
    pl.col("dt").dt.truncate("2h").alias("trunc_2h"),
    pl.col("dt").dt.truncate("4h").alias("trunc_4h"),
)
write_golden(result, "tier8_truncate")

print("All Phase A golden files generated successfully!")
