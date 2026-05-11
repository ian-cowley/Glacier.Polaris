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
    # Convert row-oriented (default) to column-oriented
    cols = {}
    for row in rows:
        for col, val in row.items():
            if col not in cols:
                cols[col] = []
            cols[col].append(val)
    with open(path, "w") as f:
        json.dump(cols, f, indent=2)
    print(f"Generated {name}.json")

def write_golden_with_binary_cols(df, name, binary_col_prefixes):
    """Write DataFrame with binary columns to golden JSON, encoding binary as hex strings."""
    path = os.path.join(GOLDEN_DIR, f"{name}.json")
    rows = json.loads(df.write_json())
    cols = {}
    for row in rows:
        for col, val in row.items():
            if col not in cols:
                cols[col] = []
            # Binary values appear as objects in JSON, encode them
            if isinstance(val, dict) and ("Invalid" in val or val == {}):
                cols[col].append(None)
            elif isinstance(val, dict):
                # Binary data... we need to read from the dataframe instead
                cols[col].append(str(val))
            else:
                cols[col].append(val)
    # Override approach: read binary columns directly
    for col in df.columns:
        for prefix in binary_col_prefixes:
            if col.startswith(prefix):
                encoded = []
                for val in df[col]:
                    if val is None:
                        encoded.append(None)
                    elif isinstance(val, bytes):
                        encoded.append(base64.b64encode(val).decode("ascii"))
                    else:
                        encoded.append(str(val))
                cols[col] = encoded
    with open(path, "w") as f:
        json.dump(cols, f, indent=2)
    print(f"Generated {name}.json")

# === Math: Floor / Ceil / Round ===
df = pl.DataFrame({"a": [1.5, -2.7, 3.0, -0.5, 0.0, None]})
result = df.select([
    pl.col("a").floor().alias("floor"),
    pl.col("a").ceil().alias("ceil"),
    pl.col("a").round(0).alias("round_0"),
    pl.col("a").round(1).alias("round_1"),
])
write_golden(result, "math_floor_ceil_round")

# === CumCount ===
df = pl.DataFrame({"a": [1, None, 1, 2, None, 3]})
result = df.select([
    pl.col("a").cum_count().alias("cum_count"),
    pl.col("a").cum_count(reverse=True).alias("cum_count_rev"),
])
write_golden(result, "cum_count")

# === CumProd ===
df = pl.DataFrame({"a": [1.0, 2.0, 3.0, 4.0]})
result = df.select([
    pl.col("a").cum_prod().alias("cum_prod"),
    pl.col("a").cum_prod(reverse=True).alias("cum_prod_rev"),
])
write_golden(result, "cum_prod")

# === CumCount + CumProd with nulls (int) ===
df = pl.DataFrame({"a": pl.Series([1, None, 2, None, 3], dtype=pl.Int32)})
result = df.select([
    pl.col("a").cum_count().alias("cum_count"),
    pl.col("a").cum_prod().alias("cum_prod"),
])
write_golden(result, "cum_count_prod_nulls")

# === Binary: Encode (returns strings that serialize cleanly) ===
df = pl.DataFrame({
    "data": [b"hello", b"foo bar", b"world!"],
})
result = df.select([
    pl.col("data").bin.encode("hex").alias("encode_hex"),
    pl.col("data").bin.encode("base64").alias("encode_b64"),
])
write_golden(result, "tier7_bin_encode_decode")

print("All golden files generated successfully!")
