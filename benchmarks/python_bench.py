import polars as pl
import time
import numpy as np

def benchmark():
    n = 10_000_000
    print(f"Benchmarking with {n:,} rows...")

    # 1. Ingestion/Creation
    start = time.time()
    df = pl.DataFrame({
        "id": np.random.randint(0, 1000, n),
        "val": np.random.random(n),
        "name": np.random.choice(["apple", "banana", "cherry", "date"], n)
    })
    print(f"Creation: {time.time() - start:.4f}s")

    # 2. Sort
    start = time.time()
    df_sorted = df.sort("val")
    print(f"Sort (float): {time.time() - start:.4f}s")

    # 3. Filter + Regex
    start = time.time()
    df_regex = df.filter(pl.col("name").str.contains("a.*a"))
    print(f"Regex Match: {time.time() - start:.4f}s")

    # 4. GroupBy Agg
    start = time.time()
    df_agg = df.group_by("id").agg(pl.col("val").sum().alias("sum"), pl.col("val").count().alias("count"))
    print(f"GroupBy Agg: {time.time() - start:.4f}s")

    # 5. Rolling Mean
    start = time.time()
    df_rolling = df.select(pl.col("val").rolling_mean(window_size=5))
    print(f"Rolling Mean: {time.time() - start:.4f}s")

    # 6. Join
    df2 = pl.DataFrame({
        "id": np.arange(1000),
        "meta": np.random.random(1000)
    })
    start = time.time()
    df_join = df.join(df2, on="id")
    print(f"Join (10M x 1k): {time.time() - start:.4f}s")

if __name__ == "__main__":
    benchmark()
