# lab5_dask_aggregation.py
import time
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import numpy as np
import os

# Configuration (adjust based on your machine's memory and cores)
NUM_ROWS = 5_000_000          # Total number of rows in the DataFrame
NUM_PARTITIONS = os.cpu_count() or 4  # Dask DataFrame partitions (often ~ number of cores)
NUM_UNIQUE_IDS = 1_000           # Number of unique IDs to group by

def generate_sample_dataframe(num_rows):
    """Generates a sample Pandas DataFrame."""
    print(f"Generating sample Pandas DataFrame with {num_rows} rows...")
    start_time = time.perf_counter()
    ids = np.random.randint(0, NUM_UNIQUE_IDS, size=num_rows)
    values = np.random.rand(num_rows) * 100
    df = pd.DataFrame({'id': ids, 'value': values})
    end_time = time.perf_counter()
    print(f"Pandas DataFrame generation took {end_time - start_time:.2f} seconds.")
    # print(df.info(memory_usage='deep'))  # Optional: check memory usage
    return df

def run_sequential_aggregation(df):
    """Performs groupby aggregation sequentially using Pandas."""
    print("Running sequential aggregation with Pandas...")
    start_time = time.perf_counter()
    result = df.groupby('id')['value'].mean()
    end_time = time.perf_counter()
    print(f"Sequential (Pandas) execution time: {end_time - start_time:.4f} seconds")
    return result

def run_parallel_dask_aggregation(df, npartitions):
    """Performs groupby aggregation in parallel using Dask DataFrame."""
    print(f"Running parallel aggregation with Dask ({npartitions} partitions)...")
    result = None
    start_time = time.perf_counter()

    # 1) Convert Pandas -> Dask DataFrame
    ddf = dd.from_pandas(df, npartitions=npartitions)

    # ) Apply the same groupby-mean on Dask (lazy result)
    dask_result_lazy = ddf.groupby('id')['value'].mean()

    # 3) trigger computation and return a Pandas Series
    result = dask_result_lazy.compute()


    # Placeholder to keep starter runnable until TODO is completed
    if result is None:
        print("Dask aggregation not yet implemented.")
        result = pd.Series(dtype=float)

    end_time = time.perf_counter()
    print(f"Parallel (Dask) execution time: {end_time - start_time:.4f} seconds")
    return result


if __name__ == "__main__":
    # Set up a local Dask cluster (separate worker processes by default)
    print("Setting up Dask LocalCluster...")
    cluster = LocalCluster(n_workers=NUM_PARTITIONS, threads_per_worker=1)
    # To use threads instead (subject to the GIL for pure Python):
    # cluster = LocalCluster(n_workers=1, threads_per_worker=NUM_PARTITIONS)
    client = Client(cluster)
    print(f"Dask Dashboard link: {client.dashboard_link}")
    print("-" * 30)

    # Generate data
    pandas_df = generate_sample_dataframe(NUM_ROWS)
    print("-" * 30)

    # Sequential baseline
    sequential_result = run_sequential_aggregation(pandas_df.copy())
    print("-" * 30)

    # Parallel Dask version
    parallel_result = run_parallel_dask_aggregation(pandas_df.copy(), NUM_PARTITIONS)
    print("-" * 30)

    # Optional verification (enable after implementing Dask path)
    # if isinstance(sequential_result, pd.Series) and isinstance(parallel_result, pd.Series):
    #     try:
    #         pd.testing.assert_series_equal(
    #             sequential_result.sort_index(),
    #             parallel_result.sort_index(),
    #             check_dtype=False,
    #             atol=1e-5,
    #         )
    #         print("Verification: Sequential and parallel results match.")
    #     except AssertionError as e:
    #         print(f"Verification error: results do not match.\n{e}")

    print("-" * 30)
    print("Shutting down Dask client and cluster...")
    client.close()
    cluster.close()
    print("Lab 5 finished.")
