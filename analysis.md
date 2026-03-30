# Lab 5 Analysis


## Recorded timings

- Dask partitions used: 7
- Sequential (Pandas) time: 2.90 s
- Parallel (Dask) time: 1.67 s
- Speedup (Sequential / Parallel): 1.60×

## Analysis questions

1. **Performance comparison**  
   Did Dask provide a noticeable speedup on your machine for the chosen data size?

   Dask did provide a noticeable speedup on my machine, but it depended on the size of the dataset. For larger datasets, the parallel version performed faster because the work was split across multiple CPU cores. However, for smaller datasets, the difference was minimal and sometimes Pandas was just as fast or even faster due to the overhead of managing parallel tasks in Dask.

2. **Ease of use**  
   How similar was the Dask code in `run_parallel_dask_aggregation` to the Pandas baseline, and how much effort was needed?

   The Dask code was very similar to the Pandas version. The main difference was converting the Pandas DataFrame into a Dask DataFrame using dd.from_pandas() and then calling .compute() at the end. The groupby and mean operations were almost identical, which made it easy to switch from Pandas to Dask without needing to learn a completely new API.

3. **Lazy evaluation**  
   When was computation actually triggered in your function, and why is laziness helpful here?

   The computation was only triggered when .compute() was called. Before that, Dask was just building a task graph of operations without executing them. This laziness is useful because it allows Dask to optimize the execution plan and avoid unnecessary work, making it more efficient when working with large datasets or multiple chained operations.

4. **Abstraction**  
   Which complexities did Dask handle for you vs using `multiprocessing.Pool` or `mpi4py` (partitioning, scheduling, result collection, potential fault tolerance)?

   Dask handled several complex tasks automatically, including splitting the data into partitions, scheduling tasks across multiple workers, and combining the results at the end. Compared to using multiprocessing.Pool or mpi4py, I did not need to manually manage processes, distribute data, or handle communication between workers. This makes Dask much easier to use while still providing parallel performance.
