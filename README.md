# Benchmarking polars vs pandas with focus on time series

## Installing requirements
```bash
pip install polars pandas jupyterlab
```

## Running the benchmarks
Run the notebook `benchmark.py`
*Note:* We are using `jupytext` for checking in notebooks to git. 

There's a rust notebook, running polars benchmarks using its native rust api,
but it's not complete: `benchmark-polars-rs.rs`

## Benchmark results
See [benchmark_results.md](./benchmark_results.md)