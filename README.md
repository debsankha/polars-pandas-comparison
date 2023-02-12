# Benchmarking polars vs pandas with focus on time series

## Installing requirements
```bash
pip install polars pandas jupyterlab
```

## Running the benchmarks

We are using `jupytext` for checking in notebooks to git.

1. Generate data: `generate_data.py`
2. Run pandas benchmarks: `benchmark-pandas.py`
2. Run polars benchmarks: `benchmark-polars-py.py`

There's a rust notebook, running polars benchmarks using its native rust api,
but it's not complete: `benchmark-polars-rs.rs`