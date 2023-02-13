# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Benchmark Pandas and Polars in some typical data analysis scenarios
#
# * [x] Select random row
# * [x] Select a slice
# * [x] join
# * [x] asof join

# %% [markdown]
# # Generate random data

# %% tags=[]
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime

from collections import defaultdict

# %% tags=[]
# helper functions for benchmarking
parse_timeresult = lambda tr:dict(mean=tr.average, std=tr.stdev)

benchmark_data = defaultdict(lambda :dict()) # example: {('scenario', 'pandas'): {'mean': 0, 'std': 1}, ...}

# %% tags=[]
N = 1000_000

# %% tags=[]
times = (pd.Timestamp("2020-01-01") + pd.to_timedelta(np.random.uniform(0, 365*24*3600, N), unit='s'))

vehicles = np.random.randint(0, 10, N)
x = np.random.uniform(0, 1, N)

pd.DataFrame(dict(timestamp=times, vehicle=vehicles, x=x)).to_parquet("fake_vehicle_data.pq", allow_truncated_timestamps=True, coerce_timestamps='ms')

# %% [markdown]
# # Some benchmarks

# %% [markdown]
# ## Read the data

# %% tags=[]
import pandas as pd

# %% tags=[]
df_pd = pd.read_parquet("fake_vehicle_data.pq")
df_pl = pl.read_parquet("fake_vehicle_data.pq")
print(df_pd.head())
print(df_pl.head())

# %% tags=[]
df_pd_indexed = df_pd.set_index('timestamp')
df_pd_indexed_sorted = df_pd_indexed.sort_index()

df_pl_sorted = df_pl.sort(by='timestamp')

# %% [markdown] tags=[]
# ## Retrieving single row

# %% tags=[]
single_timestamp = df_pd.iloc[df_pd.shape[0]//2].timestamp
single_timestamp

# %% [markdown]
# ### In unsorted unindexed df

# %% tags=[]
# tr = %timeit -o df_pd.loc[df_pd.timestamp==single_timestamp]
benchmark_data[('retrieve_singlerow_unsorted_unindexed', 'pandas')] = parse_timeresult(tr)

# %% tags=[]
# tr = %timeit -o df_pl.filter(pl.col('timestamp')==single_timestamp)
benchmark_data[('retrieve_singlerow_unsorted_unindexed', 'polars')] = parse_timeresult(tr)

# %% [markdown]
# ### In unsorted indexed df

# %% tags=[]
# tr = %timeit -o df_pd_indexed.loc[single_timestamp, :]
benchmark_data[('retrieve_singlerow_unsorted_indexed', 'pandas')] = parse_timeresult(tr)

# %% [markdown]
# ### In sorted (indexed for pandas) df

# %% tags=[]
# tr = %timeit -o df_pd_indexed_sorted.loc[single_timestamp, :]
benchmark_data[('retrieve_singlerow_sorted', 'pandas')] = parse_timeresult(tr)

# %% tags=[]
# tr = %timeit -o df_pl_sorted.filter(pl.col('timestamp')==single_timestamp)
benchmark_data[('retrieve_singlerow_sorted', 'polars')] = parse_timeresult(tr)

# %% [markdown]
# ## Retrieving a slice

# %% [markdown]
# ### In unsorted df

# %% tags=[]
# tr = %timeit -o df_pd.loc['2020-04-01':'2020-04-30']
benchmark_data[('retrieve_slice', 'pandas')] = parse_timeresult(tr)

# %% tags=[]
# %%timeit -o
df_pl.filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
).sum()

# %%
benchmark_data[('retrieve_slice', 'polars')] = parse_timeresult(_)

# %% [markdown]
# ### In sorted df

# %% tags=[]
# tr = %timeit -o df_pd_indexed_sorted.loc['2020-04-01':'2020-04-30'].sum()
benchmark_data[('retrieve_slice_sorted', 'pandas')] = parse_timeresult(tr)

# %% tags=[]
# %%timeit -o
df_pl_sorted.filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
).sum()

# %%
benchmark_data[('retrieve_slice_sorted', 'polars')] = parse_timeresult(_)

# %% [markdown]
# ## asof join 

# %% tags=[]
df_pd1 = df_pd.iloc[::2].set_index("timestamp").rename(columns={'x': 'a'}).sort_index()
df_pd2 = df_pd.iloc[1::2].set_index("timestamp").rename(columns={'x': 'b'}).sort_index()

# %% tags=[]
# %%timeit -o
pd.merge_asof(
    left=df_pd1,
    right=df_pd2,
    left_index=True,
    right_index=True,
    by='vehicle',
    suffixes=('_left', '_right'),
)

# %%
benchmark_data[('asof_join', 'pandas')] = parse_timeresult(_)

# %% tags=[]
df_pl1 = df_pl[::2].sort(by="timestamp").rename({'x': 'a'})
df_pl2 = df_pl[1::2].sort(by="timestamp").rename({'x': 'b'})

# %% tags=[]
# %%timeit -o
df_pl1.join_asof(
    other=df_pl2,
    on='timestamp',
    by='vehicle',
    suffix='_right',
)

# %%
benchmark_data[('asof_join', 'polars')] = parse_timeresult(_)

# %% [markdown]
# ## normal join

# %% tags=[]
## prepare fake columns to join on
import numpy as np

rstate = np.random.default_rng(seed=0)

j1 = rstate.integers(0, len(df_pd1)*100, len(df_pd1))
df_pd1_p = df_pd1.assign(j=j1).set_index('j')

j2 = rstate.integers(0, len(df_pd2)*100, len(df_pd2))
df_pd2_p = df_pd2.assign(j=j2).set_index('j')

# %% tags=[]
# %%timeit -o
df_pd1_p.merge(df_pd2_p, left_index=True, right_index=True, how="inner", suffixes=['', '_right'])


# %%
benchmark_data[('join', 'pandas')] = parse_timeresult(_)

# %% tags=[]
## prepare fake columns to join on
import numpy as np

rstate = np.random.default_rng(seed=0)

j1 = rstate.integers(0, len(df_pl1)*100, len(df_pl1))
df_pl1_p = df_pl1.with_columns(pl.Series(name="j", values=j1))

j2 = rstate.integers(0, len(df_pl2)*100, len(df_pl2))
df_pl2_p = df_pl2.with_columns(pl.Series(name="j", values=j2))

# %% tags=[]
# %%timeit -o
df_pl1_p.join(df_pl2_p, on="j", how="inner")



# %%
benchmark_data[('join', 'polars')] = parse_timeresult(_)

# %% [markdown]
# # Display benchmark result

# %% tags=[]
summary = pd.DataFrame(benchmark_data).T.unstack().sort_index()
summary

# %% tags=[]
speedup_summary = (summary.loc[:, ('mean', 'pandas')]/summary.loc[:, ('mean', 'polars')]).rename("speedup_vs_pandas").to_frame()

speedup_summary

# %% tags=[]
with open("benchmark_results.md", 'w') as f:
    f.write(speedup_summary.to_markdown())
