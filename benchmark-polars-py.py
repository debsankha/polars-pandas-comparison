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
# # Some benchmarks

# %% [markdown]
# ## Read the data

# %%
import polars as pl

from datetime import datetime, timedelta 

# %%
df = pl.read_parquet("fake_vehicle_data.pq")
df

# %%
df_sorted = df.sort(by='timestamp')

# %% [markdown] tags=[]
# ## Retrieving single row

# %% tags=[]
single_timestamp = df[df.shape[0]//2, 'timestamp']
single_timestamp

# %% [markdown]
# ### In unsorted df

# %% tags=[]
# %timeit df.filter(pl.col('timestamp')==single_timestamp)

# %% [markdown]
# ### In sorted df

# %% tags=[]
# %timeit df_sorted.filter(pl.col('timestamp')==single_timestamp)

# %% [markdown]
# ## Selecting data

# %% [markdown] tags=[]
# ### In unsorted df

# %% tags=[]
df.filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
).sum()

# %% tags=[]
df.lazy().filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
).sum().collect()

# %% [markdown]
# ### In sorted df

# %% tags=[]
df_sorted.filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
).sum()

# %% [markdown]
# # asof join

# %% tags=[]
df1 = df[::2].sort(by="timestamp").rename({'x': 'a'})
df2 = df[1::2].sort(by="timestamp").rename({'x': 'b'})

# %% tags=[]
df1.join_asof(
    other=df2,
    on='timestamp',
    by='vehicles',
    suffix='_right',
)

# %% [markdown]
# ## Normal join

# %% tags=[]
## prepare fake columns to join on
import numpy as np

rstate = np.random.RandomState(seed=0)

j1 = rstate.randint(0, len(df1)*100, len(df1))
df1_p = df1.with_columns(pl.Series(name="j", values=j1))

j2 = rstate.randint(0, len(df2)*100, len(df2))
df2_p = df2.with_columns(pl.Series(name="j", values=j2))

# %% tags=[]
# %%timeit
df1_p.join(df2_p, on="j", how="inner")

# %%
