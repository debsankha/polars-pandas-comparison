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
df.head()

# %% [markdown]
# ## Selecting data

# %% [markdown]
# ### In unsorted df

# %% tags=[]
df.filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
).sum()

# %% [markdown]
# ### In sorted df

# %%
df_sorted = df.sort(by='timestamp')

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

# %%
