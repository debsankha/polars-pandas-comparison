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

# %% tags=[]
import pandas as pd

# %% tags=[]
df = pd.read_parquet("fake_vehicle_data.pq")
df

# %% tags=[]
df_indexed = df.set_index('timestamp').sort_index()

# %% [markdown] tags=[]
# ## Retrieving single row

# %% tags=[]
single_timestamp = df.iloc[df.shape[0]//2].timestamp
single_timestamp

# %% [markdown]
# ### In unsorted unindexed df

# %% tags=[]
# %timeit df.loc[df.timestamp==single_timestamp]

# %%
df_indexed_unsorted = df.set_index("timestamp")

# %% [markdown]
# ## In unsorted indexed df

# %% tags=[]
# %timeit df_indexed_unsorted.loc[single_timestamp, :]

# %% [markdown]
# ## In sorted indexed df

# %%
# %timeit df_indexed.loc[single_timestamp, :]

# %% [markdown]
# ## Selecting data

# %% [markdown]
# ### In unsorted df

# %% tags=[]
# %timeit df.loc['2020-04-01':'2020-04-30']

# %% [markdown]
# ### In sorted df

# %% tags=[]
# %timeit df_indexed.loc['2020-04-01':'2020-04-30'].sum()

# %% [markdown]
# ## asof join 

# %% tags=[]
df1 = df.iloc[::2].set_index("timestamp").rename(columns={'x': 'a'}).sort_index()
df2 = df.iloc[1::2].set_index("timestamp").rename(columns={'x': 'b'}).sort_index()

# %% tags=[]
# %%timeit
pd.merge_asof(
    left=df1,
    right=df2,
    left_index=True,
    right_index=True,
    by='vehicles',
    suffixes=('_left', '_right'),
)

# %% [markdown]
# ## normal join

# %% tags=[]
## prepare fake columns to join on
import numpy as np

rstate = np.random.RandomState(seed=0)

j1 = rstate.randint(0, len(df1)*100, len(df1))
df1_p = df1.assign(j=j1).set_index('j')

j2 = rstate.randint(0, len(df2)*100, len(df2))
df2_p = df2.assign(j=j2).set_index('j')

# %% tags=[]
# %%timeit
df1_p.merge(df2_p, left_index=True, right_index=True, how="inner", suffixes=['', '_right'])

# %%
