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

# %% [markdown]
# ## Selecting data

# %% [markdown]
# ### In unsorted df

# %% tags=[]
# %timeit df.loc['2020-04-01':'2020-04-30']

# %% [markdown]
# ### In sorted df

# %% tags=[]
df_indexed = df.set_index('timestamp').sort_index()

# %% tags=[]
df_indexed.loc['2020-04-01':'2020-04-30'].sum()

# %% [markdown]
# ## asof join 

# %% tags=[]
df1 = df.iloc[::2].set_index("timestamp").rename(columns={'x': 'a'}).sort_index()
df2 = df.iloc[1::2].set_index("timestamp").rename(columns={'x': 'b'}).sort_index()

# %% tags=[]
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

# %%
