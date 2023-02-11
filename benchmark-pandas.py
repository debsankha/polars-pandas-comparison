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
# ## Some benchmarks
#
# * Selecting data in sorted df
# * normal join
# * asof_join 

# %%
import pandas as pd

# %% tags=[]
df = pd.read_parquet("fake_vehicle_data.pq")
df

# %% tags=[]
df_indexed = df.set_index('timestamp').sort_index()

# %% tags=[]
# %timeit df.loc['2020-04-01':'2020-04-30']

# %% tags=[]
# %timeit df_indexed.loc['2020-04-01':'2020-04-30']

# %%
