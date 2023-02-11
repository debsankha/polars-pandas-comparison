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

# %%
import polars as pl

from datetime import datetime, timedelta 

# %%
df = pl.read_parquet("fake_vehicle_data.pq")
df

# %% tags=[]
df.filter(
    pl.col("timestamp").is_between(datetime(2020, 4, 1), datetime(2020, 4, 30)),
)

# %%
