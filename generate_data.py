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

# %% tags=[]
import pandas as pd
import numpy as np

# %% tags=[]
N = 100_000_000

# %% tags=[]
times = (pd.Timestamp("2020-01-01") + pd.to_timedelta(np.random.uniform(0, 365*24*3600, N), unit='s'))

vehicles = np.random.randint(0, 10, N)
x = np.random.uniform(0, 1, N)

pd.DataFrame(dict(timestamp=times, vehicles=vehicles, x=x)).to_parquet("fake_vehicle_data.pq", allow_truncated_timestamps=True, coerce_timestamps='ms')
