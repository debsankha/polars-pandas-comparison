// ---
// jupyter:
//   jupytext:
//     formats: ipynb,rs:percent
//     text_representation:
//       extension: .rs
//       format_name: percent
//       format_version: '1.3'
//       jupytext_version: 1.14.1
//   kernelspec:
//     display_name: Rust
//     language: rust
//     name: rust
// ---

// %% tags=[]
// :dep polars = { version = "0.24.3", features = ["lazy", "parquet", "csv-file", "strings", "temporal", "dtype-duration", "dtype-categorical", "concat_str", "list", "list_eval", "rank", "lazy_regex"]}
// :dep color-eyre = {version = "0.6.2"}
// :dep rand = {version = "0.8.5"}
// :dep reqwest = { version = "0.11.11", features = ["blocking"]}

use color_eyre::{Result};
use polars::prelude::*;

// %% tags=[]
let mut file = std::fs::File::open("fake_vehicle_data.pq").unwrap();

let df: polars::prelude::DataFrame = polars::prelude::ParquetReader::new(&mut file).finish().unwrap();

// %% tags=[]
df

// %% tags=[]
// :dep chrono

use chrono::NaiveDate;

// %% tags=[]
let t_start: NaiveDate = NaiveDate::from_ymd_opt(2020, 4, 1).unwrap();
let t_end: NaiveDate = NaiveDate::from_ymd_opt(2020, 4, 30).unwrap();

// %% tags=[]
df.clone().lazy().filter(col("timestamp").gt(lit(t_start))).collect()?.lazy().filter(col("timestamp").lt(lit(t_end))).collect()?

// %%
