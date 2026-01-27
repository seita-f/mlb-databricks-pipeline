# Databricks notebook source
# MAGIC %pip install pybaseball

# COMMAND ----------

from pybaseball import statcast
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("target_start_date", "2025-01-01")
dbutils.widgets.text("target_end_date", "2025-01-31")

target_start_date_str = dbutils.widgets.get("target_start_date")
target_end_date_str = dbutils.widgets.get("target_end_date")

# COMMAND ----------

pdf = statcast(start_dt=target_start_date_str, end_dt=target_end_date_str)
df = spark.createDataFrame(pdf)

df_to_save = (df
    .withColumn("game_date_dt", F.to_date("game_date"))
    .withColumn("y", F.year("game_date_dt"))
    .withColumn("m", F.format_string("%02d", F.month("game_date_dt")))
    .withColumn("d", F.format_string("%02d", F.dayofmonth("game_date_dt")))
    .drop("game_date_dt") 
)

# COMMAND ----------

root_path = "/Volumes/mlb/00_landing/data_sources/statcast"

(df_to_save.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("y", "m", "d")
    .save(root_path))

print(f"Data saved to {root_path} partitioned by y, m, d")