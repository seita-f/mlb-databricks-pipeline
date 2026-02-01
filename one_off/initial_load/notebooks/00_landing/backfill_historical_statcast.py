# Databricks notebook source
# MAGIC %pip install pybaseball

# COMMAND ----------

from pybaseball import statcast
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("target_start_date", "2025-01-01")
dbutils.widgets.text("target_end_date", "2025-12-31")

target_start_date_str = dbutils.widgets.get("target_start_date")
target_end_date_str = dbutils.widgets.get("target_end_date")

# COMMAND ----------

df.display()

# COMMAND ----------

stat_df = statcast(start_dt=target_start_date_str, end_dt=target_end_date_str)
df = spark.createDataFrame(stat_df)

df_to_save = (df
    .withColumn("y", F.year("game_date"))
    .withColumn("m", F.format_string("%02d", F.month("game_date")))
    .withColumn("d", F.format_string("%02d", F.dayofmonth("game_date")))
)

root_path = "/Volumes/mlb/00_landing/data_sources/statcast"

# partitioned by y, m, d 
(df_to_save.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("y", "m", "d")
    .save(root_path))

print(f"Data saved to {root_path} with structure: y=YYYY/m=MM/d=DD")
