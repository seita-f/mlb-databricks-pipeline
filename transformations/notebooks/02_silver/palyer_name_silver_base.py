# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

brz_player_df = spark.read.table("mlb.01_bronze.player_name")

# COMMAND ----------

brz_player_df = brz_player_df.filter(F.col("key_mlbam").isNotNull())

# COMMAND ----------

slv_player_name_df = brz_player_df.select(
    F.col("key_mlbam").cast("long").alias("player_id"),
    F.concat_ws(
        " ", 
        F.trim(F.col("name_first")), 
        F.trim(F.col("name_last"))
    ).alias("player_name"),
    "ingestion_timestamp",
    "source_file"
)

# COMMAND ----------

# silver_player_name_df.display()

# COMMAND ----------

slv_player_name_df.display()
slv_player_name_df.write.mode("overwrite").saveAsTable("mlb.02_silver.player_name")
slv_player_name_df = spark.read
