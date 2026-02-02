# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

brz_player_df = spark.read.table("mlb.01_bronze.player_name")

# COMMAND ----------

slv_player_name_df = brz_player_df.filter(F.col("key_mlbam").isNotNull()).select(
    F.col("key_mlbam").cast("long").alias("player_id"),
    F.concat_ws(
        " ", 
        F.trim(F.col("name_first")), 
        F.trim(F.col("name_last"))
    ).alias("player_name"),
    # empty SCD2 
    F.current_date().alias("effective_date"),
    F.lit(None).cast("date").alias("end_date"),
    F.lit(True).alias("is_current")
)

# COMMAND ----------

# silver_player_name_df.display()

# COMMAND ----------

slv_player_name_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("mlb.02_silver.player_name")
