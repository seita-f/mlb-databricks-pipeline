# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

root_volume = "/Volumes/mlb/00_landing/data_sources/statcast"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

bronze_df = (
    spark.read
         .format("parquet")
         .option("basePath", root_volume)
         .load(f"{root_volume}/y=*/m=*/d=*")
)

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

bronze_df = bronze_df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

bronze_df.write.mode("overwrite").saveAsTable("mlb.01_bronze.statcast")
