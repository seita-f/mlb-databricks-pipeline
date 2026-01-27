# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

root_volume = "/Volumes/mlb/00_landing/data_sources/statcast/*"

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEV

# COMMAND ----------

# DEV
all_games = (spark.read.format("parquet")
             .load(root_volume)
             .select("game_pk")
             .distinct())

sample_game_pks = all_games.sample(withReplacement=False, fraction=0.01, seed=42)

# COMMAND ----------

bronze_df = (spark.read.format("parquet")
             .load(root_volume)
             .join(sample_game_pks, on="game_pk", how="inner")
             )

print(f"Sampled Games: {sample_game_pks.count()}")
print(f"Total Rows (full games): {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

# bronz_df = spark.read.format("parquet").load(root_volume)

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

bronze_df = bronze_df.withColumn("prcessed_timestamp", current_timestamp())

# COMMAND ----------

bronze_df.write.mode("overwrite").saveAsTable("mlb.01_bronze.statcast")