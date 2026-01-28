# Databricks notebook source
game_context_df = spark.read.table("mlb.02_silver.statcast_game_context")

# COMMAND ----------

game_context_df.display()

# COMMAND ----------

game_context_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### bunt
