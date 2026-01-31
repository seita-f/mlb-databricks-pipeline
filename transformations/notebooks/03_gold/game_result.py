# Databricks notebook source
df_statcast_enrich = spark.read.table("mlb.02_silver.statcast_enrich")

# COMMAND ----------

df_statcast_enrich.display()

# COMMAND ----------


