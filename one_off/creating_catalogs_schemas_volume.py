# Databricks notebook source
spark.sql("create catalog if not exists mlb managed location 'abfss://unity-catalog-storage@dbstoragefopdnnasraguq.dfs.core.windows.net/7405608285525042'")

# COMMAND ----------

spark.sql("create schema if not exists mlb.00_landing")
spark.sql("create schema if not exists mlb.01_bronze")
spark.sql("create schema if not exists mlb.02_silver")
spark.sql("create schema if not exists mlb.03_gold")

# COMMAND ----------

spark.sql("create volume if not exists mlb.00_landing.data_sources")