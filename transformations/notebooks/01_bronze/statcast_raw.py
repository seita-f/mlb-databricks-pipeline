# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

root_volume = "/Volumes/mlb/00_landing/data_sources/statcast/"
table_name = "mlb.01_bronze.statcast"
task_name = "00_Ingest_Statcast"

# COMMAND ----------

start_dt = dbutils.jobs.taskValues.get(taskKey=task_name, key="start_date")
end_dt = dbutils.jobs.taskValues.get(taskKey=task_name, key="end_date")
continue_flag = dbutils.jobs.taskValues.get(taskKey=task_name, key="continue_downstream", default="no")

if continue_flag == "no":
    dbutils.notebook.exit("No new data to process.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

new_bronze_df = (spark.read.format("parquet").load(root_volume)
                 .filter(F.col("game_date").between(start_dt, end_dt)))

# COMMAND ----------

new_bronze_df = new_bronze_df.withColumn("processed_timestamp", F.current_timestamp())

# COMMAND ----------

if spark.catalog.tableExists(table_name):
    target_table = DeltaTable.forName(spark, table_name)
    
    target_table.alias("t").merge(
        new_bronze_df.alias("s"),
        "t.game_pk = s.game_pk AND t.at_bat_number = s.at_bat_number AND t.pitch_number = s.pitch_number"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # initialize table
    new_bronze_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
