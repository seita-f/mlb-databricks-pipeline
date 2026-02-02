# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_table_name = "mlb.02_ssilver.player_name"
task_name = "01_PlayerName_LookUp_Raw" 

# COMMAND ----------

continue_flag = dbutils.jobs.taskValues.get(taskKey=task_name, key="continue_downstream", default="no")

if continue_flag == "no":
    dbutils.notebook.exit("Bronze task skipped. Skipping Silver.")

# COMMAND ----------

updates_df = spark.read.table("mlb.01_bronze.player_name") \
    .filter(F.col("key_mlbam").isNotNull()) \
    .select(
        F.col("key_mlbam").cast("long").alias("player_id"),
        F.concat_ws(" ", F.trim(F.col("name_first")), F.trim(F.col("name_last"))).alias("player_name"),
        F.current_date().alias("effective_date")
    )

# COMMAND ----------

if not spark.catalog.tableExists(silver_table_name):
    updates_df.withColumn("end_date", F.lit(None).cast("date")) \
              .withColumn("is_current", F.lit(True)) \
              .write.format("delta").saveAsTable(silver_table_name)
    dbutils.notebook.exit("Initial table created.")

# COMMAND ----------

target_table = DeltaTable.forName(spark, silver_table_name)

staged_updates = updates_df.alias("s").join(
    target_table.toDF().alias("t"),
    (F.col("s.player_id") == F.col("t.player_id")) & (F.col("t.is_current") == True)
).filter("s.player_name <> t.player_name") \
 .select("s.*")

target_table.alias("t").merge(
    staged_updates.alias("s"),
    "t.player_id = s.player_id AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": "false",
    "end_date": "s.effective_date"
}).execute()

# COMMAND ----------

new_records = updates_df.alias("s").join(
    target_table.toDF().alias("t"),
    (F.col("s.player_id") == F.col("t.player_id")) & (F.col("t.is_current") == True),
    "left_anti"
).withColumn("end_date", F.lit(None).cast("date")) \
 .withColumn("is_current", F.lit(True))

new_records.write.format("delta").mode("append").saveAsTable(silver_table_name)
