# Databricks notebook source
silver_base_df = spark.read.table("mlb.02_silver.statcast_base")

# COMMAND ----------

silver_base_df.display()

# COMMAND ----------

# silver_base_df.printSchema()

# COMMAND ----------

silver_base_df = silver_base_df.filter("game_type == 'R'")

# COMMAND ----------

silver_game_context_df = silver_base_df.select(
    # game 
    "game_pk", 
    "game_date", 
    "game_type",
    "home_team", 
    "away_team",

    # tracking
    "home_score", 
    "away_score",
    "events",
    "description", 
    "description_details", 
    "balls", 
    "strikes",
    "pitch_type", 
    "pitch_result",
    "hit_location",
    "inning", 
    "inning_topbot",
    "outs_when_up", 

    # player
    "batter", 
    "pitcher", 
    "fielder_2",
    "fielder_3",
    "fielder_4",
    "fielder_5",
    "fielder_6",
    "fielder_7",
    "fielder_8",
    "fielder_9",
  
    # runner on base
    "on_1b", 
    "on_2b", 
    "on_3b",

    "prcessed_timestamp",
)


# COMMAND ----------

silver_game_context_df.write.mode("overwrite").saveAsTable("mlb.02_silver.statcast_game_context")