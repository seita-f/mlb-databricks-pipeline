# Databricks notebook source
silver_base_df = spark.read.table("mlb.02_silver.statcast_base")

# COMMAND ----------

# silver_base_df.display()

# COMMAND ----------

# silver_base_df.printSchema()

# COMMAND ----------

silver_game_context_df = silver_base_df.select(
    # game 
    "game_pk", 
    "game_date", 
    "game_type",
    "home_team", 
    "away_team",
    "home_score", 
    "away_score",
    "batting_team",
    "fielding_team",
    "idx_game_pitch",

    # tracking
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
    "intercept_ball_minus_batter_pos_x_inches",
    "intercept_ball_minus_batter_pos_y_inches",

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

    # expectations
    "delta_home_win_exp",
    "delta_run_exp",

    "prcessed_timestamp",
)


# COMMAND ----------

silver_game_context_df.display()

# COMMAND ----------

silver_game_context_df.write.mode("overwrite").saveAsTable("mlb.02_silver.statcast_game_context")
