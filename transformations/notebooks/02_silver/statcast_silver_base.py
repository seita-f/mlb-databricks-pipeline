# Databricks notebook source
# MAGIC %md
# MAGIC # DOC / Note
# MAGIC - statcast doc: https://baseballsavant.mlb.com/csv-docs
# MAGIC - `zone`: http://reddit.com/r/Sabermetrics/comments/o511ks/how_to_interpret_statcast_zones/
# MAGIC - remain fielder's posisiton as number (well known in baseball)

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading 

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

silver_table_name = "mlb.02_silver.statcast_base"
task_name = "01_Statcast_Raw"

# COMMAND ----------

start_dt = dbutils.jobs.taskValues.get(taskKey=task_name, key="start_date")
end_dt = dbutils.jobs.taskValues.get(taskKey=task_name, key="end_date")
continue_flag = dbutils.jobs.taskValues.get(taskKey=task_name, key="continue_downstream", default="no")

if continue_flag == "no":
    dbutils.notebook.exit("Skipping Silver layer: No new data.")

# COMMAND ----------

df_bronze = (spark.table("mlb.01_bronze.statcast")
             .filter(F.col("game_date").between(start_dt, end_dt)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Main Part

# COMMAND ----------

MPH_TO_KMH = 1.60934
FEET_TO_CM = 30.48
FEET_TO_M = 0.3048
INCH_TO_CM = 2.54

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preprocessing 

# COMMAND ----------

# Eliminate spring trainig and exhibiton game 
df = bronze_df.filter((col("game_type") != "S") & (col("game_type") != "E"))

# COMMAND ----------


silver_base_df = df.select(
    "game_pk",
    F.to_date(col("game_date")).alias("date"),
    "pitch_type",
    "events",
    "description",
    "home_team",
    "away_team",
    "pitch_name",
    "at_bat_number",
    "home_score",
    "away_score",
    "delta_home_win_exp",
    "delta_run_exp",
    "if_fielding_alignment",
    "of_fielding_alignment",
    "intercept_ball_minus_batter_pos_x_inches",
    "intercept_ball_minus_batter_pos_y_inches",
    "inning_topbot",
    "inning",
    "pitch_number",
    "balls",
    "strikes",
    "hit_location",
    "outs_when_up",
    "prcessed_timestamp",
    "on_1b", 
    "on_2b", 
    "on_3b",
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

    # Speeds: mph -> km/h
    F.round(col("release_speed").cast("double") * MPH_TO_KMH, 2).alias("pitcher_release_speed_kmh"),
    F.round(col("launch_speed").cast("double") * MPH_TO_KMH, 2).alias("launch_speed_kmh"),
    F.round(col("bat_speed").cast("double") * MPH_TO_KMH, 2).alias("bat_speed_kmh"),
    
    # Positions / movement: ft -> cm
    F.round(col("release_pos_x").cast("double") * FEET_TO_CM, 2).alias("pitch_release_pos_x_cm_catcher_view"),
    F.round(col("release_pos_y").cast("double") * FEET_TO_CM, 2).alias("pitch_release_pos_y_cm_catcher_view"),
    F.round(col("release_pos_z").cast("double") * FEET_TO_CM, 2).alias("pitch_release_pos_z_cm_catcher_view"),
    F.round(col("pfx_x").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_move_x_cm_catcher_view"),
    F.round(col("pfx_z").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_move_z_cm_catcher_view"),
    F.round(col("plate_x").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_pos_x_cm_catcher_view"),
    F.round(col("plate_z").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_pos_z_cm_catcher_view"),
    F.round(col("sz_top").cast("double") * FEET_TO_CM, 2).alias("strike_zone_top_cm"),
    F.round(col("sz_bot").cast("double") * FEET_TO_CM, 2).alias("strike_zone_bot_cm"),
    F.round(col("swing_length").cast("double") * FEET_TO_CM, 2).alias("swing_length_cm"),

    F.round(col("hc_x").cast("double") * FEET_TO_M, 2).alias("hit_field_coord_x_m"),
    F.round(col("hc_y").cast("double") * FEET_TO_M, 2).alias("hit_field_coord_y_m"),

    # Velocities: ft/s -> m/s
    F.round(col("vx0").cast("double") * FEET_TO_M, 2).alias("pitch_ball_vx_ms"),
    F.round(col("vy0").cast("double") * FEET_TO_M, 2).alias("pitch_ball_vy_ms"),
    F.round(col("vz0").cast("double") * FEET_TO_M, 2).alias("pitch_ball_vz_ms"),

    # Accelerations: ft/s^2 -> m/s^2
    F.round(col("ax").cast("double") * FEET_TO_M, 2).alias("pitch_ball_ax_ms2"),
    F.round(col("ay").cast("double") * FEET_TO_M, 2).alias("pitch_ball_ay_ms2"),
    F.round(col("az").cast("double") * FEET_TO_M, 2).alias("pitch_ball_az_ms2"),

    # Hit distance: ft -> m
    F.round(col("hit_distance_sc").cast("double") * FEET_TO_M, 2).alias("hit_dist_m"),

    # game type
    F.when(col("game_type") == "R", "Regular Season")
      F.when(col("game_type") == "W", "Wild Card")
      F.when(col("game_type") == "D", "Divisional Series")
      F.when(col("game_type") == "L", "League Championship Series")
      F.when(col("game_type") == "W", "World Series")
      .otherwise(None)
      .alias("game_type"),

    # launch_speed_angle label
    Fwhen(col("launch_speed_angle") == 1, "Weak")
      F.when(col("launch_speed_angle") == 2, "Topped")
      F.when(col("launch_speed_angle") == 3, "Under")
      F.when(col("launch_speed_angle") == 4, "Flare/Burner")
      F.when(col("launch_speed_angle") == 5, "Solid Contact")
      F.when(col("launch_speed_angle") == 6, "Barrel")
      .otherwise(None)
      .alias("launch_speed_angle"),

    # zone
    Fwhen(col("zone") == 1, "high-left")
      F.when(col("zone") == 2, "high-center")
      F.when(col("zone") == 3, "high-right")
      F.when(col("zone") == 4, "middle-left")
      F.when(col("zone") == 5, "middle-center")
      F.when(col("zone") == 6, "middle-right")
      F.when(col("zone") == 7, "low-left")
      F.when(col("zone") == 8, "low-center")
      F.when(col("zone") == 9, "low-right")
      F.when(col("zone") == 11, "waste-high-left")  # 10 is abandoned
      F.when(col("zone") == 12, "waste-high-right")
      F.when(col("zone") == 13, "waste-low-left")
      F.when(col("zone") == 14, "waste-low-right")
      .otherwise(None)
      .alias("pitch_zone_catcher_view"),

    # Team for hit and field
    Fwhen(col("inning_topbot") == "Top", col("away_team"))
      F.when(col("inning_topbot") == "Bot", col("home_team"))
      .alias("batting_team"),

    Fwhen(col("inning_topbot") == "Top", col("home_team"))
      F.when(col("inning_topbot") == "Bot", col("away_team"))
      .alias("fielding_team"),

    # Renames
    col("p_throws").alias("pitch_hand"),
    col("stand").alias("batter_side"),
    col("des").alias("description_details"),
    col("type").alias("pitch_result"),
    col("bb_type").alias("hit_ball_type"),
    col("spin_axis").cast("int").alias("spin_axis_degree"),
    col("arm_angle").cast("double").alias("pitch_arm_angle"),
)

# COMMAND ----------

# silver_base_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort by game event

# COMMAND ----------

window_spec = Window.partitionBy("game_pk").orderBy("at_bat_number", "pitch_number")

silver_indexed_base_df = silver_base_df.withColumn(
    "idx_game_pitch", 
    F.row_number().over(window_spec)
)

# COMMAND ----------

# silver_indexed_base_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save

# COMMAND ----------

if spark.catalog.tableExists(silver_table_name):
    target_table = DeltaTable.forName(spark, silver_table_name)
    
    # Merge
    (target_table.alias("t")
     .merge(
         silver_indexed_base_df.alias("s"),
         "t.game_pk = s.game_pk AND t.at_bat_number = s.at_bat_number AND t.pitch_number = s.pitch_number"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll() 
     .execute())
    
    print(f"Merged updates into {silver_table_name}")

else:
    (silver_indexed_base_df.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("date")
     .saveAsTable(silver_table_name))
    
    print(f"Created new table {silver_table_name}")
