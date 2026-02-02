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
task_name = "00_Ingest_Statcast"

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
df = df_bronze.filter((F.col("game_type") != "S") & (F.col("game_type") != "E"))

# COMMAND ----------


df_silver_base = df.select(
    "game_pk",
    F.to_date(F.col("game_date")).alias("date"),
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
    "processed_timestamp",
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
    F.round(F.col("release_speed").cast("double") * MPH_TO_KMH, 2).alias("pitcher_release_speed_kmh"),
    F.round(F.col("launch_speed").cast("double") * MPH_TO_KMH, 2).alias("launch_speed_kmh"),
    F.round(F.col("bat_speed").cast("double") * MPH_TO_KMH, 2).alias("bat_speed_kmh"),
    
    # Positions / movement: ft -> cm
    F.round(F.col("release_pos_x").cast("double") * FEET_TO_CM, 2).alias("pitch_release_pos_x_cm_catcher_view"),
    F.round(F.col("release_pos_y").cast("double") * FEET_TO_CM, 2).alias("pitch_release_pos_y_cm_catcher_view"),
    F.round(F.col("release_pos_z").cast("double") * FEET_TO_CM, 2).alias("pitch_release_pos_z_cm_catcher_view"),
    F.round(F.col("pfx_x").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_move_x_cm_catcher_view"),
    F.round(F.col("pfx_z").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_move_z_cm_catcher_view"),
    F.round(F.col("plate_x").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_pos_x_cm_catcher_view"),
    F.round(F.col("plate_z").cast("double") * FEET_TO_CM, 2).alias("pitch_ball_pos_z_cm_catcher_view"),
    F.round(F.col("sz_top").cast("double") * FEET_TO_CM, 2).alias("strike_zone_top_cm"),
    F.round(F.col("sz_bot").cast("double") * FEET_TO_CM, 2).alias("strike_zone_bot_cm"),
    F.round(F.col("swing_length").cast("double") * FEET_TO_CM, 2).alias("swing_length_cm"),

    F.round(F.col("hc_x").cast("double") * FEET_TO_M, 2).alias("hit_field_coord_x_m"),
    F.round(F.col("hc_y").cast("double") * FEET_TO_M, 2).alias("hit_field_coord_y_m"),

    # Velocities: ft/s -> m/s
    F.round(F.col("vx0").cast("double") * FEET_TO_M, 2).alias("pitch_ball_vx_ms"),
    F.round(F.col("vy0").cast("double") * FEET_TO_M, 2).alias("pitch_ball_vy_ms"),
    F.round(F.col("vz0").cast("double") * FEET_TO_M, 2).alias("pitch_ball_vz_ms"),

    # Accelerations: ft/s^2 -> m/s^2
    F.round(F.col("ax").cast("double") * FEET_TO_M, 2).alias("pitch_ball_ax_ms2"),
    F.round(F.col("ay").cast("double") * FEET_TO_M, 2).alias("pitch_ball_ay_ms2"),
    F.round(F.col("az").cast("double") * FEET_TO_M, 2).alias("pitch_ball_az_ms2"),

    # Hit distance: ft -> m
    F.round(F.col("hit_distance_sc").cast("double") * FEET_TO_M, 2).alias("hit_dist_m"),

    # game type
    F.when(F.col("game_type") == "R", "Regular Season")
      .when(F.col("game_type") == "W", "Wild Card")
      .when(F.col("game_type") == "D", "Divisional Series")
      .when(F.col("game_type") == "L", "League Championship Series")
      .when(F.col("game_type") == "W", "World Series")
      .otherwise(None)
      .alias("game_type"),

    # launch_speed_angle label
    F.when(F.col("launch_speed_angle") == 1, "Weak")
      .when(F.col("launch_speed_angle") == 2, "Topped")
      .when(F.col("launch_speed_angle") == 3, "Under")
      .when(F.col("launch_speed_angle") == 4, "Flare/Burner")
      .when(F.col("launch_speed_angle") == 5, "Solid Contact")
      .when(F.col("launch_speed_angle") == 6, "Barrel")
      .otherwise(None)
      .alias("launch_speed_angle"),

    # zone
    F.when(F.col("zone") == 1, "high-left")
      .when(F.col("zone") == 2, "high-center")
      .when(F.col("zone") == 3, "high-right")
      .when(F.col("zone") == 4, "middle-left")
      .when(F.col("zone") == 5, "middle-center")
      .when(F.col("zone") == 6, "middle-right")
      .when(F.col("zone") == 7, "low-left")
      .when(F.col("zone") == 8, "low-center")
      .when(F.col("zone") == 9, "low-right")
      .when(F.col("zone") == 11, "waste-high-left")  # 10 is abandoned
      .when(F.col("zone") == 12, "waste-high-right")
      .when(F.col("zone") == 13, "waste-low-left")
      .when(F.col("zone") == 14, "waste-low-right")
      .otherwise(None)
      .alias("pitch_zone_catcher_view"),

    # Team for hit and field
    F.when(F.col("inning_topbot") == "Top", F.col("away_team"))
      .when(F.col("inning_topbot") == "Bot", F.col("home_team"))
      .alias("batting_team"),

    F.when(F.col("inning_topbot") == "Top", F.col("home_team"))
      .when(F.col("inning_topbot") == "Bot", F.col("away_team"))
      .alias("fielding_team"),

    # Renames
    F.col("p_throws").alias("pitch_hand"),
    F.col("stand").alias("batter_side"),
    F.col("des").alias("description_details"),
    F.col("type").alias("pitch_result"),
    F.col("bb_type").alias("hit_ball_type"),
    F.col("spin_axis").cast("int").alias("spin_axis_degree"),
    F.col("arm_angle").cast("double").alias("pitch_arm_angle"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort by game event

# COMMAND ----------

window_spec = Window.partitionBy("game_pk").orderBy("at_bat_number", "pitch_number")

df_silver_indexed_base = df_silver_base.withColumn(
    "idx_game_pitch", 
    F.row_number().over(window_spec)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save

# COMMAND ----------

if spark.catalog.tableExists(silver_table_name):
    target_table = DeltaTable.forName(spark, silver_table_name)
    
    # Merge
    (target_table.alias("t")
     .merge(
         df_silver_indexed_base.alias("s"),
         "t.game_pk = s.game_pk AND t.at_bat_number = s.at_bat_number AND t.pitch_number = s.pitch_number"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll() 
     .execute())
    
    print(f"Merged updates into {silver_table_name}")

else:
    (df_silver_indexed_base.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("date")
     .saveAsTable(silver_table_name))
    
    print(f"Created new table {silver_table_name}")
