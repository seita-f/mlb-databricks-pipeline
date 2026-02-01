# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

enrich_table_name = "mlb.02_silver.statcast_enrich"

# COMMAND ----------

task_name = "ingest_statcast" 
start_dt = dbutils.jobs.taskValues.get(taskKey=task_name, key="start_date")
end_dt = dbutils.jobs.taskValues.get(taskKey=task_name, key="end_date")
continue_flag = dbutils.jobs.taskValues.get(taskKey=task_name, key="continue_downstream", default="no")

if continue_flag == "no":
    dbutils.notebook.exit("No new data to enrich.")

statcast_df = spark.read.table("mlb.02_silver.statcast_base").filter(F.col("date").between(start_dt, end_dt))
player_name_df = spark.read.table("mlb.02_silver.player_name")

# COMMAND ----------

df_join_batter = statcast_df.join(
        player_name_df, 
        statcast_df.batter == player_name_df.player_id,
        how="left",
    ).select(
        statcast_df.date,
        statcast_df.game_pk,
        player_name_df.player_name.alias("batter"),
        statcast_df.pitch_type,
        statcast_df.events,
        statcast_df.description,
        statcast_df.game_type,
        statcast_df.home_team,
        statcast_df.away_team,
        statcast_df.pitch_name,
        statcast_df.at_bat_number,
        statcast_df.home_score,
        statcast_df.away_score,
        statcast_df.delta_home_win_exp,
        statcast_df.delta_run_exp,
        statcast_df.if_fielding_alignment,
        statcast_df.of_fielding_alignment,
        statcast_df.intercept_ball_minus_batter_pos_x_inches,
        statcast_df.intercept_ball_minus_batter_pos_y_inches,
        statcast_df.inning_topbot,
        statcast_df.inning,
        statcast_df.pitch_number,
        statcast_df.balls,
        statcast_df.strikes,
        statcast_df.hit_location,
        statcast_df.outs_when_up,
        statcast_df.prcessed_timestamp,
        statcast_df.on_1b,
        statcast_df.on_2b,
        statcast_df.on_3b,
        statcast_df.pitcher,
        statcast_df.fielder_2,
        statcast_df.fielder_3,
        statcast_df.fielder_4,
        statcast_df.fielder_5,
        statcast_df.fielder_6,
        statcast_df.fielder_7,
        statcast_df.fielder_8,
        statcast_df.fielder_9,
        statcast_df.pitcher_release_speed_kmh,
        statcast_df.launch_speed_kmh,
        statcast_df.bat_speed_kmh,
        statcast_df.pitch_release_pos_x_cm_catcher_view,
        statcast_df.pitch_release_pos_y_cm_catcher_view,
        statcast_df.pitch_release_pos_z_cm_catcher_view,
        statcast_df.pitch_ball_move_x_cm_catcher_view,
        statcast_df.pitch_ball_move_z_cm_catcher_view,
        statcast_df.pitch_ball_pos_x_cm_catcher_view,
        statcast_df.pitch_ball_pos_z_cm_catcher_view,
        statcast_df.strike_zone_top_cm,
        statcast_df.strike_zone_bot_cm,
        statcast_df.swing_length_cm,
        statcast_df.hit_field_coord_x_m,
        statcast_df.hit_field_coord_y_m,
        statcast_df.pitch_ball_vx_ms,
        statcast_df.pitch_ball_vy_ms,
        statcast_df.pitch_ball_vz_ms,
        statcast_df.pitch_ball_ax_ms2,
        statcast_df.pitch_ball_ay_ms2,
        statcast_df.pitch_ball_az_ms2,
        statcast_df.hit_dist_m,
        statcast_df.launch_speed_angle,
        statcast_df.pitch_zone_catcher_view,
        statcast_df.batting_team,
        statcast_df.fielding_team,
        statcast_df.pitch_hand,
        statcast_df.batter_side,
        statcast_df.description_details,
        statcast_df.pitch_result,
        statcast_df.hit_ball_type,
        statcast_df.spin_axis_degree,
        statcast_df.pitch_arm_angle,
        statcast_df.idx_game_pitch,
    )

# COMMAND ----------

df_join_final = df_join_batter.join(
    player_name_df,
    df_join_batter.pitcher == player_name_df.player_id,
    how="left",
).select(
    df_join_batter.game_pk,
    df_join_batter.date,
    player_name_df.player_name.alias("pitcher"),
    df_join_batter.batter, 
    df_join_batter.pitch_type,
    df_join_batter.events,
    df_join_batter.description,
    df_join_batter.game_type,
    df_join_batter.home_team,
    df_join_batter.away_team,
    df_join_batter.pitch_name,
    df_join_batter.at_bat_number,
    df_join_batter.home_score,
    df_join_batter.away_score,
    df_join_batter.delta_home_win_exp,
    df_join_batter.delta_run_exp,
    df_join_batter.if_fielding_alignment,
    df_join_batter.of_fielding_alignment,
    df_join_batter.intercept_ball_minus_batter_pos_x_inches,
    df_join_batter.intercept_ball_minus_batter_pos_y_inches,
    df_join_batter.inning_topbot,
    df_join_batter.inning,
    df_join_batter.pitch_number,
    df_join_batter.balls,
    df_join_batter.strikes,
    df_join_batter.hit_location,
    df_join_batter.outs_when_up,
    df_join_batter.prcessed_timestamp,
    df_join_batter.on_1b,
    df_join_batter.on_2b,
    df_join_batter.on_3b,
    
    # For now, remaining fielders as id
    df_join_batter.fielder_2,
    df_join_batter.fielder_3,
    df_join_batter.fielder_4,
    df_join_batter.fielder_5,
    df_join_batter.fielder_6,
    df_join_batter.fielder_7,
    df_join_batter.fielder_8,
    df_join_batter.fielder_9,
    df_join_batter.pitcher_release_speed_kmh,
    df_join_batter.launch_speed_kmh,
    df_join_batter.bat_speed_kmh,
    df_join_batter.pitch_release_pos_x_cm_catcher_view,
    df_join_batter.pitch_release_pos_y_cm_catcher_view,
    df_join_batter.pitch_release_pos_z_cm_catcher_view,
    df_join_batter.pitch_ball_move_x_cm_catcher_view,
    df_join_batter.pitch_ball_move_z_cm_catcher_view,
    df_join_batter.pitch_ball_pos_x_cm_catcher_view,
    df_join_batter.pitch_ball_pos_z_cm_catcher_view,
    df_join_batter.strike_zone_top_cm,
    df_join_batter.strike_zone_bot_cm,
    df_join_batter.swing_length_cm,
    df_join_batter.hit_field_coord_x_m,
    df_join_batter.hit_field_coord_y_m,
    df_join_batter.pitch_ball_vx_ms,
    df_join_batter.pitch_ball_vy_ms,
    df_join_batter.pitch_ball_vz_ms,
    df_join_batter.pitch_ball_ax_ms2,
    df_join_batter.pitch_ball_ay_ms2,
    df_join_batter.pitch_ball_az_ms2,
    df_join_batter.hit_dist_m,
    df_join_batter.launch_speed_angle,
    df_join_batter.pitch_zone_catcher_view,
    df_join_batter.batting_team,
    df_join_batter.fielding_team,
    df_join_batter.pitch_hand,
    df_join_batter.batter_side,
    df_join_batter.description_details,
    df_join_batter.pitch_result,
    df_join_batter.hit_ball_type,
    df_join_batter.spin_axis_degree,
    df_join_batter.pitch_arm_angle,
    df_join_batter.idx_game_pitch,
)

# COMMAND ----------

if spark.catalog.tableExists(enrich_table_name):
    target_table = DeltaTable.forName(spark, enrich_table_name)
    (target_table.alias("t")
     .merge(
         df_join_final.alias("s"),
         "t.game_pk = s.game_pk AND t.at_bat_number = s.at_bat_number AND t.pitch_number = s.pitch_number"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
else:
    (df_join_final.write
     .format("delta")
     .mode("overwrite")
     .partitionBy("date")
     .saveAsTable(enrich_table_name))
