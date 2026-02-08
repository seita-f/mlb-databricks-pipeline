# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df_game_stat = spark.read.table("mlb.03_gold.game_summary_stats")

# COMMAND ----------

df_game_stat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pitch Type Distribution by Team

# COMMAND ----------

home_p_df = df_game_stat.select(
    F.col("home_team").alias("team"),
    F.col("home_pitch_fastball").alias("fastball"),
    F.col("home_pitch_breaking").alias("breaking"),
    F.col("home_pitch_offspeed").alias("offspeed"),
    F.col("home_pitch_count").alias("total")
)

away_p_df = df_game_stat.select(
    F.col("away_team").alias("team"),
    F.col("away_pitch_fastball").alias("fastball"),
    F.col("away_pitch_breaking").alias("breaking"),
    F.col("away_pitch_offspeed").alias("offspeed"),
    F.col("away_pitch_count").alias("total")
)

pitch_ratio_df = home_p_df.union(away_p_df) \
    .groupBy("team") \
    .agg(
        F.sum("fastball").alias("fastball"),
        F.sum("breaking").alias("breaking"),
        F.sum("offspeed").alias("offspeed"),
        F.sum("total").alias("total_count")
    ) \
    .withColumn("fastball_pct", F.round(F.col("fastball") / F.col("total_count") * 100, 2)) \
    .withColumn("breaking_pct", F.round(F.col("breaking") / F.col("total_count") * 100, 2)) \
    .withColumn("offspeed_pct", F.round(F.col("offspeed") / F.col("total_count") * 100, 2))

# COMMAND ----------

display(pitch_ratio_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correlation between Pitching Speed / Launching Speed and Win

# COMMAND ----------

winner_stats = df_game_stat.select(
    F.col("game_pk"),
    F.when(F.col("winner") == F.col("home_team"), F.col("home_pitch_avg_speed"))
     .otherwise(F.col("away_pitch_avg_speed")).alias("pitch_speed"),
    F.when(F.col("winner") == F.col("home_team"), F.col("home_hit_avg_launch_speed"))
     .otherwise(F.col("away_hit_avg_launch_speed")).alias("launch_speed"),
    F.lit("Winner").alias("result")
)

loser_stats = df_game_stat.select(
    F.col("game_pk"),
    F.when(F.col("loser") == F.col("home_team"), F.col("home_pitch_avg_speed"))
     .otherwise(F.col("away_pitch_avg_speed")).alias("pitch_speed"),
    F.when(F.col("loser") == F.col("home_team"), F.col("home_hit_avg_launch_speed"))
     .otherwise(F.col("away_hit_avg_launch_speed")).alias("launch_speed"),
    F.lit("Loser").alias("result")
)

correlation_df = winner_stats.union(loser_stats)

# COMMAND ----------

display(correlation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Home Field Advantage

# COMMAND ----------

summary_analysis = df_game_stat.select(
    "game_pk",
    "date",
    "home_team",
    (F.col("home_hit_avg_bat_speed") - F.col("away_hit_avg_bat_speed")).alias("bat_speed_diff"),
    (F.col("home_score") - F.col("away_score")).alias("score_diff"),
    F.when(F.col("winner") == F.col("home_team"), "Win").otherwise("Loss").alias("home_result")
)

# COMMAND ----------

display(summary_analysis)

# COMMAND ----------

from pyspark.sql import functions as F

# ホームチーム視点で、スイング速度が打球速度にどれだけ変換されたか（ミート効率）を計算
efficiency_df = df_game_stat.withColumn(
    "hit_efficiency", F.col("home_hit_avg_launch_speed") / F.col("home_hit_avg_bat_speed")
).withColumn(
    "is_win", F.when(F.col("winner") == F.col("home_team"), 1).otherwise(0)
)

# 勝利試合と敗北試合での「ミート効率」の平均を比較
efficiency_df.groupBy("is_win").agg(
    F.avg("hit_efficiency").alias("avg_efficiency"),
    F.avg("home_hit_avg_bat_speed").alias("avg_bat_speed")
).show()
