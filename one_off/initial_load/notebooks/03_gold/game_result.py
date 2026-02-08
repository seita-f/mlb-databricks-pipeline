# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.read.table("mlb.02_silver.statcast_enrich")

# COMMAND ----------

# df.display()

# COMMAND ----------

fastball_types = ['4-Seam Fastball', 'Sinker', 'Cutter']

breaking_types = [
    'Slider', 'Sweeper', 'Curveball', 'Knuckle Curve', 
    'Slurve', 'Slow Curve', 'Knuckleball'
]

offspeed_types = ['Changeup', 'Split-Finger', 'Forkball', 'Screwball']

ignore_types = ['Pitch Out', 'Other', 'Unknown', None]

stats_groups = {
    "fastball": fastball_types,
    "breaking": breaking_types,
    "offspeed": offspeed_types
}

# COMMAND ----------

game_base = df.groupBy("game_pk").agg(
    F.first(F.to_date("date")).alias("date"),
    F.first("game_type").alias("game_type"),
    F.first("home_team").alias("home_team"),
    F.first("away_team").alias("away_team"),
    F.max("home_score").alias("home_score"),
    F.max("away_score").alias("away_score")
).withColumn(
    "winner",
    F.when(F.col("home_score") > F.col("away_score"), F.col("home_team"))
     .when(F.col("away_score") > F.col("home_score"), F.col("away_team"))
     .otherwise("Tie")
).withColumn(
    "loser",
    F.when(F.col("home_score") < F.col("away_score"), F.col("home_team"))
     .when(F.col("away_score") < F.col("home_score"), F.col("away_team"))
     .otherwise("Tie")
) 

# COMMAND ----------

team_game_stats = df.filter(F.col("pitcher_release_speed_kmh").isNotNull()). \
    groupBy("game_pk", "fielding_team").  \
    agg(
        # Fielding stats
        F.count("pitch_number").alias("pitch_count"),
        F.round(F.avg(F.col("pitcher_release_speed_kmh")), 2).alias("pitch_avg_speed_kmh"),
        F.count(F.when(F.col("events") == "strikeout", 1)).alias("field_strikeout_count"),
        F.count(F.when(F.col("events") == "walk", 1)).alias("walk_allowed_count"),
        F.count(F.when(F.col("events") == "hit_by_pitch", 1)).alias("field_hit_by_pitch_count"),
        *[F.count(F.when(F.col("pitch_name").isin(v), 1)).alias(f"n_{k}") for k, v in stats_groups.items()],
        F.count(F.when(F.col("events").contains("fielders_choice"), 1)).alias("fielders_choice_count"),

        # Batting stats (opponent team)
        F.count(F.when(F.col("events") == "single", 1)).alias("hit_single"),
        F.count(F.when(F.col("events") == "double", 1)).alias("hit_double"),
        F.count(F.when(F.col("events") == "double", 1)).alias("hit_triple"),
        F.count(F.when(F.col("events") == "home_run", 1)).alias("hit_homerun"),
        F.round(F.avg("launch_speed_kmh"), 2).alias("hit_avg_launch_speed"),
        F.round(F.avg("bat_speed_kmh"), 2).alias("hit_avg_bat_speed"),
)

# COMMAND ----------

# Home team (from infield stats)
df_merged = game_base.join(
    team_game_stats.alias("h_stats"),
    (game_base.game_pk == F.col("h_stats.game_pk")) & (game_base.home_team == F.col("h_stats.fielding_team")),
    "left"
).select(
    game_base["*"],

    # home fields stats
    F.col("h_stats.pitch_avg_speed_kmh").alias("home_pitch_avg_speed"),
    F.col("h_stats.field_strikeout_count").alias("home_pitch_k"),
    F.col("h_stats.pitch_count").alias("home_pitch_count"),
    F.col("h_stats.n_fastball").alias("home_pitch_fastball"),
    F.col("h_stats.n_breaking").alias("home_pitch_breaking"),
    F.col("h_stats.n_offspeed").alias("home_pitch_offspeed"),
    
    # away batting stats (occurs when home team fields)
    F.col("h_stats.hit_homerun").alias("away_hit_homerun"),
    F.col("h_stats.hit_avg_launch_speed").alias("away_hit_avg_launch_speed"),
    F.col("h_stats.hit_avg_bat_speed").alias("away_hit_avg_bat_speed")
)

# COMMAND ----------

# Away team (from infield stats)
df_final = df_merged.join(
    team_game_stats.alias("a_stats"),
    (df_merged.game_pk == F.col("a_stats.game_pk")) & (df_merged.away_team == F.col("a_stats.fielding_team")),
    "left"
).select(
    df_merged["*"],

    # away fileds stats
    F.col("a_stats.pitch_avg_speed_kmh").alias("away_pitch_avg_speed"),
    F.col("a_stats.field_strikeout_count").alias("away_pitch_k"),
    F.col("a_stats.pitch_count").alias("away_pitch_count"),
    F.col("a_stats.n_fastball").alias("away_pitch_fastball"),
    F.col("a_stats.n_breaking").alias("away_pitch_breaking"),
    F.col("a_stats.n_offspeed").alias("away_pitch_offspeed"),

    # home hitting stats（occurs when away team fileds）
    F.col("a_stats.hit_homerun").alias("home_hit_homerun"),
    F.col("a_stats.hit_avg_launch_speed").alias("home_hit_avg_launch_speed"),
    F.col("a_stats.hit_avg_bat_speed").alias("home_hit_avg_bat_speed")
)

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("mlb.03_gold.game_summary_stats")
