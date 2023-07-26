# Databricks notebook source
# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

from pyspark.sql.functions import date_format, filter

race_circuits_df = races_df.join(
                                circuits_df,
                                (races_df.circuit_id == circuits_df.circuit_id) ,
                                "inner")\
                            .select(
                                races_df.race_id,
                                races_df.race_year,
                                date_format(races_df.race_date, "MM").alias("race_month"),
                                circuits_df.race_country
                            ).filter(col("race_year") >= 2000)
race_circuits_df.display()

# COMMAND ----------

# Get list of seasons & races
from pyspark.sql.functions import date_format, filter, concat_ws, explode, sum, col, count

race_circuits_seasons_df = races_df.join(
                                circuits_df,
                                (races_df.circuit_id == circuits_df.circuit_id) ,
                                "inner")\
                            .select(
                                races_df.race_id,
                                concat_ws(' - ', races_df.race_year,date_format(races_df.race_date, "MM")).alias("year_month"),
                                circuits_df.race_country
                            ).filter(col("race_year") >= 2000)
# race_circuits_seasons_df.display()

sum_up_df = race_circuits_seasons_df\
    .groupBy(col("year_month"), col("race_country"))\
    .agg(
        count(col("race_id")).alias("q_races"),
        )\
    .orderBy(desc("year_month"))

sum_up_df.display()



# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, count_distinct, desc, count, array_sort, array_distinct, collect_list
from pyspark.sql.window  import Window

monthly_races_per_country_seasons_no_sort_df = race_circuits_df\
    .groupBy(col("race_country"))\
    .agg(
        count(col("race_id")).alias("q_races"),
        concat_ws(", ",  array_sort(array_distinct(collect_list(concat_ws(' - ', col("race_year"), col("race_month")))))).alias("seasons")
        )

monthly_races_per_country_season_df = monthly_races_per_country_seasons_no_sort_df.orderBy(desc("q_races"))

monthly_races_per_country_season_df.display()

# COMMAND ----------

# Races per country in 2000s
# Frecuencia por año y mes  puede depender de la temporada / meses
# Reordenar las temporadas y lugares en donde se realizan las actividades

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.driver_standings")
