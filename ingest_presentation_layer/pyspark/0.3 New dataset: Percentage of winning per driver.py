# Databricks notebook source
# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col("race_year") >= 2000)


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col,lit, concat_ws, collect_list, array_distinct, array_sort, count_distinct, coalesce, round

participation_per_driver_df = race_results_df\
    .groupBy("driver_name")\
    .agg(
        count_distinct(col("race_year")).alias("q_years"),
        count(when(col("position") == lit(1), True)).alias("wins"),
        concat_ws(", ",  array_sort(array_distinct(collect_list(col("race_year"))))).alias("list_years_of_participation")
        )\
    # .orderBy(count_distinct(col("race_year")).desc())
    
# Calculate probability

probability_per_driver_df = participation_per_driver_df\
    .withColumn("%_win", round(coalesce((col("wins")/col("q_years")) * 100, lit(0)),0)) \
    
all_details_ppd_df = probability_per_driver_df.select("driver_name", "q_years", "wins", "%_win", "list_years_of_participation")

probability_per_driver_sorted_df = all_details_ppd_df.orderBy(col("%_win").desc())
probability_per_driver_sorted_df.display()

# COMMAND ----------

# Validar el mito de si mas antiguedad tiene, mas probabilidades de ganar

# COMMAND ----------

probability_per_driver_sorted_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.driver_standings")
