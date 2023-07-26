# Databricks notebook source
# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col,lit, concat_ws, collect_list, array_distinct, array_sort, count_distinct

participation_per_nationality_df = race_results_df\
    .groupBy("driver_nationality")\
    .agg(
        count_distinct(col("race_year")).alias("q_years"),
        count(when(col("position") == lit(1), True)).alias("wins"),
        concat_ws(", ",  array_sort(array_distinct(collect_list(col("race_year"))))).alias("detail_years_of_participation")
        )

participation_per_nationality_df.display()


# COMMAND ----------

# Buscar reactivar la participacion en deportistas de nacionalidades que actualmente han tenido una baja participacion
# Conocer que no siempre por mayor participacion se ganara mas veces

# COMMAND ----------

participation_per_nationality_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.participation_per_nationality")
