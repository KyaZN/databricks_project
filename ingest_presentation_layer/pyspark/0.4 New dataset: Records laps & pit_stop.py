# Databricks notebook source
# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

lap_times_df = spark.read.format("delta").load(f"{processed_folder_path}/lap_times")

# COMMAND ----------

pit_stop_df = spark.read.format("delta").load(f"{processed_folder_path}/pit_stops")

# COMMAND ----------


