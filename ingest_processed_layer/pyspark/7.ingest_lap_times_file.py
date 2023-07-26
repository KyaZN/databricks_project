# Databricks notebook source
# MAGIC %md ####Ingestar los archivos lap_times

# COMMAND ----------

# dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "./../../includes/configuration"

# COMMAND ----------

# MAGIC %run "./../../includes/common_functions"

# COMMAND ----------

# MAGIC %md **Paso 1 - Leer archivo CSV usando Spark DataFrame Reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
                                        StructField("raceId",IntegerType(), False),
                                        StructField("driverId",IntegerType(), True),
                                        StructField("lap",IntegerType(), True),
                                        StructField("position",IntegerType(), True),
                                        StructField("time",StringType(), True),
                                        StructField("milliseconds",IntegerType(), True)
])

# COMMAND ----------

list_lap_time_data = read_type_files_in_folder(f"{raw_folder_path}/{v_file_date}/lap_times",lap_times_schema, 'csv')

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar nuevas columnas**
# MAGIC 1. Renombrar driverId y raceId
# MAGIC 1. Agregar el campo ingestion_date y current timestamp

# COMMAND ----------

list_lap_times_ingestion_date_df = []
for lap in list_lap_time_data:
    lap_times_ingestion_date_df = add_ingestion_date(lap)
    list_lap_times_ingestion_date_df.append(lap_times_ingestion_date_df)


# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

list_final_df = []

for  final_lap in list_lap_times_ingestion_date_df:
    final_df = final_lap.withColumnRenamed("driverId","driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("data_source", lit(v_data_source))
    list_final_df.append(final_df)


# COMMAND ----------

# MAGIC %md **Paso 3 - Escribir el resultado en un archivo Parquet**

# COMMAND ----------

for save_final_df in list_final_df:
    save_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
