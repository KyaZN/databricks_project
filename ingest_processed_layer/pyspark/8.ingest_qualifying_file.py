# Databricks notebook source
# MAGIC %md ####Ingesta los archivos json qualifying

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

# MAGIC %md **Paso 1 - Leer los archivos JSON usando Spark Reader API**

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[
                                        StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                      ]
                               )

# COMMAND ----------

list_qualifying_data = read_type_files_in_folder(f"{raw_folder_path}/{v_file_date}/qualifying",qualifying_schema, 'json')

# COMMAND ----------

# MAGIC %md **Paso 2 - Renombrar y agregar columnas**
# MAGIC 1. Renombrar qualifyingId, driverId, constructorId y raceId
# MAGIC 1. Añadir campo ingestion_date con current timestamp

# COMMAND ----------

list_qualifying_ingestion_date_df = []
for q in list_qualifying_ingestion_date_df:
    qualifying_ingestion_date_df = add_ingestion_date(q)
    list_qualifying_ingestion_date_df.append(qualifying_ingestion_date_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

list_final_df = []

for  final_q in list_qualifying_ingestion_date_df:
    final_df = final_q.withColumnRenamed("qualifyId","qualify_id") \
                        .withColumnRenamed("driverId","driver_id") \
                        .withColumnRenamed("raceId","race_id") \
                        .withColumnRenamed("constructorId","constructor_id") \
                        .withColumn("data_source", lit(v_data_source))
    list_final_df.append(final_df)



# COMMAND ----------

# MAGIC %md **Paso 3 - Escribir el resultado en un archivo Parquet**

# COMMAND ----------

for save_final_df in list_final_df:
    save_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
