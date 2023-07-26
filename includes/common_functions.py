# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# def overwrite_partition(input_df, db_name, table_name, partition_column):
#   output_df = re_arrange_partition_column(input_df, partition_column)
#   spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#   if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#     output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#   else:
#     output_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

import os

def read_type_files_in_folder(folder_path, schema, f_type):    
    file_paths = [os.path.join(folder_path, file[1]) for file in dbutils.fs.ls(folder_path)]
    dataframes = []
    for file_path in file_paths:
        try:
            if f_type == 'csv':
                if os.path.splitext(file_path)[1].lower() == f".{f_type}":
                    df = spark.read \
                        .schema(schema) \
                        .csv(file_path)
                    
                    dataframes.append(df)
                else:
                    print(f"Skipping file {file_path}. Not a CSV file.")
            
            if f_type == 'json':
                if os.path.splitext(file_path)[1].lower() == f".{f_type}":
                    df = spark.read \
                        .schema(schema) \
                        .option("multiLine", True)\
                        .json(file_path)
                    
                    dataframes.append(df)
                else:
                    print(f"Skipping file {file_path}. Not a JSON file.")

        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
    
    return dataframes

