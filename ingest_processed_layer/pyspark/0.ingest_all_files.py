# Databricks notebook source
# Parameters
isource = "Ergast API"
idates = ["2021-03-21", "2021-03-28", "2021-04-18"]

# COMMAND ----------

#dbutils.notebook.help()
for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("1.ingest_circuits_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 1 : Loop {index} result -> {v_result}")


# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("2.ingest_races_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 2 : Loop {index} result -> {v_result}")

# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("3.ingesting_constructors_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 3 : Loop {index} result -> {v_result}")

# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("4.ingest_drivers_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 4 : Loop {index} result -> {v_result}")

# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 5 : Loop {index} result -> {v_result}")

# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("6.ingest_pit_stops_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 6 : Loop {index} result -> {v_result}")

# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("7.ingest_lap_times_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 7 : Loop {index} result -> {v_result}")

# COMMAND ----------

for index, idate in enumerate(idates):
    v_result = dbutils.notebook.run("8.ingest_qualifying_file",0,{"p_data_source": isource, "p_file_date" : idate})
    print(f"Notebook 8 : Loop {index} result -> {v_result}")
