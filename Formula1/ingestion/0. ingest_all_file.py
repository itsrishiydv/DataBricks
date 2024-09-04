# Databricks notebook source
v_results = dbutils.notebook.run("1. ingest_circuits_file",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("2. ingest_race_file",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("3. ingest_constructors_file",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("4. ingest_driver_files",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("5. ingest_Results_File",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("6. ingest_pit_tops_files",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("7. ingest_lap_times_file",0,{"Data Source":"Ergast API"})
display(v_results)

# COMMAND ----------

v_results = dbutils.notebook.run("8. ingest_qualifying_file",0,{"Data Source":"Ergast API"})
display(v_results)
