# Databricks notebook source
# MAGIC %md
# MAGIC ## Access DataFrame Using SQL
# MAGIC #### Objective
# MAGIC 1. Create temp view On DataFrame
# MAGIC 2. Access The view from sql cell
# MAGIC 3. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_result_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results where race_year = 2019

# COMMAND ----------

dbutils.widgets.text("p_race_year","")
p_race_year = dbutils.widgets.get("p_race_year")

# COMMAND ----------

race_result_2019_df = spark.sql(f"SELECT * FROM v_race_results where race_year = {p_race_year}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Accessing Global Temp View
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from global_temp.gv_race_result_df

# COMMAND ----------

gv_tmp = spark.sql("select * from global_temp.gv_race_result_df")

# COMMAND ----------

display(gv_tmp)

# COMMAND ----------


