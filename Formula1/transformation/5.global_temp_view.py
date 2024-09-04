# Databricks notebook source
# MAGIC %md
# MAGIC ##### Global temp view
# MAGIC 1. Create global temp view on dataframe
# MAGIC 2. Access the view from the sql cell
# MAGIC 3. Access the view from python cell
# MAGIC 3. Access the view from another notebook

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createOrReplaceGlobalTempView("gv_race_result_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_result_df where race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

final_df = spark.sql(f"SELECT * FROM global_temp.gv_race_result_df where race_year = {p_race_year}")
display(final_df)

# COMMAND ----------


