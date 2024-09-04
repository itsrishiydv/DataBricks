# Databricks notebook source
# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

display(df.filter("circuit_id = 1 and country = 'Australia'"))

# COMMAND ----------

display(df.filter((df.circuit_id==1) & (df.country == 'Australia')))

# COMMAND ----------


