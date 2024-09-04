# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore DBFS root
# MAGIC 1. list all the folders in the DBFS root
# MAGIC 2. interect with DBFS file Browser
# MAGIC 3. Upload file to DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/002_circuits.csv',header=True))

# COMMAND ----------


