# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Cluster Scoped Credentials
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List file from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net/002 circuits.csv", header=True).show()

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net/002 circuits.csv", header=True))
