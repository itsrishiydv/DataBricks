# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Access Kay
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List file from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope="formula1-scope",key="formula1-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dldatabrickstut.dfs.core.windows.net",
               formula1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net/002 circuits.csv", header=True))

# COMMAND ----------


