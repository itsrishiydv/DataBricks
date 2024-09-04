# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using SAS Token
# MAGIC 1. Set the spark config SAS Token
# MAGIC 2. List file from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formula1_SAS_Token = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-SAS-Token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dldatabrickstut.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dldatabrickstut.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dldatabrickstut.dfs.core.windows.net", formula1_SAS_Token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net/002 circuits.csv", header=True))

# COMMAND ----------


