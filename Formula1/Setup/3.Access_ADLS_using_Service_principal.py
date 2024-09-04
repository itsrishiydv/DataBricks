# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Service Principal 
# MAGIC 1. Register the Azure AD Application/ Service Principal
# MAGIC 2. Generate the Secret/ Password for the application 
# MAGIC 3. Set Spark config with App/Client id,Directory / TenentID and Secret
# MAGIC 4. Assign Role 'Storage Blob data Contributor' to the Data Lake

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope",key="formula1-app-clientid")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secrets")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dldatabrickstut.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dldatabrickstut.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dldatabrickstut.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dldatabrickstut.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dldatabrickstut.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dldatabrickstut.dfs.core.windows.net/002 circuits.csv", header=True))

# COMMAND ----------


