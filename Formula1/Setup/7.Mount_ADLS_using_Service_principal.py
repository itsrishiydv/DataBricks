# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Using Service Principal 
# MAGIC 1. Get the client_id,tenant_is and client_secret from key vault.
# MAGIC 2. set spark config with App/Client_id, Directory /Tenant_id and Client_secrets
# MAGIC 3. call the file sysytem utility mount to mount the storage 
# MAGIC 4. Explore the other file system utilities to mount(list all mounts, unmount)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientid")
tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secrets")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formula1dldatabrickstut.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dldatabrickstut/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dldatabrickstut/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dldatabrickstut/demo/002 circuits.csv", header=True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dldatabrickstut/demo")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


