# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake container for the project 

# COMMAND ----------

def mount_adls_container(storage_account_name, container_name):
    # Get Secrets from key Vault
    client_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-clientid")
    tenant_id = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-tenant-id")
    client_secret = dbutils.secrets.get(scope="formula1-scope",key="formula1-app-client-secrets")

    # Define configuration to mount ADLS
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount if already mounted
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        #dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
        return print("Already mounted")

    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts( ))


# COMMAND ----------

mount_adls_container("formula1dldatabrickstut", "processed")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dldatabrickstut/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dldatabrickstut/demo/002 circuits.csv", header=True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dldatabrickstut/demo")

# COMMAND ----------

if [mount.mountPoint == "/mnt/formula1dldatabrickstut/raw" for mount in dbutils.fs.mounts()]:
    dbutils.fs.unmount("/mnt/formula1dldatabrickstut/raw")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


