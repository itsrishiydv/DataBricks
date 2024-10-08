# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE f1_processed
# MAGIC LOCATION '/mnt/formula1dldatabrickstut/processed'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE f1_presentation
# MAGIC LOCATION '/mnt/formula1dldatabrickstut/presentation'
