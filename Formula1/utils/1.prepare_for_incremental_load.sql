-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE f1_processed
LOCATION '/mnt/formula1dldatabrickstut/processed'

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION  '/mnt/formula1dldatabrickstut/presentation'

-- COMMAND ----------

show databases
