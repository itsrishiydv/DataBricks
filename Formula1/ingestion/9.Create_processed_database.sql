-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION  "/mnt/formula1dldatabrickstut/processed"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dldatabrickstut/processed"
