-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Objective
-- MAGIC 1. Create database demo
-- MAGIC 2. Data Tab in the UI
-- MAGIC 3. Show Command
-- MAGIC 4. Describe Command
-- MAGIC 5. find the current database

-- COMMAND ----------

CREATE DATABASE demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESC DATABASE demo

-- COMMAND ----------

DESC DATABASE EXTENDED demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

USE DEMO

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Managed Table Objective
-- MAGIC 1. Create managed table using python
-- MAGIC 2. Create amanaged table using sql
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe Table  

-- COMMAND ----------

-- MAGIC %run "../includes/folder_path_config"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.mode("overwrite").saveAsTable("race_result_python")

-- COMMAND ----------

select * from race_result_python where race_year = '2020'

-- COMMAND ----------

desc TABLE extended race_result_python

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS race_result_sql
AS
SELECT * FROM race_result_python 

-- COMMAND ----------

SELECT * FROM race_result_Sql

-- COMMAND ----------

DESC TABLE EXTENDED race_result_sql

-- COMMAND ----------

DROP TABLE IF EXISTS race_result_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Table Objective
-- MAGIC 1. Create external table using python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping table
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.format("parquet").option("path",f"{demo_folder_path}/race_result_ext_py").mode("overwrite").saveAsTable("race_result_ext_py")

-- COMMAND ----------

desc table extended race_result_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_result_ext_sql
(
race_year	int,
race_name	string,
race_date	timestamp,
circuit_location	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team	string,
grid	int,
fastest_lap	int,
race_time	string,
points	double,
position	int,
created_date	timestamp
)
USING PARQUET
LOCATION '/mnt/formula1dldatabrickstut/demo/race_result_ext_sql' 

-- COMMAND ----------

INSERT INTO race_result_ext_sql
SELECT * FROM race_result_ext_py WHERE race_year=2020

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create View On table
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_result
AS
SELECT * FROM race_result_ext_py
where race_year = 2020

-- COMMAND ----------

select * from v_race_result

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_result
AS
SELECT * FROM race_result_ext_py
where race_year=2019

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

select * from global_temp.gv_race_result

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_result
AS
SELECT * FROM demo.race_result_ext_py
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM pv_race_result

-- COMMAND ----------


