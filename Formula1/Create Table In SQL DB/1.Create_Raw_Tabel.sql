-- Databricks notebook source
-- MAGIC %run "../includes/folder_path_config"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 1. Ingest circuits data

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

CREATE DATABASE f1_raw

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS circuits
(
circuitId Integer,
circuitRef String,
name String,
location String,
country String,   
lat Decimal,
lng Decimal,
alt Integer, 
url String
)
USING csv
OPTIONs (path "/mnt/formula1dldatabrickstut/raw/circuits.csv", header True)

-- COMMAND ----------

select * from circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 2. Ingest races data 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId    Integer,
  year      Integer,
  round     Integer,
  circuitId Integer,
  name      String,
  date      Date,
  time      String,
  url       String
)
USING CSV
OPTIONS (path "/mnt/formula1dldatabrickstut/raw/races.csv", header True)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 3. Ingesting constructors.json data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
  constructorId     Integer,
  constructorRef    String,
  name              String,
  nationality       String,
  url               String
)
USING JSON
OPTIONS (PATH "/mnt/formula1dldatabrickstut/raw/constructors.json", header True)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 4. Ingesting drivers.json data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  code        String,
  dob         Date,
  driverId    Integer,
  driverRef   String,
  name        STRUCT<forename: STRING, surname: STRING>,
  nationality String,
  number      Integer,
  url         String
)
USING JSON
OPTIONS (PATH "/mnt/formula1dldatabrickstut/raw/drivers.json",header True)

-- COMMAND ----------

drop table f1_raw.drivers

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

use f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 5. Ingesting results.json **data**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results
(
  constructorId      Integer,
  driverId           Integer,
  fastestLap         Integer,
  fastestLapSpeed    Double,
  fastestLapTime     String,
  grid               Integer,
  laps               Integer,
  milliseconds       Integer,
  number             Integer,
  points             Double,
  position           Integer,
  positionOrder      Integer,
  positionText       String,
  raceId             Integer,
  rank               Integer,
  resultId           Integer,
  statusId           String,
  time               String
)
USING JSON
OPTIONS (PATH "/mnt/formula1dldatabrickstut/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 6. Ingesting pit_stops.json **data**

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId          Integer,
  driverId        Integer,
  stop            Integer,
  lap             Integer,
  time            String,
  duration        String,
  milliseconds    Integer
)
USING JSON
OPTIONS (PATH "/mnt/formula1dldatabrickstut/raw/pit_stops.json",multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 7. Ingesting multiple lap_times.csv **data** 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId        Integer,
driverId      Integer,
lap           Integer,
position      Integer,
duration      String,
millisecond   Integer
)
USING CSV
OPTIONS (PATH "/mnt/formula1dldatabrickstut/raw/lap_times/*.csv")

-- COMMAND ----------

select count(*) from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 8. Ingesting multiple qualifying.csv **data** 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE f1_raw.qualifying (
constructorId     Integer,
driverId          Integer,
number            Integer,
position          Integer,
q1                String,    
q2                String,
q3                String,
qualifyId         Integer,
raceId            Integer
)
USING JSON
OPTIONS (PATH "/mnt/formula1dldatabrickstut/raw/qualifying/*.json",multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying
