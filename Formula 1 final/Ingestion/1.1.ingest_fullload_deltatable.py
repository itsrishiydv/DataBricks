# Databricks notebook source
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DecimalType , DateType

# COMMAND ----------

from pyspark.sql.functions import col, concat

# COMMAND ----------

# MAGIC %run "../includes/path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_func"

# COMMAND ----------

# Data Source Name
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# Source Folder Name
dbutils.widgets.text("data_source_folder","")
v_data_source_folder = dbutils.widgets.get("data_source_folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Ingest Circuit File

# COMMAND ----------

circuit_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DecimalType(), True),
    StructField("lng", DecimalType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),

])

# COMMAND ----------

# Ingesting Circuit File
df_circuit = spark.read.format("csv").schema(circuit_schema).option("header", True).load(f"{raw_path}/{v_data_source_folder}/circuits.csv")

# COMMAND ----------

final_circuit_df = df_circuit.withColumnRenamed("circuitId","circuit_id")\
                            .withColumnRenamed("circuitRef","circuit_ref")

# COMMAND ----------

final_circuit_df = add_col(final_circuit_df,v_data_source)\
                  .withColumn("file_date",lit(v_data_source_folder))

# COMMAND ----------

final_circuit_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Ingest Races File
# MAGIC

# COMMAND ----------

race_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), False),
    StructField("round", IntegerType(), False),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("date", DateType(), False),
    StructField("time", StringType(), False),
    StructField("url", StringType(), False)
])

# COMMAND ----------

race_df = spark.read.schema(race_schema).option("header", True).csv(f"{raw_path}/{v_data_source_folder}/races.csv")

# COMMAND ----------

race_renamed_df = race_df.withColumnRenamed("raceId","race_id")\
                                      .withColumnRenamed("year","race_year")\
                                      .withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

#race_final_df = race_renamed_df.withColumn("Ingest_Date", current_timestamp()) 
race_final_df = add_col(race_renamed_df,v_data_source)\
                .withColumn("file_date",lit(v_data_source_folder))

# COMMAND ----------

race_final_df.write.format("delta").mode("overwrite").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.Ingest Constructor Files

# COMMAND ----------

constructors_schema = StructType([
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

constructors_df = spark.read.option("header",True).schema(constructors_schema).json(f"{raw_path}/{v_data_source_folder}/constructors.json")

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col("url"))
constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                                .withColumnRenamed("constructorRef", "constructor_ref")
                                                

# COMMAND ----------

constructors_final_df = add_col(constructors_renamed_df,v_data_source)\
                        .withColumn("file_date", lit(v_data_source_folder))

# COMMAND ----------

constructors_final_df.write.format("parquet").mode("overwrite").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Ingest Driver Files

# COMMAND ----------

## Schema for the Nasted Column 
name_schema = StructType([
    StructField("forename", StringType(), True),    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType([
    StructField("code", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.option("header",True).schema(drivers_schema).json(f"{raw_path}/{v_data_source_folder}/drivers.json")


# COMMAND ----------

drivers_final_df = add_col(drivers_df, v_data_source)\
                                .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("file_date", lit(v_data_source_folder))\
                                .drop(col("url"))

# COMMAND ----------

drivers_final_df.write.format("delta").mode("overwrite").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct file_date from f1_processed.drivers
