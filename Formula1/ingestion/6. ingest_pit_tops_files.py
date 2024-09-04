# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest Pit_Stops.json File

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Run Another Notebook to import path and Functions to reuse the code
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

## Take Input for this note book
dbutils.widgets.text("Data Source","")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1: Read data from pitsstops file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType

# COMMAND ----------

pit_stops_schema = StructType([
  StructField("raceId", IntegerType(), False),
  StructField("driverId",IntegerType(), True),
  StructField("stop", IntegerType(),  True),
  StructField("lap", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("duration", StringType(), True),
  StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read\
.option("multiLine", True)\
.schema(pit_stops_schema).json("/mnt/formula1dldatabrickstut/raw/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-2: Transform the columns and Add new Columns 

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, udf

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
                                 .withColumnRenamed("driverId", "driver_id")
                                 

# COMMAND ----------

pit_stops_final_df = ingest_date(pit_stops_final_df)\
                     .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3: Write the processed data into parquet

# COMMAND ----------

# pit_stops_final_df.write.mode("overwrite").parquet("/mnt/formula1dldatabrickstut/processed/pit_stops")

# COMMAND ----------

pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops

# COMMAND ----------

pit_stops_parquet_df = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/pit_stops")

# COMMAND ----------

display(pit_stops_parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
