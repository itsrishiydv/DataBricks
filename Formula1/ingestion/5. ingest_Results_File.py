# Databricks notebook source
# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# To Take Input Parameters
dbutils.widgets.text("Data Source", "")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, LongType

# COMMAND ----------

results_schema = StructType([
    StructField("constructorId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("fastestLapSpeed", DoubleType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("points", DoubleType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("resultId", IntegerType(), True),
    StructField("statusId", StringType(), True),
    StructField("time", StringType(), True)
])


# COMMAND ----------

results_df = spark.read.option("header ",True).schema(results_schema).json(f"{raw_folder_path}/results.json")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename The column And Add the required column

# COMMAND ----------

from pyspark.sql.functions import current_date,current_timestamp, col, lit

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id")\
                             .withColumnRenamed("raceId", "race_id")\
                             .withColumnRenamed("driverId", "driver_id")\
                             .withColumnRenamed("constructorId", "constructor_id")\
                             .withColumnRenamed("positionText", "position_text")\
                             .withColumnRenamed("positionOrder", "position_order")\
                             .withColumnRenamed("fastestLap", "fastest_lap")\
                             .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                             .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                             .drop(col("statusId"))

# COMMAND ----------

results_final_df  = ingest_date(results_final_df)\
                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Write transformed data to parquet

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1dldatabrickstut/processed/results")

# COMMAND ----------

results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

results_parquet_df = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/results")
display(results_parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
