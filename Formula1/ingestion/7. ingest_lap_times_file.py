# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting multiple files from lap_times folder

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("Data Source","")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType

# COMMAND ----------

lap_times_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap",IntegerType(), False),
    StructField("position", IntegerType(), False),
    StructField("duration", StringType(), True),
    StructField("millisecond", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read\
.option("header", True)\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/lap_times/lap_times_split_*.csv")

# COMMAND ----------

display(lap_times_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reaming the columns and adding ingestion_date into df 

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumn("ingestion_date",current_timestamp())\
                                 .withColumnRenamed("raceId","race_id")\
                                 .withColumnRenamed("driverId","driver_id")

# COMMAND ----------

lap_times_final_df = ingest_date(lap_times_final_df)\
                     .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write proceed data into parquet file

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

lap_times_parquet_df = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/lap_times")
display(lap_times_parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
