# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting multiple files from qualifying folder

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("Data Source","")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DataType

# COMMAND ----------

qualifying_schema = StructType([
    StructField("constructorId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),    
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
    StructField("qualifyId", IntegerType(), True),
    StructField("raceId", IntegerType(), True) 

])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/qualifying/qualifying_split_*.json")
display(qualifying_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### rename the columns  and add the ingestion date

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumn("ingestion_date", current_timestamp())\
                                    .withColumnRenamed("qualifyId","qualifying_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

qualifying_final_df = ingest_date(qualifying_final_df)\
                      .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write processed data into parquet file

# COMMAND ----------

# qualifying_final_df.write.mode("overwrite").parquet("{processed_folder_path}/qualifying")

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying

# COMMAND ----------

parquet_df = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/qualifying")

# COMMAND ----------

display(parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
