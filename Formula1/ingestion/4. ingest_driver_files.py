# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step-1: Read json from spark DataFrame reader **API**

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("Data Source", "")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

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

drivers_df = spark.read.option("header",True).schema(drivers_schema).json(f"{raw_folder_path}/drivers.json")
display(drivers_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1. Add column name from nasted column "forename" and "surname 
# MAGIC ##### 2. rename the required column
# MAGIC ##### 3. drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp

# COMMAND ----------

drivers_final_df = drivers_df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                                .withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("data_source", lit(v_data_source))\
                                .drop(col("url"))
display(drivers_final_df)

# COMMAND ----------

drivers_final_df = ingest_date(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Write the processed file into parquet

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

drivers_parquet_df = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/drivers")
display(drivers_parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
