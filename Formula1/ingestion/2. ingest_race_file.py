# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Race File

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# Used to take input at run time
dbutils.widgets.text("Data Source","")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1: Read race csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, DateType, StringType, TimestampType, TimestampNTZType

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

race_df = spark.read\
.schema(race_schema)\
.option("header", "true")\
.csv(f"{raw_folder_path}/races.csv")
display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-2: Select Only Required Column

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, concat

# COMMAND ----------

race_selected_col_df = race_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),
                                      to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss").alias("race_timestamp"))

# COMMAND ----------

display(race_selected_col_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-3 Rename the required column 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

race_renamed_df = race_selected_col_df.withColumnRenamed("raceId","race_id")\
                                      .withColumnRenamed("year","race_year")\
                                      .withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

display(race_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-4  Add the required column 

# COMMAND ----------

#race_final_df = race_renamed_df.withColumn("Ingest_Date", current_timestamp()) 
race_final_df = ingest_date(race_renamed_df)\
                .withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(race_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-5 Write Processed Data to Parquet

# COMMAND ----------

# race_final_df.write.partitionBy("race_year").parquet(f"{presentation_folder_path}/race", mode="overwrite")

# COMMAND ----------

race_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

race_parquet_df = spark.read.parquet(f"{processed_folder_path}/races")
display(race_parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
