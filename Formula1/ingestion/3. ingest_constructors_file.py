# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step-1: Read constructors.json

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("Data Source","")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

# COMMAND ----------

constructors_schema = StructType([
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

## AnotherWay To Define Schema

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.option("header",True).schema(constructors_schema).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step-2: Add and drop the unwanted Column

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------



constructors_dropped_df = constructors_df.drop(col("url"))
display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-4: Rename and Add Columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
                                                .withColumnRenamed("constructorRef", "constructor_ref")
                                                

# COMMAND ----------

constructors_final_df = ingest_date(constructors_renamed_df)\
                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step-5: Write the processed data to parquet file

# COMMAND ----------

# constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_processed.constructors

# COMMAND ----------

constructors_parquet_df = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/constructors")
display(constructors_parquet_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
