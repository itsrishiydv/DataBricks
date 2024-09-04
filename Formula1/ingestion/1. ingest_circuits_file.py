# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("Data Source", "")
v_data_source = dbutils.widgets.get("Data Source")

# COMMAND ----------

dbutils.widgets.text("Folder Path", "")
v_folder_path = dbutils.widgets.get("Folder Path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType


# COMMAND ----------

circuits_schema = StructType([
    StructField("circuitId", IntegerType(), True),
    StructField("circuitRef", StringType(), False),
    StructField("name", StringType(), False),
    StructField("location", StringType(), False),
    StructField("country", StringType(), False),    
    StructField("lat", DecimalType(), False),
    StructField("lng", DecimalType(), False),
    StructField("alt", IntegerType(), False),   
     StructField("url", StringType(), False)
])

# COMMAND ----------

circuits_df = spark.read\
.option("header", True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 2: Select Only the Required Column

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 4 way to select the column

# COMMAND ----------

# 1. 
circuits_selected_columns_df = circuits_df.select("circuitId","circuitRef","name","location", "country", "lat", "lng","alt")

# COMMAND ----------

# 2.
circuits_selected_columns_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng,circuits_df.alt)

# COMMAND ----------

# 3. 
circuits_selected_columns_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# 4.
circuits_selected_columns_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 3: Rename The Required Column 

# COMMAND ----------

circuits_rename_columns_df = circuits_selected_columns_df.withColumnRenamed("circuitId", "circuit_id")\
                                                        .withColumnRenamed("circuitRef", "circuit_ref")\
                                                        .withColumnRenamed("lat", "latitude")\
                                                        .withColumnRenamed("lng", "longitude")\
                                                        .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 4: Add ingestion date to the data frame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

# circuits_final_df = circuits_rename_columns_df.withColumn("ingest_date", current_timestamp())\
#                                                 .withColumn("env", lit("Production"))

# COMMAND ----------

circuits_final_df = ingest_date(circuits_rename_columns_df)\
                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 5: Write processed data to parquet

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits
# MAGIC

# COMMAND ----------

df_read_parquet = spark.read.parquet("/mnt/formula1dldatabrickstut/processed/circuits")


# COMMAND ----------

display(df_read_parquet)

# COMMAND ----------

dbutils.notebook.exit("Success")
