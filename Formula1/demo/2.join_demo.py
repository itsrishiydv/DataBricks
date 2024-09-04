# Databricks notebook source
# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

from pyspark.sql.functions import col,lit 

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name")
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/race").filter("race_year = 2019").withColumnRenamed("name","races_name")
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Inner Join

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, races_df["circuit_id"] == circuits_df["circuit_id"], "inner")\
    .select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.races_name,races_df.round)
display(races_circuits_df)

# COMMAND ----------

races_circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Outer Join

# COMMAND ----------

## Left Outer Join
races_circuits_left_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id, "left")
display(races_circuits_left_df)

# COMMAND ----------

## Right Outer Join
races_circuits_right_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "right")
display(races_circuits_right_df)

# COMMAND ----------

## full Outer Join
races_circuits_full_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "full")
display(races_circuits_full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. semi join

# COMMAND ----------

# Semi Join is similar to an Inner Join, but only returns the rows from the left DataFrame that have a match in the right
races_circuits_semi_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "semi")
display(races_circuits_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Anti Join 

# COMMAND ----------

## Anti Join is the inverse of a Semi Join, returning all rows from the left DataFrame that do not have a match in the right
races_circuits_anti_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "anti")
display(races_circuits_anti_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Cross Join

# COMMAND ----------

## Cross Join is a Cartesian Product of two DataFrames, returning all combinations of rows from both DataFrames
races_circuits_cross_df = circuits_df.crossJoin(races_df)
display(races_circuits_cross_df)

