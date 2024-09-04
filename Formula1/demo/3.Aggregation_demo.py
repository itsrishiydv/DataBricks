# Databricks notebook source
# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregation Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built-In Aggregate function

# COMMAND ----------

results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(results_df)

# COMMAND ----------

demo_df = results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("race_name")).withColumnRenamed("count(race_name)","race_count").show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).withColumnRenamed("count(DISTINCT race_name)","race_count").show()

# COMMAND ----------

demo_df.filter("race_name = 'Austrian Grand Prix'").select(count("race_name"),sum("points"))\
        .withColumnRenamed("count(race_name)","race_count")\
        .withColumnRenamed("sum(points)", "total_points").show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(countDistinct("race_name").alias("race_count"),sum("points").alias("total_points"))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

demo_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year in (2019,2020)")

# COMMAND ----------

demo_gropped_df = demo_df.groupBy("race_year","driver_name")\
    .agg(countDistinct("race_name").alias("race_count"),sum("points").alias("total_points"))


# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
display(demo_gropped_df.withColumn("rank", rank().over(driverRankSpec)))


# COMMAND ----------


