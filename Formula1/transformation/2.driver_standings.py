# Databricks notebook source
# MAGIC %md
# MAGIC #### Driver Standings

# COMMAND ----------

# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_result_df)

# COMMAND ----------

from pyspark.sql.functions import when, lit, col, count, sum, desc, rank

# COMMAND ----------

driver_result_df = race_result_df\
    .groupBy("race_year","driver_name","driver_nationality","team")\
    .agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

rank_config = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins")) 
final_df = driver_result_df.withColumn("rank",rank().over(rank_config))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings
