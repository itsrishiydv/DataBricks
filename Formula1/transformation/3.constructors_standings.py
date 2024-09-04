# Databricks notebook source
# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

from pyspark.sql.functions import lit, col, when, desc, count, sum, rank
from pyspark.sql.window import Window

# COMMAND ----------

constructor_standing_df = race_result_df.groupBy("race_year","team")\
                                        .agg(count(when(col("position") == 1, True)).alias("wins"),sum("points").alias("total_points"))

# COMMAND ----------

partition_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standing_df.withColumn("rank", rank().over(partition_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.constructor_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings
