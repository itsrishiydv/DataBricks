# Databricks notebook source
# MAGIC %run "../includes/folder_path_config"

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name")\
                                                             .withColumnRenamed("race_timestamp", "race_date")


# COMMAND ----------

driver_df = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name")\
                                                             .withColumnRenamed("driver_timestamp", "driver_date")\
                                                             .withColumnRenamed("number", "driver_number")\
                                                             .withColumnRenamed("nationality", "driver_nationality")
display(driver_df)

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name")\
                                                  .withColumnRenamed("location","circuit_location")                            

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time")

# COMMAND ----------

race_circuit_df = race_df.join(circuits_df, race_df.circuit_id == circuits_df.circuit_id, "inner")


# COMMAND ----------

final_result_df = results_df.join(race_circuit_df, results_df.race_id == race_circuit_df.race_id, "inner")\
                         .join(driver_df, results_df.driver_id == driver_df.driver_id, "inner")\
                         .join(constructor_df, results_df.constructor_id == constructor_df.constructor_id, "inner")
                         

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = final_result_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality","team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year = 2020 AND race_name like '%Abu Dhabi Grand Prix%' ").orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results

# COMMAND ----------

dbutils.notebook.exit("Success")
