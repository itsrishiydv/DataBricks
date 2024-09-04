# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Introduction
# MAGIC ## UI Introduction
# MAGIC ## Magic Command 
# MAGIC - %python - to Run Python Code
# MAGIC - %sql - To Run SQL Query
# MAGIC - %scala - To Run Scala Code
# MAGIC - %md - Markdown Language Command  to write comments 
# MAGIC - %fs - File System (ls) to use list down filesystem
# MAGIC - %sh - Shell Command (ps) to list down all the running process

# COMMAND ----------

message = "Hello, Databricks!"

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1.2 as col

# COMMAND ----------

# MAGIC %scala
# MAGIC var msg = "Hello Scala"
# MAGIC print(msg)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------


