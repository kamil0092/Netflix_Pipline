# Databricks notebook source
# MAGIC %md
# MAGIC ### **Incremetal Data Loading Using AutoLoader**
# MAGIC ### Using Spark Structure Steaming

# COMMAND ----------

# %sql 
# CREATE SCHEMA netflix_cata.net_schema

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

df = spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"abfss://silver@netflix2003sa.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://bronze@netflix2003sa.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Write Data**

# COMMAND ----------

df.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://silver@netflix2003sa.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path", f"abfss://silver@netflix2003sa.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

