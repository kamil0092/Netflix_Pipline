# Databricks notebook source
# MAGIC %md
# MAGIC ### **Silver Data transformation**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window  

# COMMAND ----------

df = spark.read.format("delta").load("abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_title")

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling null value

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### Type cast 
# MAGIC - ##### split 
# MAGIC - ##### Add Flag

# COMMAND ----------

df = df.withColumn("duration_minutes", df["duration_minutes"].cast("int"))\
    .withColumn("duration_seasons", df["duration_seasons"].cast("int"))

# COMMAND ----------

df = df.withColumn("Shorttitle", split(col('title'), ':')[0])

# COMMAND ----------

df = df.withColumn("rating", split(col('rating'), '-')[0])

# COMMAND ----------

df = df.withColumn("type_flag", when(col("type")== 'Movie', 1).when(col("type")== 'TV Show', 2).otherwise(0))

# COMMAND ----------

df = df.withColumn("duration_rank", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

display(df)    

# COMMAND ----------

# MAGIC %md
# MAGIC -  Temp View: Notebook based View
# MAGIC -  Global Temp View: Session Based View Accessable out side the cluster via global.global_view
# MAGIC -  Regural View : Permanat View

# COMMAND ----------

## Using SQL Rank Function 
# df.createOrReplaceTempView('df_view')

# COMMAND ----------

# df = spark.sql("select duration_minutes, dense_rank() over(order by duration_minutes desc) from df_view")

# COMMAND ----------

df_viaualization = df.groupBy('type').agg(count("*").alias("total_count"))

# COMMAND ----------

# display(df_visualization)

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .option("path", "abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_title")\
    .save()

# COMMAND ----------

