# Databricks notebook source
# MAGIC %md
# MAGIC ###  DLT Notebook - Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectation Rules

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

lookup_rules = {
  "rule1": "show_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)
@dlt.expect_all_or_drop(lookup_rules)
def myfunc():
    df = spark.readStream.format("delta")\
        .load("abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)
@dlt.expect_all_or_drop(lookup_rules)
def myfunc():
    df = spark.readStream.format("delta")\
        .load("abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)
@dlt.expect_all_or_drop(lookup_rules)
def myfunc():
    df = spark.readStream.format("delta")\
        .load("abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_countries")
    return df 

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)
@dlt.expect_all_or_drop(lookup_rules)
def myfunc():
    df = spark.readStream.format("delta")\
        .load("abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_category")
    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stagging Layer

# COMMAND ----------

@dlt.table
def gold_stg_netflixtitles():
    df = spark.readStream.format("delta")\
        .load("abfss://silver@netflix2003sa.dfs.core.windows.net/netflix_title")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### View

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view

def gold_trans_netflixtitles():

  df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
  df = df.withColumn("newflag", lit(1))
  return df


# COMMAND ----------

masterdata_rules = {
    "rule1": "newflag is NOT NULL",
    "rule2": "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():
    df = spark.readStream.table("LIVE.gold_trans_netflixtitles")
    return df

# COMMAND ----------

