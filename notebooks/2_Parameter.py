# Databricks notebook source
datasets = [
    {
        "file_name": "netflix_cast"
    },
    {
        "file_name": "netflix_category"
    },
    {
        "file_name": "netflix_countries"
    },
    {
        "file_name": "netflix_directors"
    },
    {
        "file_name": "netflix_title"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)

# COMMAND ----------

