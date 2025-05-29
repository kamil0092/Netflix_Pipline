# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey="WeekDayLookup", key="weekoutput", debugValue=1)

# COMMAND ----------

print(var)

# COMMAND ----------

