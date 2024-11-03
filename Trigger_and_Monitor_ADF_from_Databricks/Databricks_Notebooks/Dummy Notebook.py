# Databricks notebook source
# MAGIC %md
# MAGIC # Dummy Notebook

# COMMAND ----------

import requests
import json

# COMMAND ----------

dbutils.widgets.text("adf_run_id", "adf-test")
dbutils.widgets.text("dbx_run_id", "dbx-test")

adf_run_id = dbutils.widgets.get("adf_run_id")
dbx_run_id = dbutils.widgets.get("dbx_run_id")

print(f'adf_run_id: {adf_run_id}')
print(f'dbx_run_id: {dbx_run_id}')
