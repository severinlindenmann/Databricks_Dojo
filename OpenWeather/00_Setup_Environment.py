# Databricks notebook source
# MAGIC %md
# MAGIC # Setup UC Environment
# MAGIC This notebooks prepares our environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

import re

# COMMAND ----------

def transform_email(email):
    match = re.match(r'^([^@]+)@', email)
    if match:
        username = match.group(1)
        username = re.sub(r'[._]', '_', username)
        return username
    else:
        return None

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_catalog_name = transform_email(user_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {user_catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema's

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.raw")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.elt")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.temp")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.eventhub")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {user_catalog_name}.adventureworks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume's

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {user_catalog_name}.raw.files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {user_catalog_name}.silver.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {user_catalog_name}.bronze.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables for ELT

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {user_catalog_name}.elt.target_cities;")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_catalog_name}.elt.target_cities (
  id INT NOT NULL PRIMARY KEY,
  city VARCHAR(100)
  );
""")

spark.sql(f"""
INSERT INTO {user_catalog_name}.elt.target_cities (id, city)
VALUES
  (1, 'Basel'),
  (2, 'Zurich'),
  (3, 'Geneva'),
  (4, 'Bern'),
  (5, 'Lucerne')

""")

# COMMAND ----------

spark.sql(f"SELECT * FROM {user_catalog_name}.elt.target_cities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create tables for weather sample pipeline

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {user_catalog_name}.bronze.current;")
spark.sql(f"CREATE TABLE IF NOT EXISTS {user_catalog_name}.bronze.air_pollution;")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {user_catalog_name}.bronze.cities
        (Response MAP<STRING, STRING>,
        LoadID STRING,
        LoadTimeStamp TIMESTAMP,
        City STRING,
        Longitude DOUBLE,
        Latitude DOUBLE
        );
        """)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`
