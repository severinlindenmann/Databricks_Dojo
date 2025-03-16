# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Load 2 Bronze

# COMMAND ----------

import dlt, re

from pyspark.sql.types import DateType, FloatType, StringType, IntegerType, StructType, StructField, LongType, TimestampType
from pyspark.sql.functions import col, to_date, regexp_replace, current_timestamp, regexp_extract, to_timestamp, date_format, concat, lit, max

from datetime import datetime, timedelta, date

# COMMAND ----------

# DBTITLE 1,append
table_name = "customer_append"

@dlt.table(
    name = table_name,
    comment = 'This is an append only table',
    table_properties = {"quality": "bronze"}
)

def build_table():
    df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("mergeSchema", "true")
                .load(f"/Volumes/demo/source/raw/customer/")
                .select(
                    "*"
                    , col('_metadata')
                )
    )
    # Add the filename by extracting it from _metadata.file_path
    df = df.withColumn("file_name", col("_metadata").getField("file_name"))

    # Extract timestamp using regex
    df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
    df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

    df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
    
    # drop unnecessary columns
    df = df.drop("filename_timestamp_str")
    df = df.drop("file_name")
    
    return df

# COMMAND ----------

# DBTITLE 1,customer_raw_files
view_name = "customer_raw_files"

@dlt.view(
        name = view_name,
        comment = 'The purpose of this view is to use Auto-Loader to read the data from the datalake.',
    )
def build_view():
    df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("mergeSchema", "true")
                .load(f"/Volumes/demo/source/raw/customer/")
                .select(
                    "*"
                    , col('_metadata')
                )
    )
    # Add the filename by extracting it from _metadata.file_path
    df = df.withColumn("file_name", col("_metadata").getField("file_name"))

    # Extract timestamp using regex
    df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
    df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

    df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
    
    # drop unnecessary columns
    df = df.drop("filename_timestamp_str")
    df = df.drop("file_name")
    
    return df

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd1
cdc_table_scd1 = 'customer_cdc_scd1'

dlt.create_streaming_table(cdc_table_scd1)

dlt.apply_changes(
        target=cdc_table_scd1,
        source=view_name,
        keys=["primary_key"],
        sequence_by=col("filename_timestamp"),
        stored_as_scd_type = 1
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2
cdc_table_scd2 = 'customer_cdc_scd2'

dlt.create_streaming_table(cdc_table_scd2)

dlt.apply_changes(
        target=cdc_table_scd2,
        source=view_name,
        keys=["primary_key"],
        sequence_by=col("filename_timestamp"),
        stored_as_scd_type = 2,
        track_history_except_column_list = ["_metadata", "filename_timestamp", "dlt_load_timestamp"]
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2 deletes
cdc_scd2_deletes = "cdc_scd2_deletes"

@dlt.table(
  name=cdc_scd2_deletes)

def filter_deletes():
    return dlt.read("demo.bronze.customer_cdc_scd2").alias("c").where(
        "c.__END_AT IS NULL AND c.__START_AT != (SELECT MAX(__START_AT) FROM demo.bronze.customer_cdc_scd2)"
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2 active
cdc_view_active = "cdc_scd2_active"

@dlt.table(
  name=cdc_view_active
)
def filter_active():
    return dlt.read("demo.bronze.customer_cdc_scd2").alias("c").where(
        "c.__END_AT IS NULL AND c.__START_AT = (SELECT MAX(__START_AT) FROM demo.bronze.customer_cdc_scd2)"
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2 final
cdc_view_final = "cdc_scd2_final"

@dlt.table(
  name=cdc_view_final
)
def filter_active():
    return spark.sql("""
        SELECT * 
        FROM demo.bronze.customer_cdc_scd2 c
        WHERE 
            -- Keep all rows with an end date
            c.__END_AT IS NOT NULL 
            
            -- Keep active rows (__END_AT IS NULL), but only if they are not in the deleted ones
            OR (
                c.__END_AT IS NULL
                AND c.primary_key NOT IN (
                    SELECT primary_key 
                    FROM demo.bronze.cdc_scd2_deletes
                    WHERE __END_AT IS NULL
                )
            )
    """)

# COMMAND ----------

# DBTITLE 1,customer_cdc_snapshot_scd1

def exist(file_path):
    # Checks if the file exists (location-dependent)
    try:
        dbutils.fs.ls(file_path)
        return True
    except:
        return False

# Function to determine the next snapshot based on date
def next_snapshot_and_version(latest_snapshot_version):
    # If no previous snapshot exists, start with the first day (replace with start date)
    if latest_snapshot_version is None:
        latest_snapshot_date = date(2025, 3, 1)
    else:
        latest_snapshot_date = datetime.strptime(str(latest_snapshot_version), "%Y%m%d") + timedelta(days=1)

    # Create the path to the next snapshot
    next_version = latest_snapshot_date.strftime("%Y%m%d")

    file_path = f"/Volumes/demo/source/raw/customer/Year={latest_snapshot_date.year}/Month={latest_snapshot_date.month}/Day={latest_snapshot_date.day}/"

    if exist(file_path):
        df = spark.read.option("basePath", file_path).parquet(f"{file_path}*").select("*", col('_metadata'))

        # Add the filename by extracting it from _metadata.file_path
        df = df.withColumn("file_name", col("_metadata").getField("file_name"))

        # Extract timestamp using regex
        df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
        df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

        df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
        
        # drop unnecessary columns
        df = df.drop("filename_timestamp_str")
        df = df.drop("file_name")
        
        return df, int(next_version)
    
    else:
        return None

dlt.create_streaming_live_table("snapshot_scd1")

dlt.apply_changes_from_snapshot(
    target="snapshot_scd1",
    source=next_snapshot_and_version,
    keys=["primary_key"],
    stored_as_scd_type=1
)

# COMMAND ----------

# DBTITLE 1,customer_cdc_snapshot_scd2

def exist(file_path):
    # Prüft, ob die Datei existiert (Speicherort-abhängig)
    try:
        dbutils.fs.ls(file_path)
        return True
    except:
        return False

# Function to determine the next snapshot based on date
def next_snapshot_and_version(latest_snapshot_version):
    # If no previous snapshot exists, start with the first day (replace with start date)
    if latest_snapshot_version is None:
        latest_snapshot_date = date(2025, 3, 1)
    else:
        latest_snapshot_date = datetime.strptime(str(latest_snapshot_version), "%Y%m%d") + timedelta(days=1)

    # Create the path to the next snapshot
    next_version = latest_snapshot_date.strftime("%Y%m%d")
    
    file_path = f"/Volumes/demo/source/raw/customer/Year={latest_snapshot_date.year}/Month={latest_snapshot_date.month}/Day={latest_snapshot_date.day}/"

    if exist(file_path):
        df = spark.read.option("basePath", file_path).parquet(f"{file_path}*").select("*", col('_metadata'))

        # Add the filename by extracting it from _metadata.file_path
        df = df.withColumn("file_name", col("_metadata").getField("file_name"))

        # Extract timestamp using regex
        df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
        df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

        df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
        
        # drop unnecessary columns
        df = df.drop("filename_timestamp_str")
        df = df.drop("file_name")
        
        return df, int(next_version)
    
    else:
        return None

dlt.create_streaming_live_table("snapshot_scd2")

dlt.apply_changes_from_snapshot(
    target="snapshot_scd2",
    source=next_snapshot_and_version,
    keys=["primary_key"],
    stored_as_scd_type=2,
    track_history_except_column_list = ["_metadata", "filename_timestamp", "dlt_load_timestamp"]
)
