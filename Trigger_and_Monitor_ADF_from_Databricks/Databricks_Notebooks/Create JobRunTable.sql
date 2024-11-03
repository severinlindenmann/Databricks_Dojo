-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create JobRunTable

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo;
CREATE SCHEMA IF NOT EXISTS demo.demo;
CREATE TABLE IF NOT EXISTS demo.demo.job_run (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    dbx_start_run_id STRING,
    dbx_follow_run_id STRING,
    adf_run_id STRING,
    adf_run_status STRING, 
    PRIMARY KEY (id)
);

-- COMMAND ----------


