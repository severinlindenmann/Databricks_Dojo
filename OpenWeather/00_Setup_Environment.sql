-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup OpenWeather Environment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Tables for ELT

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS weather.elt.target_cities (
  id INT NOT NULL PRIMARY KEY,
  city VARCHAR(100)
  );

INSERT INTO weather.elt.target_cities (id, city)
VALUES
  (1, 'Lucerne'),
  (2, 'Zurich'),
  (3, 'Geneva'),
  (4, 'Basel'),
  (5, 'Zug'),
  (6, 'Bern'),
  (7, 'Winterthur'),
  (8, 'Chur'),
  (9, 'Lugano'),
  (10, 'Lausanne'),
  (11, 'Thun'),
  (12, 'Sitten'),
  (13, 'Olten'),
  (14, 'Schaffhausen'),
  (15, 'Aarau'),
  (16, 'Oberrueti'),
  (17, 'Davos'),
  (18, 'Interlaken'),
  (19, 'Kloten'),
  (20, 'Ascona')

-- COMMAND ----------

SELECT * FROM weather.elt.target_cities 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ----------------- END OF SCRIPTS ---------------
-- MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`
