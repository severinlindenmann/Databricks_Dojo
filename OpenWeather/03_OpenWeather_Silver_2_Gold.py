# Databricks notebook source
# MAGIC %md
# MAGIC # OpenWeather Silver 2 Gold

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

sql = f"""
CREATE OR REPLACE VIEW {user_catalog_name}.gold.weather_detailed
AS
SELECT
  c.City,
  c.Longitude,
  c.Latitude,
  c.State,
  c.Country,

  w.Clouds_All,
  w.Weather_TimeStamp,
  w.Main_Feels_Like,
  w.Main_Temp_Min,
  w.Main_Temp,
  w.Main_Temp_Max,
  w.Sys_Sunrise AS Sunrise,
  w.Sys_Sunset AS Sunset,
  w.Visibility,
  w.Wind_Speed,
  w.Wind_Deg

FROM {user_catalog_name}.silver.current AS w
INNER JOIN {user_catalog_name}.silver.cities AS c
  ON  c.City      = w.City
"""
spark.sql(sql)

# COMMAND ----------

sql = f"""
CREATE OR REPLACE VIEW {user_catalog_name}.gold.weather_aggregated
AS
SELECT
  City,
  DATE(Weather_TimeStamp) AS Date,
  AVG(Main_Temp) AS Avg_Temperature,
  MAX(Main_Temp_Max) AS Max_Temperature,
  MIN(Main_Temp_Min) AS Min_Temperature,
  AVG(Wind_Speed) AS Avg_Wind_Speed,
  AVG(Visibility) AS Avg_Visibility
FROM {user_catalog_name}.silver.current
GROUP BY City, DATE(Weather_TimeStamp);
"""
spark.sql(sql)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`
