-- Databricks notebook source
-- MAGIC %md
-- MAGIC # OpenWeather Silver 2 Gold

-- COMMAND ----------

CREATE OR REPLACE VIEW weather.gold.weather
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
  w.Rain_1h,
  w.Sys_Sunrise AS Sunrise,
  w.Sys_Sunset AS Sunset,
  w.Visibility,
  w.Wind_Speed,
  w.Wind_Deg

FROM            weather.silver.current AS w
INNER JOIN weather.silver.cities AS c
  ON  c.City      = w.City

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ----------------- END OF SCRIPTS ---------------
-- MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`

-- COMMAND ----------

-- SELECT * FROM weather.gold.weather
