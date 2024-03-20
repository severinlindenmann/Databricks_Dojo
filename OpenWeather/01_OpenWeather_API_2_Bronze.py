# Databricks notebook source
# MAGIC %md
# MAGIC # OpenWeather API Load 2 Bronze
# MAGIC
# MAGIC In this notebook, we load city informations, weather and air pollution data for different cities from the Openweather API into a bronze table. </br></br>
# MAGIC
# MAGIC
# MAGIC An API key from Openweather is required to run this notebook. You can create a free account on https://openweathermap.org and generate the key there.

# COMMAND ----------

# DBTITLE 1,Import libraries
import requests
import json
import uuid

from datetime import datetime
from pyspark.sql.functions import lit, col

# COMMAND ----------

# DBTITLE 1,variables
api_key             = dbutils.secrets.get("kv", "OpenWeatherApiKey")
load_id             = str(uuid.uuid4())
utc_timestamp       = datetime.utcnow()
target_cities_table = "weather.elt.target_cities"
bronze_table        = "weather.bronze.current"

# COMMAND ----------

# DBTITLE 1,create table if not exists
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather.bronze.current;
# MAGIC CREATE TABLE IF NOT EXISTS weather.bronze.air_pollution;
# MAGIC CREATE TABLE IF NOT EXISTS weather.bronze.cities
# MAGIC     (Response MAP<STRING, STRING>,
# MAGIC      LoadID STRING,
# MAGIC      LoadTimeStamp TIMESTAMP,
# MAGIC      City STRING,
# MAGIC      Longitude STRING,
# MAGIC      Latitude STRING
# MAGIC     );

# COMMAND ----------

# DBTITLE 1,define functions
def get_response(url):
    try:
        response = requests.get(url)
        return response
    except Exception as e:
        print(e)  
        raise
        
def create_dataframe(response):
    response_dict = json.loads(response.content)
    df = spark.createDataFrame([response_dict])
    df = (df
          .withColumn("LoadID", lit(load_id))
          .withColumn("LoadTimeStamp", lit(utc_timestamp))
    )
    return df

# COMMAND ----------

# DBTITLE 1,get target cities from a table
df_target_cities = spark.read.table(target_cities_table)
target_cities = df_target_cities.select("city").rdd.flatMap(lambda x: x).collect()
# display(df_target_cities)

# COMMAND ----------

# DBTITLE 1,get target cities
df_cities = None
for city in target_cities:
    print(f"Load metadata for: {city}")
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&&appid={api_key}"
    response = get_response(url)

    df = create_dataframe(response)

    if df_cities is None:
        df_cities = df
    else:
        df_cities = df_cities.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,add columns to the cities dataframe
df_cities = (df_cities          
                .withColumnRenamed("_1", "Response")
                .withColumn("City", col("Response")["name"])
                .withColumn("Longitude", col("Response")["lon"])
                .withColumn("Latitude", col("Response")["lat"])
)
display(df_cities)

# COMMAND ----------

# DBTITLE 1,create temp view from dataframe
df_cities.createOrReplaceTempView("TempViewCities")

# COMMAND ----------

# DBTITLE 1,merge dataframe into bronze table
# MAGIC %sql
# MAGIC MERGE INTO weather.bronze.cities AS target 
# MAGIC   USING TempViewCities AS source 
# MAGIC     ON target.City = source.City
# MAGIC     AND target.Longitude = source.Longitude
# MAGIC     AND target.Latitude = source.Latitude
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# DBTITLE 1,create a list with city, lon and lat
# Create a list of cities from the bronze.weather.cities table, containing Longitude and Latitude
cities_list = spark.sql("SELECT City, Longitude, Latitude FROM weather.bronze.cities").collect()
print(f"City List: {cities_list}")

# COMMAND ----------

# DBTITLE 1,download cirrent weather
df_current = None
for c in cities_list:
    print(f"Load current weather data for: {c.City}")
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={c.Latitude}&lon={c.Longitude}&appid={api_key}&units=metric"
    response = get_response(url)

    df = create_dataframe(response)

    if df_current is None:
        df_current = df
    else:
        df_current = df_current.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,save current weather data into bronze table
(
    df_current.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("weather.bronze.current")
)

# COMMAND ----------

# DBTITLE 1,load air pollution data
df_air_pollution = None

for c in cities_list:
    print(f"Load air pollution data for: {c.City}")

    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={c.Latitude}&lon={c.Longitude}&appid={api_key}"
    response = get_response(url)

    df = create_dataframe(response)

    df = df.withColumn("City", lit(city))

    if df_air_pollution is None:
        df_air_pollution = df
    else:
        df_air_pollution = df_air_pollution.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,save air pollution data into bronze table
(
    df_air_pollution.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("weather.bronze.air_pollution")
)

# COMMAND ----------

# DBTITLE 1,exit notebook
dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM weather.bronze.current

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM weather.bronze.air_pollution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM weather.bronze.cities
