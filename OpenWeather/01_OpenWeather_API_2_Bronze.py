# Databricks notebook source
# MAGIC %md
# MAGIC # OpenWeather API Load 2 Bronze
# MAGIC
# MAGIC In this notebook, we load city informations, weather and air pollution data for different cities from the Openweather API into a bronze table. </br></br>
# MAGIC
# MAGIC
# MAGIC An API key from Openweather is required to run this notebook. You can create a free account on https://openweathermap.org and generate the key there.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

# DBTITLE 1,Import libraries
import requests
import json
import uuid
import re
from datetime import datetime
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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

# DBTITLE 1,variables
api_key             = dbutils.secrets.get("kv", "API-Key")
load_id             = str(uuid.uuid4())
utc_timestamp       = datetime.utcnow()
target_cities_table = f"{user_catalog_name}.elt.target_cities"
bronze_table        = f"{user_catalog_name}.bronze.current"

print(f"api_key: {api_key}")
print(f"load_id: {load_id}")
print(f"utc_timestamp: {utc_timestamp}")
print(f"target_cities_table: {target_cities_table}")
print(f"bronze_table: {bronze_table}")

# COMMAND ----------

# DBTITLE 1,define functions
def get_response(url):
    """
    Sends a GET request to the specified URL and returns the response.

    Args:
        url (str): The URL to send the GET request to.

    Returns:
        response: The response object from the GET request.

    Raises:
        Exception: If an error occurs during the GET request.
    """
    try:
        response = requests.get(url)
        return response
    except Exception as e:
        print(e)  
        raise
        
def create_dataframe(response):
    """
    Creates a Spark DataFrame from the JSON response and adds LoadID and LoadTimeStamp columns.

    Args:
        response: The response object containing JSON data.

    Returns:
        DataFrame: A Spark DataFrame with the JSON data and additional columns.
    """
    data = response.json()
    df = spark.createDataFrame(data if isinstance(data, list) else [data])
    df = (df
          .withColumn("LoadID", lit(load_id))
          .withColumn("LoadTimeStamp", lit(utc_timestamp))
    )
    return df

# COMMAND ----------

df_target_cities = spark.read.table(target_cities_table)
target_cities_rows = df_target_cities.select("city").collect()
target_cities = [city["city"] for city in target_cities_rows]
target_cities

# COMMAND ----------

# DBTITLE 1,get target cities
df_cities = None
for city in target_cities:
    print(f"Load metadata for: {city}")
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&appid={api_key}"
    response = get_response(url)
    df = create_dataframe(response)

    if df_cities is None:
        df_cities = df
    else:
        df_cities = df_cities.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

display(df_cities)

# COMMAND ----------

# DBTITLE 1,add columns to the cities dataframe
df_cities = (df_cities          
                .withColumnRenamed("name", "City")
                .withColumnRenamed("lon", "Longitude")
                .withColumnRenamed("lat", "Latitude")
)
display(df_cities)



# COMMAND ----------

# DBTITLE 1,create temp view from dataframe
df_cities.createOrReplaceTempView("TempViewCities")

# COMMAND ----------

# Execute the following code only if the table is empty, if not, skip
if spark.sql(f"SELECT COUNT(*) FROM {user_catalog_name}.bronze.cities").first()[0] == 0:
    print("Write dataframe to table.")
    (
        df_cities.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{user_catalog_name}.bronze.cities")
    )
else: 
    print("There's already data in the table.")

# COMMAND ----------

spark.sql(f"SELECT City, Longitude, Latitude FROM {user_catalog_name}.bronze.cities").display()

# COMMAND ----------

# DBTITLE 1,create a list with city, lon and lat
# Create a list of cities from the bronze.weather.cities table, containing Longitude and Latitude
cities_list = spark.sql(f"SELECT City, Longitude, Latitude FROM {user_catalog_name}.bronze.cities").collect()
print(cities_list)

# COMMAND ----------

# DBTITLE 1,download current weather
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

display(df_current)

# COMMAND ----------

# DBTITLE 1,save current weather data into bronze table
(
    df_current.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{user_catalog_name}.bronze.current")
)

# COMMAND ----------

spark.sql(f"SELECT * FROM {user_catalog_name}.bronze.current").display()

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
    .saveAsTable(f"{user_catalog_name}.bronze.air_pollution")
)

# COMMAND ----------

spark.sql(f"SELECT * FROM {user_catalog_name}.bronze.air_pollution").display()

# COMMAND ----------

# DBTITLE 1,exit notebook
dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`
