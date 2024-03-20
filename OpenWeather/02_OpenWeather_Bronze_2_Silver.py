# Databricks notebook source
# MAGIC %md
# MAGIC # OpenWeather Bronze 2 Silver

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import col, from_unixtime, to_timestamp

# COMMAND ----------

# DBTITLE 1,define functions
def transform_columns_current(df):
    """
    transforms the input dataframe for current weather data
    - converts columns
    - adds data from nested columns as additional columns
    """
    df = (
        df
        .select(
            col("base").alias("Base"), 
            # "clouds",
            col("clouds")["all"].alias("Clouds_All"),
            col("cod").alias("Cod"), 
            # "coord",
            col("coord")["lon"].alias("Coord_Lon"),
            col("coord")["lat"].alias("Coord_Lat"),
            # "dt", 
            to_timestamp(from_unixtime(col("dt"))).alias("Weather_TimeStamp"),
            col("id").alias("ID"), 
            # "main", 
            col("main")["feels_like"].alias("Main_Feels_Like"),
            col("main")["temp_min"].alias("Main_Temp_Min"),
            col("main")["pressure"].alias("Main_Pressure"),
            col("main")["humidity"].alias("Main_Humidity"),
            col("main")["temp"].alias("Main_Temp"),
            col("main")["temp_max"].alias("Main_Temp_Max"),
            col("name").alias("City"), 
            # "rain",
            col("rain")["1h"].alias("Rain_1h"),
            # "sys",
            col("sys")["country"].alias("Sys_Country"),
            col("sys")["id"].alias("Sys_ID"),
            to_timestamp(from_unixtime(col("sys")["sunrise"])).alias("Sys_Sunrise"),
            to_timestamp(from_unixtime(col("sys")["sunset"])).alias("Sys_Sunset"),
            col("sys")["type"].alias("Sys_Type"),
            col("timezone").alias("Timezone"), 
            col("visibility").alias("Visibility"), 
            # "weather",
            # col("weather")[0],
            col("weather")[0]["icon"].alias("Weather_0_Icon"),
            col("weather")[0]["description"].alias("Weather_0_Description"),
            col("weather")[0]["main"].alias("Weather_0_Main"),
            col("weather")[0]["id"].alias("Weather_0_ID"),
            # "wind",
            col("wind")["speed"].alias("Wind_Speed"),
            col("wind")["deg"].alias("Wind_Deg"),
            "LoadID", 
            "LoadTimeStamp"
            )
        )
    return df

def transform_columns_air_pollution(df):
    """
    transforms the input dataframe for air_pollution
    - converts columns
    - adds data from nested columns as additional columns
    """
    df = (
        df
        .select(
            col("City"),
            # col("coord").alias("Coord"),
            col("coord")["lon"].alias("Coord_Lon"),
            col("coord")["lat"].alias("Coord_Lat"),
            # col("list")[0].alias("list"),
            col("list")[0]["dt"].alias("List_dt"),
            # col("list")[0]["components"].alias("List_Components"),
            col("list")[0]["components"]["pm2_5"].alias("List_Components_pm2_5"),
            col("list")[0]["components"]["pm10"].alias("List_Components_pm10"),
            col("list")[0]["components"]["no2"].alias("List_Components_no2"),
            col("list")[0]["components"]["co"].alias("List_Components_co"),
            col("list")[0]["components"]["nh3"].alias("List_Components_nh3"),
            col("list")[0]["components"]["o3"].alias("List_Components_o3"),
            col("list")[0]["components"]["no"].alias("List_Components_no"),
            col("list")[0]["components"]["so2"].alias("List_Components_so2"),
            col("list")[0]["main"]["aqi"].alias("List_main_aqi"),
            "LoadID", 
            "LoadTimeStamp")
        )
    return df

def transform_columns_cities(df):
    """
    transforms the input dataframe for cities
    - converts columns
    - adds data from nested columns as additional columns
    """
    df = (
        df
        .select(
            col("City"),
            col("Longitude"),
            col("Latitude"),
            col("Response").alias("Response"),
            col("Response")["state"].alias("State"),
            col("Response")["country"].alias("Country"),
            "LoadID", 
            "LoadTimeStamp")
        )
    return df

# COMMAND ----------

# DBTITLE 1,Stream Current Weather data from Bronze to Silver
bronze_stream_weather = (spark
                        .readStream
                        .table("weather.bronze.current")
                        )

bronze_stream_weather = transform_columns_current(bronze_stream_weather)

silver_stream_weather = (bronze_stream_weather
                        .writeStream
                        .format("delta")
                        .outputMode("append")
                        .option("checkpointLocation", f"/Volumes/weather/silver/checkpoints/currrent_weather/_checkpoint")
                        .trigger(once=True)
                        .toTable("weather.silver.current")
                        )

silver_stream_weather.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Stream Air Pollution data from Bronze to Silver
bronze_stream_air_pollution = (spark
                              .readStream
                              .table("weather.bronze.air_pollution")
                              )

bronze_stream_air_pollution = transform_columns_air_pollution(bronze_stream_air_pollution)

silver_stream_air_pollution = (bronze_stream_air_pollution
                              .writeStream
                              .format("delta")
                              .outputMode("append")
                              .option("checkpointLocation", f"/Volumes/weather/silver/checkpoints/air_pollution/_checkpoint")
                              .trigger(once=True)
                              .toTable("weather.silver.air_pollution")
                              )

silver_stream_air_pollution.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Stream cities from Bronze to Silver
bronze_stream_cities = (spark
                              .readStream
                              .table("weather.bronze.cities")
                              )

bronze_stream_cities = transform_columns_cities(bronze_stream_cities)

silver_stream_cities = (bronze_stream_cities
                              .writeStream
                              .format("delta")
                              .outputMode("append")
                              .option("checkpointLocation", f"/Volumes/weather/silver/checkpoints/cities/_checkpoint")
                              .trigger(once=True)
                              .toTable("weather.silver.cities")
                              )

silver_stream_cities.awaitTermination()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`
