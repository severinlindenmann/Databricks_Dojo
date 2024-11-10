# Databricks notebook source
# MAGIC %md
# MAGIC # Create Dimensions for Date and Time
# MAGIC
# MAGIC ## Description  
# MAGIC - Create table for date dimension
# MAGIC - Create table for time dimension in seconds
# MAGIC - Create table for time dimension in minutes
# MAGIC - Create table which combines the dimensions date and timeminute in one table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

from pyspark.sql.types import LongType

from pyspark.sql.functions import year, month, day, hour, minute, second, col, expr, when, date_format, lit, to_timestamp, min, max, ceil, to_date

# COMMAND ----------

dbutils.widgets.text('start_date',  '2020-01-01',   'Start Date')
dbutils.widgets.text('end_date',    '2050-12-31',   'End Date')
dbutils.widgets.text('catalog',     'demo',         'Catalog')

# COMMAND ----------

start_date            = dbutils.widgets.get('start_date')
end_date              = dbutils.widgets.get('end_date')
catalog               = dbutils.widgets.get('catalog')
date_table            = f'{catalog}.silver.dim_date'
time_minute_table     = f'{catalog}.silver.dim_time_minute'
mapping_minute_table  = f'{catalog}.silver.dim_mapping_minute'

print(f'Create Dimension Tables for Date and Time')
print(f'Timerange: {start_date} to {end_date}')
print(f'Create the following tables: ')
print(f'{date_table}')
print(f'{time_minute_table}')
print(f'{mapping_minute_table}')

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver").display()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold").display()

# COMMAND ----------

# Translation and abbreviated translation for German, French, and Italian 
months_dict = {
    "January":    ["Januar",    "Jan.", "janvier",     "janv.", "gennaio",    "gen."],
    "February":   ["Februar",   "Feb.", "février",     "fév.",  "febbraio",   "feb."],
    "March":      ["März",      "Mär.", "mars",        "mar.",  "marzo",      "mar."],
    "April":      ["April",     "Apr.", "avril",       "avr.",  "aprile",     "apr."],
    "May":        ["Mai",       "Mai.", "mai",         "mai.",  "maggio",     "mag."],
    "June":       ["Juni",      "Jun.", "juin",        "jui.",  "giugno",     "giu."],
    "July":       ["Juli",      "Jul.", "juillet",     "juil.", "luglio",     "lug."],
    "August":     ["August",    "Aug.", "août",        "aoû.",  "agosto",     "ago."],
    "September":  ["September", "Sep.", "septembre",   "sept.", "settembre",  "set."],
    "October":    ["Oktober",   "Okt.", "octobre",     "oct.",  "ottobre",    "ott."],
    "November":   ["November",  "Nov.", "novembre",    "nov.",  "novembre",   "nov."],
    "December":   ["Dezember",  "Dez.", "décembre",    "déc.",  "dicembre",   "dic."],
}

# Translation and abbreviated translation for German, French, and Italian
days_dict = {
    "Monday":     ["Montag",      "Mo.", "lundi",    "lun.", "lunedì",    "lun."],
    "Tuesday":    ["Dienstag",    "Di.", "mardi",    "mar.", "martedì",   "mar."],
    "Wednesday":  ["Mittwoch",    "Mi.", "mercredi", "mer.", "mercoledì", "mer."],
    "Thursday":   ["Donnerstag",  "Do.", "jeudi",    "jeu.", "giovedì",   "gio."],
    "Friday":     ["Freitag",     "Fr.", "vendredi", "ven.", "venerdì",   "ven."],
    "Saturday":   ["Samstag",     "Sa.", "samedi",   "sam.", "sabato",    "sab."],
    "Sunday":     ["Sonntag",     "So.", "dimanche", "dim.", "domenica",  "dom."],
}

date_mapping_dict = {"Months": months_dict, "Days": days_dict}

# Define the nested mapping dictionary for different window groups
time_mapping_dict = {
    "5Min": {
        1: "00-05",
        2: "05-10",
        3: "10-15",
        4: "15-20",
        5: "20-25",
        6: "25-30",
        7: "30-35",
        8: "35-40",
        9: "40-45",
        10: "45-50",
        11: "50-55",
        12: "55-60"
    },
    "10Min": {
        1: "00-10",
        2: "10-20",
        3: "20-30",
        4: "30-40",
        5: "40-50",
        6: "50-60"
    },
    "15Min": {
        1: "00-15",
        2: "15-30",
        3: "30-45",
        4: "45-60"
    },
    "20Min": {
        1: "00-20",
        2: "20-40",
        3: "40-60"
    },
    "30Min": {
        1: "00-30",
        2: "30-60"
    },
    "60Min": {
        1: "00-60"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define function(s)

# COMMAND ----------

# Create a UDF to map the time values to the corresponding window group
get_window_group = udf(
    lambda minute, window_type: time_mapping_dict[window_type][
        next(
            (
                group
                for group, window in time_mapping_dict[window_type].items()
                if int(window.split("-")[0]) <= minute < int(window.split("-")[1])
            ),
            0,
        )
    ]
)

# COMMAND ----------

# DBTITLE 1,function to create time windows
def create_time_window(df, duration, timestampCol="TimestampStartUtc", minuteCol="TimeMinute", secondCol="TimeSecond"):
  """
  Adds time window columns to the given dataframe.

  Parameters:
  df (DataFrame): Input dataframe.
  duration (int): Duration of the time window in minutes.
  timestampCol (str): Column name for the timestamp. Default is "TimestampStartUtc".
  minuteCol (str): Column name for the minute. Default is "TimeMinute".
  secondCol (str): Column name for the second. Default is "TimeSecond".

  Returns:
  DataFrame: Dataframe with added columns:
    - IsWindow<duration>MinuteStart: Boolean indicating if the minute is the start of a window.
    - Window<duration>MinuteId: Integer ID of the window group.
    - Window<duration>MinuteName: String name of the window group.
  """
  
  is_window_start = f"IsWindow{duration}MinuteStart"
  window_id = f"Window{duration}MinuteId"
  window_name = f"Window{duration}MinuteName"
    
  df = (df
        .withColumn(is_window_start, when((col(minuteCol) % duration == 0) & (col(secondCol) == 0), True).otherwise(False))
        .withColumn(window_id, ceil((col(minuteCol) + 1) / duration).cast("integer"))
        .withColumn(window_name, get_window_group(minuteCol, lit(f"{duration}Min")))
      )
  return df

# COMMAND ----------

# DBTITLE 1,function for adding date columns
def add_date_columns(df):
  """
  Function to add date-related columns.
  
  Parameters:
  df (DataFrame): Input dataframe with a 'Date' column.
  
  Returns:
  DataFrame: Dataframe with added date-related columns.
  List[str]: List of the added date-related column names.
  
  The added columns include:
    - DatePK: Primary key for the date in the format YYYYMMDD.
    - DateString: Date formatted as dd.MM.yyyy.
    - Year: Year of the date.
    - IsLeapYear: Boolean indicating if the year is a leap year.
    - HalfYearId: Half of the year (1 for Jan-Jun, 2 for Jul-Dec).
    - YearHalfYearId: Unique identifier for the half-year.
    - HalfYearName: Name of the half-year (H1 or H2).
    - YearHalfYear: Combination of year and half-year name.
    - Quarter: Quarter of the year (1 to 4).
    - YearQuarterId: Unique identifier for the quarter.
    - YearQuarter: Combination of year and quarter.
    - QuarterShortname: Short name for the quarter (Q1 to Q4).
    - YearMonthId: Unique identifier for the year and month.
    - Month: Month of the date.
    - MonthName: Full name of the month.
    - MonthNameShort: Short name of the month.
    - MonthNameDe: Full name of the month in German.
    - MonthNameShortDe: Short name of the month in German.
    - MonthNameFr: Full name of the month in French.
    - MonthNameShortFr: Short name of the month in French.
    - MonthNameIt: Full name of the month in Italian.
    - MonthNameShortIt: Short name of the month in Italian.
    - WeekIdIso: ISO week number of the year.
    - WeekOfYearIso: ISO week number.
    - WeekOfYearIsoName: ISO week number with 'W' prefix.
    - YearWeekOfYearIso: Combination of year and ISO week number.
    - DayName: Full name of the day.
    - DayNameShort: Short name of the day.
    - DayNameDe: Full name of the day in German.
    - DayNameShortDe: Short name of the day in German.
    - DayNameFr: Full name of the day in French.
    - DayNameShortFr: Short name of the day in French.
    - DayNameIt: Full name of the day in Italian.
    - DayNameShortIt: Short name of the day in Italian.
    - DayOfYear: Day of the year.
    - Day: Day of the month.
    - DayOfWeekUs: Day of the week (0=Sunday, 6=Saturday).
    - DayOfWeekIso: ISO day of the week (1=Monday, 7=Sunday).
    - IsWeekDay: Boolean indicating if the day is a weekday.
    - IsLastDayOfMonth: Boolean indicating if the day is the last day of the month.
    - LastDayOfMonth: Last day of the month.
    - MonthDay: Date with the same day and month but year 1972.
    - StartOfWeekUs: Start date of the week (US).
    - StartOfWeekIso: Start date of the week (ISO).
    - EndOfWeekUs: End date of the week (US).
    - EndOfWeekIso: End date of the week (ISO).
    - IsHoliday: Boolean indicating if the day is a holiday.
  """
  #date
  df = (df
        .withColumn("DatePK", expr("year(Date) * 10000 + month(Date) * 100 + day(Date)"))
        .withColumn("DateString", expr("date_format(Date, 'dd.MM.yyyy')"))
      )

  #year
  df = (df
        .withColumn("Year", year(col("Date")))
        .withColumn("IsLeapYear",when((expr("year(Date) % 4 == 0") & expr("year(Date) % 100 != 0")) | expr("year(Date) % 400 == 0"), True).otherwise(False))
        # How to calculate leap year: https://learn.microsoft.com/en-us/office/troubleshoot/excel/determine-a-leap-year
      )

  #halfyear
  df = (df
        .withColumn("HalfYearId",when(month(df["Date"]).between(1, 6), 1).otherwise(2))
        .withColumn("YearHalfYearId",col("Year")*10+col("HalfYearId"))
        .withColumn("HalfYearName",when(month(df["Date"]).between(1, 6), "H1").otherwise("H2"))
        .withColumn("YearHalfYear", expr("concat(Year,'-', HalfYearName)"))
      )

  #quarter
  df = (df
        .withColumn("Quarter", expr("quarter(Date)")) 
        .withColumn("YearQuarterId", expr("year(Date) * 10 + quarter(Date)"))
        .withColumn("YearQuarter", expr("concat(Year,'-Q', quarter(Date))"))
        .withColumn("QuarterShortname", expr("concat('Q', quarter(Date))"))
      )

  #month
  df = (df
        .withColumn("YearMonthId", expr("CAST(date_format(Date, 'yyyyMM') AS INT)"))
        .withColumn("Month", month("Date"))
        .withColumn("MonthName", expr("date_format(Date, 'MMMM')"))
        .withColumn("MonthNameShort", expr("date_format(Date, 'MMM')"))
        .withColumn("MonthNameDe",expr("CASE "+ " ".join(["WHEN MonthName == '{0}' THEN '{1}'".format(k, v[0])for k, v in date_mapping_dict["Months"].items()])+ " END"),)
        .withColumn("MonthNameShortDe",expr("CASE "+ " ".join(["WHEN MonthName == '{0}' THEN '{1}'".format(k, v[1])for k, v in date_mapping_dict["Months"].items()])+ " END"),)
        .withColumn("MonthNameFr",expr("CASE "+ " ".join(["WHEN MonthName == '{0}' THEN '{1}'".format(k, v[2])for k, v in date_mapping_dict["Months"].items()])+ " END"),)
        .withColumn("MonthNameShortFr",expr("CASE "+ " ".join(["WHEN MonthName == '{0}' THEN '{1}'".format(k, v[3])for k, v in date_mapping_dict["Months"].items()])+ " END"),)
        .withColumn("MonthNameIt",expr("CASE "+ " ".join(["WHEN MonthName == '{0}' THEN '{1}'".format(k, v[4])for k, v in date_mapping_dict["Months"].items()])+ " END"),)
        .withColumn("MonthNameShortIt",expr("CASE "+ " ".join(["WHEN MonthName == '{0}' THEN '{1}'".format(k, v[5])for k, v in date_mapping_dict["Months"].items()])+ " END"),)
      )

  #week
  df = (df
        .withColumn("WeekIdIso", expr("year(Date) * 100 + extract(WEEKS FROM Date)"))
        .withColumn("WeekOfYearIso", expr("extract(WEEKS FROM Date)"))
        .withColumn("WeekOfYearIsoName", expr("concat('W', WeekOfYearIso)"))
        # the number of the ISO 8601 week-of-week-based-year. A week is considered to start on a Monday and week 1 is the first week with >3 days. In the ISO week-numbering system, it is possible for early-January dates to be part of the 52nd or 53rd week of the previous year, and for late-December dates to be part of the first week of the next year. For example, 2005-01-02 is part of the 53rd week of year 2004,  while 2012-12-31 is part of the first week of 2013.
        .withColumn("YearWeekOfYearIso", expr("concat(Year,'-', WeekOfYearIsoName)"))
      )

  #day
  df = (df
        .withColumn("DayName", expr("date_format(Date, 'EEEE')"))
        .withColumn("DayNameShort", expr("date_format(Date, 'EEE')"))
        .withColumn("DayNameDe",expr("CASE "+ " ".join(["WHEN DayName == '{0}' THEN '{1}'".format(k, v[0])for k, v in date_mapping_dict["Days"].items()])+ " END"),)
        .withColumn("DayNameShortDe",expr("CASE "+ " ".join(["WHEN DayName == '{0}' THEN '{1}'".format(k, v[1])for k, v in date_mapping_dict["Days"].items()])+ " END"),)
        .withColumn("DayNameFr",expr("CASE "+ " ".join(["WHEN DayName == '{0}' THEN '{1}'".format(k, v[2])for k, v in date_mapping_dict["Days"].items()])+ " END"),)
        .withColumn("DayNameShortFr",expr("CASE "+ " ".join(["WHEN DayName == '{0}' THEN '{1}'".format(k, v[3])for k, v in date_mapping_dict["Days"].items()])+ " END"),)
        .withColumn("DayNameIt",expr("CASE "+ " ".join(["WHEN DayName == '{0}' THEN '{1}'".format(k, v[4])for k, v in date_mapping_dict["Days"].items()])+ " END"),)
        .withColumn("DayNameShortIt",expr("CASE "+ " ".join(["WHEN DayName == '{0}' THEN '{1}'".format(k, v[5])for k, v in date_mapping_dict["Days"].items()])+ " END"),)
        .withColumn("DayOfYear", expr("dayofyear(Date)"))
        .withColumn("Day", expr("dayofmonth(Date)"))
        .withColumn("DayOfWeekUs", expr("extract(DOW FROM Date)"))
        .withColumn("DayOfWeekIso", expr("extract(DOW_ISO FROM Date)"))
        .withColumn("IsWeekDay", expr("DayOfWeekIso < 6"))
        .withColumn("IsLastDayOfMonth", expr("Date = last_day(Date)"))
        .withColumn("LastDayOfMonth", expr("last_day(Date)"))
        .withColumn("MonthDay", expr("make_date(1972, Month, Day)"))
        .withColumn("StartOfWeekUs", expr("date_sub(Date, DayOfWeekUs-1)"))
        .withColumn("StartOfWeekIso", expr("date_sub(Date, DayOfWeekIso-1)"))
        .withColumn("EndOfWeekUs", expr("date_add(Date, 7-DayOfWeekUs)"))
        .withColumn("EndOfWeekIso", expr("date_add(Date, 7-DayOfWeekIso)"))
        .withColumn("IsHoliday", lit(None).cast("boolean"))
      )
  
  date_columns = df.columns

  return df, date_columns

# COMMAND ----------

# DBTITLE 1,function for adding time_columns
def add_time_columns(df):
  """
  Adds time-related columns to the given dataframe.

  Parameters:
  df (DataFrame): Input dataframe with a 'TimestampPK' column representing seconds since epoch.

  Returns:
  DataFrame: Dataframe with added time-related columns.
  List[str]: List of the added time-related column names.
  
  The added columns include:
    - TimestampStartUtc: Timestamp in UTC.
    - TimePK: Time in seconds since the start of the day.
    - Time: Time as a timestamp.
    - TimeString: Time formatted as HH:mm:ss.
    - TimeHour: Hour of the time.
    - TimeMinute: Minute of the time.
    - TimeSecond: Second of the time.
    - AmPm: AM/PM indicator.
    - TimeSecondId: Unique identifier for each second of the day.
    - TimeMinuteId: Unique identifier for each minute of the day.
    - IsWindow<duration>MinuteStart: Boolean indicating if the minute is the start of a window.
    - Window<duration>MinuteId: Integer ID of the window group.
    - Window<duration>MinuteName: String name of the window group.
  """
  # generate timestamps from TimePK
  df = (df
        .withColumn("TimestampStartUtc", col("TimestampPK").cast("timestamp"))
        .withColumn("TimePK", expr("TimestampPK%86400"))
        .withColumn("Time", col("TimePK").cast("timestamp"))
        .withColumn("TimeString", date_format(col("Time"), "HH:mm:ss"))
      )

  # add time columns
  df = (df
        .withColumn("TimeHour", hour("Time"))
        .withColumn("TimeMinute", minute("Time"))
        .withColumn("TimeSecond", second("Time"))
        .withColumn("AmPm", date_format("Time", "a"))
      )

  # add minute and second identifiers
  df = (df
        .withColumn("TimeSecondId", expr("TimeHour*10000+TimeMinute*100+TimeSecond"))
        .withColumn("TimeMinuteId", expr("TimeHour*100+TimeMinute"))
      )

  # Create windows
  windows = [5, 10, 15, 20, 30, 60]
  for window in windows:
      df = create_time_window(df, window)
  
  time_columns = df.columns
   
  return df, time_columns

# COMMAND ----------

def expand_date_to_datetime_table(df_date, resolution_in_seconds=1):
  """
  Expands a given date range dataframe to a datetime table with specified resolution in seconds.

  Parameters:
  df_date (DataFrame): Input dataframe containing a date range with a 'Date' column.
  resolution_in_seconds (int): The resolution in seconds for the datetime table. Default is 1 second.

  Returns:
  DataFrame: Dataframe with expanded datetime information.
  List[str]: List of the added time-related column names.

  The function performs the following steps:
    1. Calculates the range of seconds for the given date range.
    2. Creates a dataframe with a range of seconds within the specified resolution.
    3. Adds time-related columns using the `add_time_columns` function.
    4. Adds date-related columns using the `add_date_columns` function.
    5. Renames the Date and WeekTime primary keys to foreign keys.
    6. Adds local date and week time columns.

  """
  date_to_seconds_range = (df_date
                        .select(to_timestamp("Date").cast(LongType()).alias("Seconds"))
                        .groupBy(lit(1))
                        .agg(min("Seconds").alias("min"), 
                            max("Seconds").alias("max")
                            )
                        .select("min","max")
                      ).collect()[0]

  # Create range for all seconds in 24h
  df = spark.range(date_to_seconds_range.min, date_to_seconds_range.max+1, resolution_in_seconds).select(col("id").cast(LongType()).alias("TimestampPK"))

  # add time columns
  df, time_columns = add_time_columns(df)

  # add date columns
  df, _ = add_date_columns(df.withColumn("Date", to_date("TimestampStartUtc")))
  
  # rename the Date and WeekTime PK to FK (Foreign Key)
  df = (df
        .withColumnRenamed("DatePK", "DateUtcFK")
        .withColumnRenamed("WeekTimeUtcPK", "WeekTimeUtcFK")
       )

  # add local date and week time columns
  df = (df     
        .withColumn("WeekTimePK", (col("DayOfWeekIso")-1)*86400 + col("TimePK"))
        .withColumn("WeekTime", to_timestamp(to_timestamp(lit("1970-01-05")).cast(LongType())+col("WeekTimePK")))
        .withColumn("WeekTimeDuration", col("WeekTimePK")/86400.0)
        .withColumn("DayTimeDuration", col("TimePK")/86400.0)
      )
  
  return df, time_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dimension date

# COMMAND ----------

# Create the dataframe with date interval
df_date_range = spark.sql(f"SELECT EXPLODE(SEQUENCE(to_date('{start_date}'), to_date('{end_date}'), INTERVAL 1 DAY)) AS Date")

# add date columns
df_date_range, date_columns = add_date_columns(df_date_range)

# COMMAND ----------

display(df_date_range)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dimension time in minutes

# COMMAND ----------

df_time_range_minutes, time_columns = expand_date_to_datetime_table(df_date=df_date_range, resolution_in_seconds=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Mapping Table

# COMMAND ----------

date_timestamp_columns = ["DateUtcFK", "TimestampStartUtc", "WeekTimePK"]

df_mapping_minutes = df_time_range_minutes.select(date_timestamp_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Time Table

# COMMAND ----------

week_time_filter_query = f"Year=year('{start_date}') AND Month=month('{start_date}')"

df_time_minutes = (df_time_range_minutes
                  .filter(week_time_filter_query)
                  .select(["WeekTimePK", "WeekTime", "WeekTimeDuration", "DayTimeDuration"]+time_columns)
                  .drop('TimestampPK', 'TimestampStartUtc')
                  .distinct()
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save dataframes to tables
# MAGIC I splittet the write-operations, because spark terminates unexpectedly when all operations are executed in the same cell.

# COMMAND ----------

df_date_range.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(date_table)

# COMMAND ----------

df_time_minutes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(time_minute_table)

# COMMAND ----------

df_mapping_minutes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(mapping_minute_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create View

# COMMAND ----------

# DBTITLE 1,vw_dim_date_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_minute AS
  SELECT
    dd.Date,
    dd.DatePK,
    dd.DateString,
    dd.Year,
    dd.IsLeapYear,
    dd.HalfYearId,
    dd.YearHalfYearId,
    dd.HalfYearName,
    dd.YearHalfYear,
    dd.Quarter,
    dd.YearQuarterId,
    dd.YearQuarter,
    dd.QuarterShortname,
    dd.YearMonthId,
    dd.Month,
    dd.MonthName,
    dd.MonthNameShort,
    dd.MonthNameDe,
    dd.MonthNameShortDe,
    dd.MonthNameFr,
    dd.MonthNameShortFr,
    dd.MonthNameIt,
    dd.MonthNameShortIt,
    dd.WeekIdIso,
    dd.WeekOfYearIso,
    dd.WeekOfYearIsoName,
    dd.YearWeekOfYearIso,
    dd.DayName,
    dd.DayNameShort,
    dd.DayNameDe,
    dd.DayNameShortDe,
    dd.DayNameFr,
    dd.DayNameShortFr,
    dd.DayNameIt,
    dd.DayNameShortIt,
    dd.DayOfYear,
    dd.Day,
    dd.DayOfWeekUs,
    dd.DayOfWeekIso,
    dd.IsWeekDay,
    dd.IsLastDayOfMonth,
    dd.LastDayOfMonth,
    dd.MonthDay,
    dd.StartOfWeekUs,
    dd.StartOfWeekIso,
    dd.EndOfWeekUs,
    dd.EndOfWeekIso,
    dd.IsHoliday,

    tm.WeekTimePK,
    tm.WeekTime,
    tm.WeekTimeDuration,
    tm.DayTimeDuration,
    tm.TimePK,
    tm.Time,
    tm.TimeString,
    tm.TimeHour,
    tm.TimeMinute,
    tm.TimeSecond,
    tm.AmPm,
    tm.TimeSecondId,
    tm.TimeMinuteId,
    tm.IsWindow5MinuteStart,
    tm.Window5MinuteId,
    tm.Window5MinuteName,
    tm.IsWindow10MinuteStart,
    tm.Window10MinuteId,
    tm.Window10MinuteName,
    tm.IsWindow15MinuteStart,
    tm.Window15MinuteId,
    tm.Window15MinuteName,
    tm.IsWindow20MinuteStart,
    tm.Window20MinuteId,
    tm.Window20MinuteName,
    tm.IsWindow30MinuteStart,
    tm.Window30MinuteId,
    tm.Window30MinuteName,
    tm.IsWindow60MinuteStart,
    tm.Window60MinuteId,
    tm.Window60MinuteName

  FROM {catalog}.silver.dim_date AS dd
  LEFT OUTER JOIN {catalog}.silver.dim_mapping_minute AS mm ON dd.DatePK = mm.DateUtcFK
  LEFT OUTER JOIN {catalog}.silver.dim_time_minute AS tm ON mm.WeekTimePK = tm.WeekTimePK
"""
spark.sql(sql).display()

# COMMAND ----------

# DBTITLE 1,vw_dim_date_5_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_5_minute AS
SELECT * FROM {catalog}.silver.vw_dim_date_minute
WHERE IsWindow5MinuteStart = true
"""
spark.sql(sql).display()

# COMMAND ----------

# DBTITLE 1,vw_dim_date_10_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_10_minute AS
SELECT * FROM {catalog}.silver.vw_dim_date_minute
WHERE IsWindow10MinuteStart = true
"""
spark.sql(sql).display()

# COMMAND ----------

# DBTITLE 1,vw_dim_date_15_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_15_minute AS
SELECT * FROM {catalog}.silver.vw_dim_date_minute
WHERE IsWindow15MinuteStart = true
"""
spark.sql(sql).display()

# COMMAND ----------

# DBTITLE 1,vw_dim_date_20_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_20_minute AS
SELECT * FROM {catalog}.silver.vw_dim_date_minute
WHERE IsWindow20MinuteStart = true
"""
spark.sql(sql).display()

# COMMAND ----------

# DBTITLE 1,vw_dim_date_30_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_30_minute AS
SELECT * FROM {catalog}.silver.vw_dim_date_minute
WHERE IsWindow30MinuteStart = true
"""
spark.sql(sql).display()

# COMMAND ----------

# DBTITLE 1,vw_dim_date_60_minute
sql = f"""
CREATE OR REPLACE VIEW {catalog}.silver.vw_dim_date_60_minute AS
SELECT * FROM {catalog}.silver.vw_dim_date_minute
WHERE IsWindow60MinuteStart = true
"""
spark.sql(sql).display()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ----------------- END OF SCRIPTS ---------------
# MAGIC The following cells may contain additional code which can be used for debugging purposes. They won't run automatically, since the notebook will exit after the last command, i.e. `dbutils.notebook.exit()`

# COMMAND ----------

sql = f"SELECT * FROM {date_table}"
spark.sql(sql).display()

# COMMAND ----------

sql = f"SELECT * FROM {time_minute_table}"
spark.sql(sql).display()

# COMMAND ----------

sql = f"SELECT * FROM {mapping_minute_table}"
spark.sql(sql).display()
