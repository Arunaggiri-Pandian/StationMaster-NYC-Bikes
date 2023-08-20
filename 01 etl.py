# Databricks notebook source
# MAGIC %md
# MAGIC # installs and include file run

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, TimestampType
import os
from pyspark.sql.functions import to_date,col, from_unixtime, count, coalesce, year, month, concat_ws, date_format, avg

# COMMAND ----------

# MAGIC %run "./includes/includes"

# COMMAND ----------

# MAGIC %md
# MAGIC # BRONZE TABLE
# MAGIC For direct data ingestion, storing historic weather and bike trips data into following delta tables
# MAGIC 1. Historic_weather_data
# MAGIC 2. Historic_Bike_trips
# MAGIC 3. Historic_station_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## historic weather data
# MAGIC
# MAGIC - using batch stream.
# MAGIC - removing duplicates by datetime, renaming columns for ease of access
# MAGIC #### For optimization
# MAGIC - Using repartition to shuffle and partition delta table into equal sizes
# MAGIC - using z-order to index datetime column

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import pyspark.sql.functions as F

# Define the schema for the DataFrame
weather_schema = StructType([
    StructField("dt", IntegerType(), False),
    StructField("temp", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("clouds", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("pop", DoubleType(), True),
    StructField("snow.1h", DoubleType(), True),
    StructField("main", StringType(), True),
    StructField("rain.1h", DoubleType(), True),
])

# Create an empty DataFrame
weather_df = spark.createDataFrame([], weather_schema)

# Get a list of all CSV files in the directory
csv_files = [os.path.join("dbfs:/FileStore/tables/raw/weather/", f.name) for f in dbutils.fs.ls("dbfs:/FileStore/tables/raw/weather/") if f.name.endswith('.csv')]

# Loop through the CSV files and append them to the DataFrame
for file in csv_files:
    temp_df = (spark
                .read
                .option("header", True)
                .schema(weather_schema)
                .csv(file)
               )
    weather_df = weather_df.unionByName(temp_df)

# store as timestamp
weather_df = weather_df.withColumn('dt', F.from_unixtime(F.col('dt')).cast(TimestampType()))

# rename the column to with special charecters
final_w_df = (weather_df
        .withColumnRenamed("rain.1h", "rain_1h")
        .withColumnRenamed("snow.1h", "snow_1h")
        .dropDuplicates(["dt"])
)

# optimiZation by Evenly balancing partition sizes 
coalesce_df = final_w_df.repartition(8)

# COMMAND ----------


# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "historic_weather_data_final_1/"

# re-create output directory
dbutils.fs.rm(output_path, recurse = True)
dbutils.fs.mkdirs(output_path)

# define writestream
coalesce_df.write.format("delta").mode("overwrite").save(output_path)

# also recreate delta table
spark.sql("""
drop TABLE if EXISTS g13_db.historic_weather_data
""")
coalesce_df.write.format("delta").mode("overwrite").saveAsTable("historic_weather_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE g13_db;
# MAGIC
# MAGIC optimize historic_weather_data ZORDER BY dt;
# MAGIC
# MAGIC DESCRIBE DETAIL historic_weather_data;

# COMMAND ----------

df = spark.read.format("delta").load(G13_BRONZE_WEATHER)
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## historic bike trips
# MAGIC
# MAGIC - using batch stream.
# MAGIC #### For optimization
# MAGIC - Using repartition to shuffle and partition delta table into equal sizes
# MAGIC - using z-order to index ride_id column

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Get a list of all CSV files in the directory
csv_files = [os.path.join(BIKE_TRIP_DATA_PATH, f.name) for f in dbutils.fs.ls(BIKE_TRIP_DATA_PATH) if f.name.endswith('.csv')]

# Define the schema for the DataFrame
bike_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("member_casual", IntegerType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("start_lat", DoubleType(), True),
])

# Create an empty DataFrame
trip_df = spark.createDataFrame([], bike_schema)

# Loop through the CSV files and append them to the DataFrame
for file in csv_files:
    temp_df = spark.read.format('csv').option('header', 'true').csv(file)
    trip_df = trip_df.unionByName(temp_df)


# optimiZation by Evenly balancing partition sizes 
coalesce_df = trip_df.repartition(8)

output_path = GROUP_DATA_PATH + "historic_bike_trips_final_1/"

# re-create output directory
dbutils.fs.rm(output_path, recurse = True)
dbutils.fs.mkdirs(output_path)

coalesce_df.write.format("delta").mode("overwrite").save(output_path)

# also recreate delta table
spark.sql("""
drop TABLE g13_db.historic_bike_trips
""")
coalesce_df.write.format("delta").mode("overwrite").saveAsTable("historic_bike_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE g13_db;
# MAGIC
# MAGIC optimize historic_bike_trips ZORDER BY ride_id;
# MAGIC
# MAGIC -- DESCRIBE DETAIL historic_bike_trips;
# MAGIC -- select * from historic_weather_data limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## stream live data
# MAGIC
# MAGIC - storing raw streaming data as is (type cast is not possible at this stage)

# COMMAND ----------

# MAGIC %md
# MAGIC #### bike status

# COMMAND ----------

# create readstream
streaming_status_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_STATION_STATUS_PATH)
)

streaming_status_df = (
    streaming_status_df
    .withColumn('last_reported', F.from_unixtime(F.col('last_reported')).cast(TimestampType()))
    )

# COMMAND ----------

# output folder
output_path = GROUP_DATA_PATH + "streaming/bike_staus/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/bike_staus"

#  re-create output directory
# dbutils.fs.rm(checkpoint_path, recurse = True)
# dbutils.fs.mkdirs(checkpoint_path)
# dbutils.fs.mkdirs(output_path)

# # re do delta table
# spark.sql("""
# DROP TABLE IF EXISTS g13_db.streaming_bike_status
# """)

# Perform any necessary transformations, such as dropping duplicates
# deduplicated_stream = streaming_status_df.dropDuplicates(["last_reported"])

status_query = (
    streaming_status_df
    .writeStream
    .outputMode("append")
    .format("delta")
    .queryName("status_traffic")
    .trigger(processingTime='30 minutes')
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
    # .awaitTermination()
)

# COMMAND ----------

bike_status_df = (
    spark
    .read.format("delta").load(GROUP_DATA_PATH + "streaming/bike_staus/output")
    .filter(F.col("station_id") == "daefc84c-1b16-4220-8e1f-10ea4866fdc7")
    .sort("last_reported")
    ) 
# 2023-05-06T17:31:45.000+0000

display(bike_status_df.sort(F.desc("last_reported")).limit(3))

# COMMAND ----------

total_rows = bike_status_df.count()

# Count the number of distinct rows in the DataFrame based on the 'value' column
distinct_rows = bike_status_df.select("last_reported").distinct().count()

# Check if the column has duplicates
has_duplicates = total_rows != distinct_rows
display(has_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC #### nyc weather

# COMMAND ----------

# create readstream
streaming_weather_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_NYC_WEATHER_PATH)
)

streaming_weather_df = streaming_weather_df.withColumn('dt', F.from_unixtime(F.col('dt')).cast(TimestampType()))

# COMMAND ----------

# output folder
output_path = GROUP_DATA_PATH + "streaming/nyc_weather/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/nyc_weather"

#  re-create output directory
# dbutils.fs.rm(checkpoint_path, recurse = True)
# dbutils.fs.mkdirs(checkpoint_path)
# dbutils.fs.mkdirs(output_path)

# # re do delta table
# spark.sql("""
# DROP TABLE IF EXISTS g13_db.streaming_nyc_weather
# """)

# Perform any necessary transformations, such as dropping duplicates
# deduplicated_stream = streaming_weather_df.dropDuplicates(["dt"])

weather_query = (
    streaming_weather_df
    .writeStream
    .outputMode("append")
    .format("delta")
    .queryName("nyc_traffic")
    .trigger(processingTime='30 minutes')
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
    # .awaitTermination()
)

# COMMAND ----------

weather_query.status

# COMMAND ----------

# MAGIC %md
# MAGIC # silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ## historic data

# COMMAND ----------

from_path = G13_BRONZE_BIKE_TRIP

historic_bike_trips_from  = (
    spark
    .read
    .format("delta")
    .load(from_path)
    .filter(col("start_station_name") == GROUP_STATION_ASSIGNMENT)
    .withColumn("start_date_column", to_date(col('started_at')))
    .withColumn("year_month", year(col('start_date_column'))*100 + month(col('start_date_column')))
    .withColumn("day_of_the_week", date_format(col("started_at"),"E"))
    .withColumn("start_hour",date_format(col("started_at"),"HH:00")))

historic_bike_trips_to = (
    spark.read.format("delta")
    .load(from_path)
    .filter(col("end_station_name") == GROUP_STATION_ASSIGNMENT)
    .withColumn("end_date_column", to_date(col('ended_at')))
    .withColumn("year_month", year(col('end_date_column')) * 100 + month(col('end_date_column')))
    .withColumn("day_of_the_week", date_format(col("ended_at"), "E"))
    .withColumn("end_hour", date_format(col('ended_at'), "HH:00"))
)

bronze_hist_weather = G13_BRONZE_WEATHER

historic_weather = (
    spark.read.format("delta")
    .load(bronze_hist_weather)
    # .withColumn('human_timestamp', from_unixtime('dt'))
    .withColumn("weather_hour", date_format(col('dt'), "HH:00"))
    .withColumn("weather_date", to_date("dt"))
)

display(historic_weather)

# COMMAND ----------

historic_bike_trips_to_agg = (
    historic_bike_trips_to
    .groupBy("end_date_column", "end_hour")
    .agg(count("ride_id").alias("num_rides_in"))
    .orderBy("end_date_column", "end_hour")
)

display(historic_bike_trips_to_agg)

# COMMAND ----------

historic_bike_trips_from_agg = (
    historic_bike_trips_from
    .groupBy("start_date_column", "start_hour")
    .agg(count("ride_id").alias("num_rides_out"))
    .orderBy("start_date_column", "start_hour")
)

display(historic_bike_trips_from_agg)

# COMMAND ----------


historic_weather_agg = (
    historic_weather
    .groupBy("weather_date", "weather_hour")
    .agg(
        avg("temp").alias("avg_temp"),
        avg("feels_like").alias("avg_feels_like"),
        avg("pressure").alias("avg_pressure"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        avg("pop").alias("avg_pop"),
        avg("snow_1h").alias("avg_snow_1h"),
        avg("rain_1h").alias("avg_rain_1h")
    )
    .orderBy("weather_date", "weather_hour")
)


display(historic_weather_agg)

# COMMAND ----------

trips_joined_df = historic_bike_trips_to_agg.join(historic_bike_trips_from_agg,
                             (col("end_date_column") == col("start_date_column"))
                             & (col("end_hour") == col("start_hour")),
                             "outer")
display(trips_joined_df)

# COMMAND ----------

# Replace null values in "num_rides_in" and "num_rides_out" columns with 0
trips_joined_df = trips_joined_df.fillna({'num_rides_in': 0, 'num_rides_out': 0})

# Replace null values in "end_date_column" and "end_hour" columns with values from "start_date_column" and "start_hour", respectively
trips_joined_df = trips_joined_df.withColumn('end_date_column', coalesce('end_date_column', 'start_date_column'))
trips_joined_df = trips_joined_df.withColumn('end_hour', coalesce('end_hour', 'start_hour'))

# Replace null values in "start_date_column" and "start_hour" columns with values from "end_date_column" and "end_hour", respectively
trips_joined_df = trips_joined_df.withColumn('start_date_column', coalesce('start_date_column', 'end_date_column'))
trips_joined_df = trips_joined_df.withColumn('start_hour', coalesce('start_hour', 'end_hour'))
display(trips_joined_df)


# COMMAND ----------

trips_joined_df.columns

# COMMAND ----------

from pyspark.sql.functions import sum, when, col

trips_joined_df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in trips_joined_df.columns]).show()

# COMMAND ----------

trips_joined_feat = trips_joined_df.select("start_date_column","start_hour","num_rides_in","num_rides_out").withColumn("net_change",col("num_rides_in")-col("num_rides_out"))
display(trips_joined_feat)

# COMMAND ----------

trips_joined_feat = (
    trips_joined_feat
    .join(
        historic_weather_agg,
        (col("start_date_column") == col("weather_date"))
        & (col("start_hour") == col("weather_hour")),
        "left"
    )
    .dropna()
)

# display(trips_joined_feat)

# COMMAND ----------

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "silver_data_final_1/"

# re-create output directory
dbutils.fs.rm(output_path, recurse = True)
dbutils.fs.mkdirs(output_path)

# define writestream
trips_joined_feat.write.format("delta").mode("overwrite").save(output_path)

# also recreate delta table
spark.sql("""
drop TABLE if EXISTS g13_db.silver_weather_trip
""")
trips_joined_feat.write.format("delta").mode("overwrite").saveAsTable("silver_weather_trip")

# COMMAND ----------

# MAGIC %md
# MAGIC ## rough work

# COMMAND ----------

df = spark.read.format("delta").load(G13_SILVER_BIKE_WEATHER)
display(df.limit(9))

# COMMAND ----------


