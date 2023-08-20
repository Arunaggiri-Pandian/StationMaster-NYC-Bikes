# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import *
## Load the historic bikke data for Lafayette St & E 8 St
bike_trips = spark.read.format("delta")\
                .option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips')\
                    .filter((col("start_station_name")== "Lafayette t & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))\
                    .withColumn("start_date", to_date(col("started_at"), "yyyy-MM-dd"))\
                    .withColumn("end_date", to_date(col("ended_at"), "yyyy-MM-dd"))\
                    .withColumn("start_hour", date_format(col("started_at"), "HH:00"))\
                    .withColumn("end_hour", date_format(col("ended_at"), "HH:00"))\
                    .withColumn("trip_duration", from_unixtime(unix_timestamp("ended_at") - unix_timestamp("started_at"), "HH:mm:ss"))                

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import current_timestamp, date_sub, to_utc_timestamp,current_date
from datetime import datetime

current_datetime = datetime.now().strftime('%Y-%m-%d %H:00:00')

# Read the stream from a source
streamDF = spark.read \
                  .format("delta") \
                  .load("dbfs:/FileStore/tables/bronze_station_status.delta").filter(col("station_id") =="66db65aa-0aca-11e7-82f6-3863bb44ef7c") \
                  .withColumn("last_reported",from_unixtime(col("last_reported")))
most_recent_status_date =  streamDF.filter(col("last_reported") <= current_datetime).select(max(col("last_reported"))).first()[0]
print(most_recent_status_date)
current_bikes_available = streamDF.filter(col("last_reported") == most_recent_status_date).select(col("num_bikes_available")).first()[0]
print(current_bikes_available)


# COMMAND ----------

streamWeather = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta").withColumn("dt_human",from_unixtime(col("dt"))).withColumnRenamed("rain.1h","rain_1h")
most_recent_weather_date = streamWeather.filter(col("dt_human") == current_datetime).select(max(col("dt_human"))).first()[0]
current_temp = streamWeather.filter(col("dt_human") == most_recent_weather_date).select(col("temp")).first()[0]
current_precipitation = streamWeather.filter(col("dt_human") == most_recent_weather_date).select(col("rain_1h")).first()[0]
print(current_temp)
print(current_precipitation)

# COMMAND ----------

from pyspark.sql.functions import col
station_info = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/bronze_station_status.delta").filter(col("station_id")=="66db65aa-0aca-11e7-82f6-3863bb44ef7c")
display(station_info)

# COMMAND ----------

displayHTML(f"""
<p>05.current_timestamp: {currentDateAndTime}</p>
<p>06.production_version: {latest_production_version}</p>
<p>07.staging_version: {latest_staging_version}</p>
<p>08.station_name: {GROUP_STATION_ASSIGNMENT}</p>
<p>09.current_bikes_available: {current_bikes_available}</p>
<p>10.current_temperature: {current_temp}</p>
<p>11.current_precipitation: {current_precipitation}</p>
""")

# COMMAND ----------

!pip install folium

# COMMAND ----------

from datetime import datetime as dt
from datetime import timedelta
import json

dbutils.widgets.removeAll()

from datetime import datetime
currentDateAndTime = datetime.now() 

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

latest_version_info = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])
latest_staging_version = latest_version_info[0].version
latest_version_info = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
latest_production_version = latest_version_info[0].version

dbutils.widgets.text('01.start_date', "2021-10-01")
dbutils.widgets.text('02.end_date', "2021-03-01")
dbutils.widgets.text('03.hours_to_forecast', '4')
dbutils.widgets.text('04.promote_model', 'No')
dbutils.widgets.text('05.current_timestamp', str(currentDateAndTime))
dbutils.widgets.text('06.production_version',str(latest_production_version))
dbutils.widgets.text('07.staging_version',str(latest_staging_version))
dbutils.widgets.text('08.station_name',str(GROUP_STATION_ASSIGNMENT))
dbutils.widgets.text('09.current_bikes_available',str(current_bikes_available))
dbutils.widgets.text('10.current_temperature',str(current_temp))
dbutils.widgets.text('11.current_precipitation',str(current_precipitation))
dbutils.widgets.dropdown("12.Stage", defaultValue="Production", choices=["Production","Staging"])
dbutils.widgets.text("13.capacity",'105')

stage = str(dbutils.widgets.get("12.Stage"))
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
capacity = int(dbutils.widgets.get("13.capacity"))
promote_model = str(dbutils.widgets.get('04.promote_model'))

# COMMAND ----------



# COMMAND ----------

import folium

# Replace 'your_station_id' with the actual station_id you're interested in

# Find the latitude and longitude of your station
station_info = bike_trips.filter(bike_trips.end_station_id == '5788.13').take(1)[0]
station_latitude = station_info.end_lat
station_longitude = station_info.end_lng

# Create a Folium map centered at the station's location with the default "OpenStreetMap" map tile
m = folium.Map(location=[station_latitude, station_longitude], zoom_start=20)

# Create a custom icon for the marker with the color set to 'red'
icon = folium.Icon(icon='bicycle', prefix='fa', color='red')

# Add a marker for your station with the custom icon and popup text
folium.Marker([station_latitude, station_longitude], popup='Station Name - Lafayette St & E 8 St', icon=icon).add_to(m)

# Display the map
m

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/streaming'))

# COMMAND ----------

!pip install fbprophet

# COMMAND ----------

import pandas as pd
from fbprophet import Prophet
from datetime import timedelta
import mlflow
from pyspark.sql.functions import current_date, date_format
# get the current date
current_date_1 = pd.to_datetime('today') - timedelta(hours=4)

# create a range of dates starting from the current date
future_dates = pd.date_range(start=current_date_1, periods=8, freq='30T')

model_staging_uri = "models:/{model_name}/staging".format(model_name=GROUP_MODEL_NAME)
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_staging_uri))
model_staging = mlflow.prophet.load_model(model_staging_uri)

import pandas as pd
from pyspark.sql.functions import desc

from pyspark.sql.functions import current_timestamp, date_sub


# format the timestamp as a string
#current_date = four_hours_ago.strftime("%Y-%m-%d")

# sort the dataframe by ds column
sorted_df = streamWeather.orderBy(desc("dt_human"))


# sort the dataframe by ds column
#sorted_df = streamWeather.filter(date_format("dt_human", "yyyy-MM-dd") == current_date()).orderBy(desc("dt_human"))

# select the 6 most recent rows
recent_rows = sorted_df.limit(100)
recent_rows = recent_rows.select("dt_human","temp","pressure","humidity","wind_speed","pop","rain_1h")
#display(recent_rows)

# convert to pandas dataframe
pandas_df = recent_rows.toPandas()
pandas_df.rename({"dt_human":"date_hour", "temp":"avg_temp","pressure":"avg_pressure","humidity":"avg_humidity","wind_speed":"avg_wind_speed","pop":"avg_pop","rain_1h":"avg_rain_1h"}, axis = 1,inplace = True)
pandas_df["avg_snow_1h"] = 0
pandas_df.fillna(0,inplace = True)

# generate forecasts for the future dates
future_pred = streamDF.filter(col("last_reported") >= (datetime.now() - timedelta(hours  = 4))).select("last_reported","num_bikes_available").toPandas()
future_df = future_pred.rename({"last_reported":"ds"},axis=1)
# create new column in both dataframes with date and hour only
future_df['date_hour'] = pd.to_datetime(future_df['ds']).apply(lambda x: x.replace(minute=0, second=0, microsecond=0))
pandas_df['date_hour'] = pd.to_datetime(pandas_df['date_hour'])
 
future_df = pd.merge( left = future_df, right = pandas_df, on = "date_hour", how = "left")
future_df.fillna(method="ffill",inplace=True)
#.drop(["dt_human"],axis = 1, inplace = True)
#future_df.head()

#print
#future_df.head()
# make predictions using the model and future dataframe
forecast = model_staging.predict(future_df)


# COMMAND ----------

import numpy as np
forecast["num_bikes_available"] = np.nan
forecast.loc[0,"num_bikes_available"] = current_bikes_available + np.round(forecast.loc[0,"yhat"]).astype(int)
for i in range(1, len(forecast)):
    # calculate num_bikes_available for t-1
    prev_num_bikes_available = forecast.loc[i-1, "num_bikes_available"]
    net_change = np.round(forecast.loc[i, "yhat"]).astype(int)
    num_bikes_available_t_1 = prev_num_bikes_available + net_change
    
    # fill in the missing value for num_bikes_available at t
    forecast.loc[i, "num_bikes_available"] = num_bikes_available_t_1
def get_status(num_bikes_available, capacity):
    percent_full = num_bikes_available / capacity
    if percent_full >= 0.5:
        return "Enough Bikes Available"
    elif percent_full >= 0.25:
        return "Some Bikes Available, Restocking should be considered"
    elif percent_full >= 0.1:
        return "Few Bikes Available, Restocking should be immminent"
    elif percent_full > 0:
        return "Very Few Bikes Available, Restock As Soon As Possible"
    else:
        return "No Bikes Available, Restock Now"
capacity = 105
forecast["status_message"] = forecast.apply(lambda x: get_status(x["num_bikes_available"], capacity), axis=1)
future_df['ds'] = pd.to_datetime(future_df['ds'])
forecast = pd.merge(left = forecast, right= future_df, how = "inner", on = "ds")
forecast

# COMMAND ----------



# COMMAND ----------

forecast['residual'] = forecast['num_bikes_available_x'] - forecast['num_bikes_available_y']
gold_df = forecast[["ds","num_bikes_available_x", "num_bikes_available_y", "status_message"]]
#plot the residuals
import plotly.express as px
fig = px.scatter(
    forecast, x='num_bikes_available_x', y='residual',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

# convert the pandas DataFrame to a Spark DataFrame
gold_sdf = spark.createDataFrame(gold_df)
display(gold_sdf)

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "gold_data_final/"

# re-create output directory
dbutils.fs.rm(output_path, recurse = True)
dbutils.fs.mkdirs(output_path)

# define writestream
gold_sdf.write.format("delta").mode("overwrite").save(output_path)

# also recreate delta table
spark.sql("""
drop TABLE if EXISTS g13_db.gold_data_final
""")
gold_sdf.write.format("delta").mode("overwrite").saveAsTable("gold_data_final")

# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
