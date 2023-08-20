# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

historic_bike_trips_df = spark.read.format('delta').load(G13_BRONZE_BIKE_TRIP)

# COMMAND ----------

from pyspark.sql import functions as F

# Extract the year and month from the 'started_at' column
lafayette_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).withColumn("year", F.year("started_at")).withColumn("month", F.month("started_at"))

# Group by year and month, and count the trips
monthly_trips = lafayette_df.groupBy("year", "month").agg(
    F.count("ride_id").alias("trip_count")
)

# Order the result by year and month
monthly_trips = monthly_trips.orderBy("year", "month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <B> What are the monthly trip trends for your assigned station?</b>

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Convert the Spark DataFrame to a Pandas DataFrame
monthly_trips_pd = monthly_trips.toPandas()

# Create a new column with a string representation of year and month
monthly_trips_pd['year_month'] = monthly_trips_pd['year'].astype(str) + '-' + monthly_trips_pd['month'].astype(str).str.zfill(2)

# Set the figure size and title
plt.figure(figsize=(15, 6))
plt.title("Monthly Trip Trends for the Station - Lafayette St & E 8 St", fontsize=18)

# Create a bar chart with a custom color
plt.bar(monthly_trips_pd['year_month'], monthly_trips_pd['trip_count'], color='cornflowerblue')

# Set the x and y axis labels
plt.xlabel("Year-Month", fontsize=14)
plt.ylabel("Trip Count", fontsize=14)

# Rotate the x-axis labels for better readability and adjust font size
plt.xticks(rotation=45, fontsize=12)
plt.yticks(fontsize=12)

# Add gridlines to the background
plt.grid(axis='y', linestyle='--', alpha=0.5)

# Display the chart
plt.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Extract the date from the 'started_at' column
lafayette_df = historic_bike_trips_df.withColumn("date", F.to_date("started_at"))

# Group by date and count the trips
daily_trips = lafayette_df.groupBy("date").agg(
    F.count("ride_id").alias("trip_count")
)

# Order the result by date
daily_trips = daily_trips.orderBy("date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <B> What are the daily trip trends for your given station?</b>

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Set the Seaborn style
sns.set(style="whitegrid")

# Convert the Spark DataFrame to a Pandas DataFrame
daily_trips_pd = daily_trips.toPandas()

# Set the figure size and title
plt.figure(figsize=(18, 7))
plt.title("Daily Trip Trends for the Station - Lafayette St & E 8 St")

# Create a line chart with custom color, line style, and markers
plt.plot(daily_trips_pd['date'], daily_trips_pd['trip_count'], color="royalblue", linestyle="-")

# Set the x and y axis labels
plt.xlabel("Date")
plt.ylabel("Trip Count")

# Rotate the x-axis labels
plt.xticks(rotation=90)

# Customize the grid and spines
sns.despine(left=True, bottom=True)
plt.grid(axis="both", linestyle="--", alpha=0.7)

# Display the chart
plt.show()

# COMMAND ----------

# This code snippet includes major federal holidays observed in Lafayette, New York, and weekends for the specified date range

from datetime import datetime, timedelta

# Function to generate weekend dates
def generate_weekend_dates(start_date, end_date):
    weekends = []
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    while start_date <= end_date:
        if start_date.weekday() >= 5:  # Saturday and Sunday have weekdays 5 and 6
            weekends.append(str(start_date.date()))
        start_date += timedelta(days=1)
    
    return weekends

# Generate weekend dates for the dataset's date range
weekends = generate_weekend_dates("2021-01-01", "2023-03-31")

holidays = [
    "2021-01-01",  # New Year's Day
    "2021-01-18",  # Martin Luther King Jr. Day
    "2021-02-15",  # Presidents' Day
    "2021-05-31",  # Memorial Day
    "2021-07-04",  # Independence Day
    "2021-09-06",  # Labor Day
    "2021-10-11",  # Columbus Day
    "2021-11-11",  # Veterans Day
    "2021-11-25",  # Thanksgiving Day
    "2021-12-25",  # Christmas Day
    "2022-01-01",  # New Year's Day
    "2022-01-17",  # Martin Luther King Jr. Day
    "2022-02-21",  # Presidents' Day
    "2022-05-30",  # Memorial Day
    "2022-07-04",  # Independence Day
    "2022-09-05",  # Labor Day
    "2022-10-10",  # Columbus Day
    "2022-11-11",  # Veterans Day
    "2022-11-24",  # Thanksgiving Day
    "2022-12-25",  # Christmas Day
    "2023-01-01",  # New Year's Day
    "2023-01-16",  # Martin Luther King Jr. Day
    "2023-02-20",  # Presidents' Day
    "2023-05-29",  # Memorial Day
    "2023-07-04",  # Independence Day
    "2023-09-04",  # Labor Day
    "2023-10-09",  # Columbus Day
    "2023-11-11",  # Veterans Day
    "2023-11-23",  # Thanksgiving Day
    "2023-12-25",  # Christmas Day
] + weekends

# Convert holiday dates to a set for faster lookups
holiday_set = set(holidays)

# COMMAND ----------

# Create a user-defined function (UDF) to check if a date is a holiday
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

@udf(returnType=BooleanType())
def is_holiday(date):
    return str(date) in holiday_set

# COMMAND ----------

# Add a new column to the daily_trips DataFrame to indicate if the date is a holiday or not
daily_trips = daily_trips.withColumn("is_holiday", is_holiday("date"))

# COMMAND ----------

# Calculate the average trip counts for holidays and non-holidays
avg_trips = daily_trips.groupBy("is_holiday").agg(
    F.avg("trip_count").alias("average_trip_count")
)

# COMMAND ----------

avg_trips_pd = avg_trips.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <b> How does a holiday affect the daily (non-holiday) system use trend?

# COMMAND ----------

import matplotlib.pyplot as plt

# Set the plot size
plt.figure(figsize=(10, 6))

# Create a bar plot
plt.bar(avg_trips_pd['is_holiday'], avg_trips_pd['average_trip_count'], color=['blue', 'orange'])

# Set the plot title, x-label, and y-label
plt.title("Average Daily Trips by Holiday Status")
plt.xlabel("Is Holiday?")
plt.ylabel("Average Trip Count")

# Customize the x-axis tick labels
plt.xticks([0, 1], ['Non-Holiday', 'Holiday'])

# Display the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <B> During holidays, the average trip count tends to decrease a little!

# COMMAND ----------

# Read the weather data
nyc_weather_df = spark.read.format('delta').load(G13_BRONZE_WEATHER)

# COMMAND ----------

from pyspark.sql.functions import to_date

# Convert the 'dt' column to a date type column
nyc_weather_df = nyc_weather_df.withColumn("date", to_date("dt"))

# COMMAND ----------

# Aggregate weather data by date
weather_daily_agg = nyc_weather_df.groupBy("date").agg(
    F.avg("temp").alias("avg_temperature"),
    F.avg("rain_1h").alias("avg_precipitation"),
    F.avg("wind_speed").alias("avg_wind_speed")
)

# COMMAND ----------

# Join the aggregated weather data with the daily_trips DataFrame on the date (or hour) column:
daily_trips_weather = daily_trips.join(weather_daily_agg, on="date")

# COMMAND ----------

# Analyze the correlation between the daily/hourly trip counts and weather variables using the corr() function:
temp_corr = daily_trips_weather.stat.corr("trip_count", "avg_temperature")
precip_corr = daily_trips_weather.stat.corr("trip_count", "avg_precipitation")
wind_speed_corr = daily_trips_weather.stat.corr("trip_count", "avg_wind_speed")

print(f"Correlation between trip_count and avg_temperature: {temp_corr}")
print(f"Correlation between trip_count and avg_precipitation: {precip_corr}")
print(f"Correlation between trip_count and avg_wind_speed: {wind_speed_corr}")

# COMMAND ----------

# Visualize the relationships between trip counts and weather variables using scatter plots. First, convert the daily_trips_weather DataFrame to a Pandas DataFrame:
daily_trips_weather_pd = daily_trips_weather.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt

# Create a 3x1 subplot grid
fig, ax = plt.subplots(3, 1, figsize=(12, 18))

# Create scatter plots for each weather variable
ax[0].scatter(daily_trips_weather_pd['avg_temperature'], daily_trips_weather_pd['trip_count'], alpha=0.7, c='orange', marker='o')
ax[0].set_title("Daily Trips vs. Average Temperature")
ax[0].set_xlabel("Average Temperature")
ax[0].set_ylabel("Daily Trips")
ax[0].grid()

ax[1].scatter(daily_trips_weather_pd['avg_precipitation'], daily_trips_weather_pd['trip_count'], alpha=0.7, c='maroon', marker='o')
ax[1].set_title("Daily Trips vs. Average Precipitation")
ax[1].set_xlabel("Average Precipitation")
ax[1].set_ylabel("Daily Trips")
ax[1].grid()

ax[2].scatter(daily_trips_weather_pd['avg_wind_speed'], daily_trips_weather_pd['trip_count'], alpha=0.7, c='purple', marker='o')
ax[2].set_title("Daily Trips vs. Average Wind Speed")
ax[2].set_xlabel("Average Wind Speed")
ax[2].set_ylabel("Daily Trips")
ax[2].grid()

# Display the plots
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Based on the correlation values, we can make the following observations:
# MAGIC
# MAGIC <b>There's a strong positive correlation between the trip count and average temperature (0.84). This suggests that as the temperature increases, the number of trips taken also tends to increase. People may be more likely to use the bike-sharing system when the weather is warmer.
# MAGIC
# MAGIC <b>There's a weak negative correlation between the trip count and average precipitation (-0.23). This indicates that as precipitation increases, the number of trips taken may decrease slightly. Rain or snow could make it less desirable for people to use bikes.
# MAGIC
# MAGIC <b>There's a weak negative correlation between the trip count and average wind speed (-0.45). This suggests that as the wind speed increases, the number of trips taken may decrease a little. High winds might make biking less enjoyable or more difficult.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional EDA

# COMMAND ----------

bike_station = f"dbfs:/FileStore/tables/G13/bronze/bike-station-info/"

# COMMAND ----------

bike_station_df = spark.read.format('delta').load(bike_station)

# COMMAND ----------

bike_status = f"dbfs:/FileStore/tables/G13/bronze/bike-status/"
bike_status_df = spark.read.format('delta').load(bike_status)

# COMMAND ----------

# First, join the bike_trip_df with bike_station_df on start_station and station_id columns:
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <b>What is the distribution of station status (active, inactive)?

# COMMAND ----------

station_status_stats = bike_status_df.groupBy("station_status").agg(count("*").alias("count")).orderBy("station_status")
station_status_list = [row.asDict() for row in station_status_stats.collect()]

# COMMAND ----------

# Convert station_status_list to a DataFrame
station_status_df = pd.DataFrame(station_status_list)

sns.set(style="whitegrid")
plt.figure(figsize=(10, 6))
sns.barplot(x="station_status", y="count", data=station_status_df)
plt.title("Station Status Counts")
plt.xlabel("Station Status")
plt.ylabel("Number of Stations")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Based on the output, we can observe that the majority of the stations are in an 'active' status, while a smaller number of stations are 'out_of_service'. This information is useful as it provides an understanding of the proportion of stations that are currently operational and can be used for forecasting net bike change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <b>How do bike trips vary across different types of bikes (electric bikes, regular bikes, etc.)?

# COMMAND ----------

bike_type_stats = historic_bike_trips_df.groupBy("rideable_type").agg(count("*").alias("count")).orderBy("rideable_type")
bike_type_list = [row.asDict() for row in bike_type_stats.collect()]

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert bike_type_list to a DataFrame
bike_type_df = pd.DataFrame(bike_type_list)

sns.set(style="whitegrid")
plt.figure(figsize=(8, 6))
sns.barplot(x="rideable_type", y="count", data=bike_type_df)
plt.title("Bike Type Distribution")
plt.xlabel("Bike Type")
plt.ylabel("Number of Trips")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <b>The output shows the distribution of bike trips across different types of bikes:
# MAGIC
# MAGIC <b>Classic Bike: 29,585,988 trips  
# MAGIC <b>Docked Bike: 306,185 trips  
# MAGIC <b>Electric Bike: 10,506,882 trips   
# MAGIC
# MAGIC <b>Classic bikes are the most frequently used, with a significantly higher number of trips compared to the other types. Electric bikes are the second most popular, while docked bikes have the least number of trips.
# MAGIC
# MAGIC <b>Understanding the distribution of bike trips across different types of bikes can help us determine the influence of bike types on net bike change at the station level.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <b>How do bike trips vary across different user types (members and casual users)?

# COMMAND ----------

user_type_stats = historic_bike_trips_df.groupBy("member_casual").agg(count("*").alias("count")).orderBy("member_casual")

# To output the result as a list of dictionaries
user_type_list = [row.asDict() for row in user_type_stats.collect()]

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert user_type_list to a DataFrame
user_type_df = pd.DataFrame(user_type_list)

sns.set(style="whitegrid")
plt.figure(figsize=(8, 6))
sns.barplot(x="member_casual", y="count", data=user_type_df)
plt.title("User Type Distribution")
plt.xlabel("User Type")
plt.ylabel("Number of Trips")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>The output shows the distribution of bike trips across different user types:
# MAGIC  
# MAGIC <b>Casual users: 8,262,716 trips  
# MAGIC <b>Members: 32,136,339 trips  
# MAGIC <b>Members account for a significantly larger number of trips compared to casual users.  
# MAGIC
# MAGIC <b>Understanding the distribution of bike trips across different user types can help us determine the influence of user types on net bike change at the station level.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question:
# MAGIC <b>What are the peak hours for bike usage, and how does this differ between weekdays and weekends?

# COMMAND ----------

from pyspark.sql.functions import hour, date_format

# Extract hour and day of week from the started_at column
historic_bike_trips_df = historic_bike_trips_df.withColumn("hour", hour("started_at"))
historic_bike_trips_df = historic_bike_trips_df.withColumn("day_of_week", date_format("started_at", "E"))

# Group by hour and day of the week, and count the number of trips
hour_day_stats = historic_bike_trips_df.groupBy("hour", "day_of_week").agg(count("*").alias("count")).orderBy("hour", "day_of_week")

# Collect the results as a list of dictionaries
hour_day_list = [row.asDict() for row in hour_day_stats.collect()]

# COMMAND ----------

# Convert hour_day_list to a DataFrame
hour_day_df = pd.DataFrame(hour_day_list)

# Create a heatmap to visualize bike usage by hour and day of the week
plt.figure(figsize=(20, 14))
sns.heatmap(hour_day_df.pivot("day_of_week", "hour", "count"), cmap="YlGnBu", annot=True, fmt="d")
plt.title("Bike Usage by Hour and Day of the Week")
plt.xlabel("Hour")
plt.ylabel("Day of the Week")
plt.show()

# COMMAND ----------

# Separate weekdays and weekends into different columns
hour_day_df["weekday_count"] = hour_day_df.apply(lambda row: row["count"] if row["day_of_week"] not in ["Sat", "Sun"] else 0, axis=1)
hour_day_df["weekend_count"] = hour_day_df.apply(lambda row: row["count"] if row["day_of_week"] in ["Sat", "Sun"] else 0, axis=1)

# Group by hour and sum the counts
hour_day_grouped = hour_day_df.groupby("hour").agg({"weekday_count": "sum", "weekend_count": "sum"}).reset_index()

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))
plt.plot(hour_day_grouped["hour"], hour_day_grouped["weekday_count"], label="Weekdays", marker="o")
plt.plot(hour_day_grouped["hour"], hour_day_grouped["weekend_count"], label="Weekends", marker="o")
plt.title("Bike Usage by Hour (Weekdays vs Weekends)")
plt.xlabel("Hour")
plt.ylabel("Number of Trips")
plt.xticks(range(0, 24))
plt.legend()
plt.grid()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Bike usage is generally higher on weekdays than weekends for most hours of the day.  
# MAGIC
# MAGIC <b>On weekdays, there are two distinct peak hours: one in the morning (around 8 am) and another in the evening (around 5 pm to 6 pm). This is likely due to people commuting to and from work.  
# MAGIC
# MAGIC <b>On weekends, the usage pattern is different. The peak hours are around midday, from 11 am to 3 pm. This may be because people are more likely to use bikes for leisure or running errands on weekends.  
# MAGIC
# MAGIC <b>Bike usage decreases significantly during late-night hours (from around 11 pm to 5 am) on both weekdays and weekends.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question
# MAGIC <b>What are the most frequent start and end stations for trips that are to or from the given station?

# COMMAND ----------

from pyspark.sql.functions import desc

GROUP_STATION_ASSIGNMENT = 'Lafayette St & E 8 St'

# Filter trips starting at the given station and ending at a different station
start_station_df = historic_bike_trips_df.filter((F.col("start_station_name") == GROUP_STATION_ASSIGNMENT) & (F.col("start_station_name") != F.col("end_station_name")))

# Group by end_station_name and count the number of trips
start_station_grouped = start_station_df.groupBy("end_station_name").agg(F.count("ride_id").alias("trip_count"))

# Sort in descending order based on trip_count and take the first row
most_frequent_end_station = start_station_grouped.sort(desc("trip_count")).first()

# Filter trips ending at the given station and starting at a different station
end_station_df = historic_bike_trips_df.filter((F.col("end_station_name") == GROUP_STATION_ASSIGNMENT) & (F.col("start_station_name") != F.col("end_station_name")))

# Group by start_station_name and count the number of trips
end_station_grouped = end_station_df.groupBy("start_station_name").agg(F.count("ride_id").alias("trip_count"))

# Sort in descending order based on trip_count and take the first row
most_frequent_start_station = end_station_grouped.sort(desc("trip_count")).first()

print(f"Most frequent start station: {most_frequent_start_station['start_station_name']} with {most_frequent_start_station['trip_count']} trips")
print(f"Most frequent end station: {most_frequent_end_station['end_station_name']} with {most_frequent_end_station['trip_count']} trips")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <b>Most frequent start station: Cleveland Pl & Spring St with 2099 trips  
# MAGIC <b>Most frequent end station: 4 Ave & E 12 St with 2061 trips

# COMMAND ----------

# bar chart showing the trip count for the top N stations
import matplotlib.pyplot as plt

# Number of top stations to display
top_n = 10

# Get the top N start stations
top_start_stations = end_station_grouped.sort(desc("trip_count")).take(top_n)

# Get the top N end stations
top_end_stations = start_station_grouped.sort(desc("trip_count")).take(top_n)

# Extract the station names and trip counts for start stations
start_station_names = [row['start_station_name'] for row in top_start_stations]
start_station_trip_counts = [row['trip_count'] for row in top_start_stations]

# Extract the station names and trip counts for end stations
end_station_names = [row['end_station_name'] for row in top_end_stations]
end_station_trip_counts = [row['trip_count'] for row in top_end_stations]

# Create a bar chart for the top N start stations
plt.figure(figsize=(12, 6))
plt.bar(start_station_names, start_station_trip_counts)
plt.xlabel('Start Stations')
plt.ylabel('Trip Count')
plt.title(f'Top {top_n} Start Stations for Trips from {GROUP_STATION_ASSIGNMENT}')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# Create a bar chart for the top N end stations
plt.figure(figsize=(12, 6))
plt.bar(end_station_names, end_station_trip_counts)
plt.xlabel('End Stations')
plt.ylabel('Trip Count')
plt.title(f'Top {top_n} End Stations for Trips to {GROUP_STATION_ASSIGNMENT}')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Knowing the most frequent start and end stations for trips involving our assigned station can help in making informed decisions related to bike-sharing operations, marketing strategies, and infrastructure planning. Here are some possible actions we can take based on this information:
# MAGIC
# MAGIC <b>Optimize bike availability:</b>   
# MAGIC Ensure that there are enough bikes available at the most frequent start station (Cleveland Pl & Spring St) and enough empty docks at the most frequent end station (4 Ave & E12 St) during peak hours. This can be done by redistributing bikes or adjusting the dock capacities.
# MAGIC
# MAGIC <b>Infrastructure improvements:</b>  
# MAGIC If the demand for bikes at these popular stations is consistently high, consider expanding the existing stations or adding new stations nearby to cater to the demand and reduce congestion.
# MAGIC
# MAGIC <b>Targeted marketing:</b>   
# MAGIC Promote membership plans or special offers to users who frequently start or end their trips at these popular stations. This can help in attracting new customers and retaining existing ones.
# MAGIC
# MAGIC <b>Collaborate with local businesses:</b>   
# MAGIC Partner with businesses near the most frequent start and end stations to offer promotions, discounts, or events that encourage more people to use the bike-sharing service.
# MAGIC
# MAGIC <b>Analyze trip patterns:</b> 
# MAGIC Further analyze the trip data to identify trends or patterns related to these popular stations, such as the time of day when they are most frequently used or if there are any seasonal variations. This information can be used to fine-tune operations, marketing strategies, and customer service.
