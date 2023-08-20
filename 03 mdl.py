# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

GROUP_MODEL_NAME

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/silver_data_final_1'))

# COMMAND ----------

silver_data = spark.read.format("delta").option("header","true").load('dbfs:/FileStore/tables/G13/silver_data_final_1')
display(silver_data)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col
silver_data.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in silver_data.columns]).show()

# COMMAND ----------

from pyspark.sql.functions import to_date,col
from pyspark.sql.functions import year, month,concat, concat_ws, date_format,lit
silver_data = silver_data.withColumn("ds", concat(col("start_date_column"), lit(" "), col("start_hour")))
silver_data = silver_data.withColumn("ds", silver_data["ds"].cast("timestamp")).withColumn("y",col("net_change"))
model_df = silver_data.select("ds","avg_temp","avg_pressure","avg_humidity","avg_wind_speed","avg_pop","avg_snow_1h", "avg_rain_1h","num_rides_in","num_rides_out", "y")
display(model_df)
model_df_pandas = model_df.toPandas()
model_df_pandas.drop(["num_rides_in","num_rides_out"],axis = 1, inplace=True)

# COMMAND ----------

model_df_pandas.info()

# COMMAND ----------

!pip install fbprophet

# COMMAND ----------

from fbprophet import Prophet
from fbprophet import Prophet, serialize
from fbprophet.diagnostics import cross_validation, performance_metrics
#from fbprophet.hdays import USFederalHolidayCalendar, NYSHolidayCalendar
import holidays
import pandas as pd

# create a list of all US and NY holidays
us_holidays = holidays.US(years=[2020, 2021, 2022,2023])
ny_holidays = holidays.US(state='NY', years=[2020, 2021, 2022,2023])
all_holidays = us_holidays + ny_holidays

# convert the holidays to a pandas DataFrame
holidays_df = pd.DataFrame(list(all_holidays.items()), columns=['ds', 'holiday'])


# create Prophet object
m = Prophet(yearly_seasonality=True,  # include yearly seasonality
    seasonality_mode='multiplicative',  # use multiplicative seasonality
    daily_seasonality=True,  # do not include daily seasonality
    weekly_seasonality=True, 
    holidays=holidays_df) # do not include weekly seasonality)



m.add_seasonality(name='hourly', period=24, fourier_order=15, prior_scale=0.1)


# add regressor variables
m.add_regressor("avg_temp")
m.add_regressor("avg_pressure")
m.add_regressor("avg_humidity")
m.add_regressor("avg_wind_speed")
m.add_regressor("avg_pop")
m.add_regressor("avg_snow_1h")
m.add_regressor("avg_rain_1h")


model_df_pandas.head()

m.fit(model_df_pandas)

# Cross validation
baseline_model_cv = cross_validation(model=m, initial='420 days', horizon = '30 days', parallel="threads")
baseline_model_cv.head()

# Model performance metrics
baseline_model_p = performance_metrics(baseline_model_cv, rolling_window=1)
baseline_model_p.head()



# COMMAND ----------

import mlflow
import json
import pandas as pd
import numpy as np
import itertools
from prophet import Prophet, serialize
from prophet.diagnostics import cross_validation, performance_metrics

## Helper routine to extract the parameters that were used to train a specific instance of the model
def extract_params(pr_model):
    return {attr: getattr(pr_model, attr) for attr in serialize.SIMPLE_ATTRIBUTES}

# Set up parameter grid
param_grid = {  
    'changepoint_prior_scale': [0.001, 0.01, 0.1, 0.5],
    'seasonality_prior_scale': [0.01, 0.1, 1.0, 10.0],
    'holidays_prior_scale': [0.01, 0.1, 1.0, 10.0],
    'seasonality_mode': ['additive', 'multiplicative'],
    'yearly_seasonality': [True],
    'weekly_seasonality': [True],
    'daily_seasonality': [True],
    'holidays':[holidays_df]
}
mae = []
# Generate all combinations of parameters
all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
#print(all_params)

# Use cross validation to evaluate all parameters
for params in all_params:
    with mlflow.start_run(): 
        # Fit a model using one parameter combination 
        m = Prophet(**params) 

        m.add_seasonality(name='hourly', period=24, fourier_order=15, prior_scale=0.1)
        # add regressor variables
        m.add_regressor("avg_temp")
        m.add_regressor("avg_pressure")
        m.add_regressor("avg_humidity")
        m.add_regressor("avg_wind_speed")
        m.add_regressor("avg_pop")
        m.add_regressor("avg_snow_1h")
        m.add_regressor("avg_rain_1h")

        m.fit(model_df_pandas) 

        # Cross-validation
        df_cv = cross_validation(model=m, initial='420 days', horizon = '30 days', parallel="threads")
        # Model performance
        df_p = performance_metrics(df_cv, rolling_window=1)

        metric_keys = ["mse", "rmse", "mae","mdape","coverage"]
        metrics = {k: df_p[k].mean() for k in metric_keys}
        log_params = extract_params(m)
        log_params.pop("component_modes")

        print(f"Logged Metrics: \n{json.dumps(metrics, indent=2)}")
        print(f"Logged Params: \n{json.dumps(log_params, indent=2)}")

        mlflow.prophet.log_model(m, artifact_path=GROUP_MODEL_NAME)
        mlflow.log_params(log_params)
        mlflow.log_metrics(metrics)
        model_uri = mlflow.get_artifact_uri(GROUP_MODEL_NAME)
        print(f"Model artifact logged to: {model_uri}")

        # Save model performance metrics for this combination of hyper parameters
        mae.append((df_p['mae'].values[0],model_uri))

# COMMAND ----------

tuning_results = pd.DataFrame(all_params)
tuning_results['mae'] = list(zip(*mae))[0]
tuning_results['model']= list(zip(*mae))[1]
best_params = dict(tuning_results.iloc[tuning_results[['mae']].idxmin().values[0]])
print(best_params)
#print(json.dumps(best_params, indent=2))

# COMMAND ----------

loaded_model = mlflow.prophet.load_model(best_params['model'])

future = loaded_model.make_future_dataframe(periods=24, freq='H')
future = pd.merge( left = future, right = model_df_pandas.drop(["y"],axis = 1), on = "ds", how = "left")
future.fillna(method="ffill",inplace=True)
future.head()
forecast = loaded_model.predict(future)

print(f"forecast:\n${forecast.tail(40)}")

# COMMAND ----------

import matplotlib.pyplot as plt
from fbprophet.plot import plot_components

fig = plot_components(loaded_model, forecast)
plt.show()

# COMMAND ----------

results=forecast[['ds','yhat']].join(model_df_pandas, lsuffix='_caller', rsuffix='_other')
results['residual'] = results['yhat'] - results['y']

# COMMAND ----------

#plot the residuals
import plotly.express as px
fig = px.scatter(
    results, x='yhat', y='residual',
    marginal_y='violin',
    trendline='ols',
)
fig.show()

# COMMAND ----------

model_details = mlflow.register_model(model_uri=best_params['model'], name=GROUP_MODEL_NAME)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()

# COMMAND ----------

client.transition_model_version_stage(

  name=model_details.name,

  version=model_details.version,

  stage='Staging',

)

# COMMAND ----------

latest_version_info = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])

latest_staging_version = latest_version_info[0].version

print("The latest staging version of the model '%s' is '%s'." % (GROUP_MODEL_NAME, latest_staging_version))

# COMMAND ----------

#client.transition_model_version_stage(

  #name=model_details.name,

  #version=model_details.version,

  #stage='Production',
#)

# COMMAND ----------

#latest_version_info = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])

#latest_production_version = latest_version_info[0].version

#print("The latest production of the model '%s' is '%s'." % (GROUP_MODEL_NAME, latest_production_version))

# COMMAND ----------


import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
