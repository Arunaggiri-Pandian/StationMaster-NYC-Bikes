# StationMaster-NYC-Bikes

An end-to-end data-intensive application designed to monitor and track the real-time inventory of bicycles and docking spaces within the Citibike Bike Sharing stations across New York City.

## Key Features and Processes:

### **ETL Pipeline:**
- Adheres to the medallion format of data refinement from raw bike and weather sources, transforming them into bronze, silver, and gold levels of data quality.
- Utilizes a spark streaming job to process historical trip and weather data, as well as to ingest new data with defined schemas and their evolution.
- Incorporates optimization strategies like Partitioning and Z-ordering for delta tables.
- Ensures data immutability and secure storage in DBFS under the designated group directory.

### **Exploratory Data Analysis (EDA):**
- Provides insights on monthly and daily trip trends for the assigned station.
- Analyzes the effects of holidays and weather patterns on system usage.
- Maintains SQL tables and views within the specified group's unique DB name.

### **Modeling & ML Ops:**
- Leverages historical data to develop a forecasting model predicting hourly net bike changes at the station.
- Integrates with Databricks model registry for staging and production levels.
- Employs MLflow for managing experiments, model registration, hyperparameter tuning, and artifact storage.

### **Application:**
- Stores inference and monitoring data in a gold data table.
- Features real-time monitoring of staging vs. production models, model versions, current weather conditions, station capacity, and bike availability predictions for the next 4 hours.
- Provides alerts for potential stock-outs or full-station scenarios.
- Incorporates a performance monitoring system for staging and production models through a detailed residual plot, illustrating forecast errors.

This platform aims to provide riders and operators with seamless insights into bike availability, catering to enhanced planning and operational efficiency.
