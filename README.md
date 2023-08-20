# StationMaster-NYC-Bikes

An end-to-end data-intensive application designed to monitor and track the real-time inventory of bicycles and docking spaces within the Citibike Bike Sharing stations across New York City.

## Tools Used:

### **Apache Spark**:
Apache Spark is a unified analytics engine for big data processing, with built-in modules for streaming, SQL, machine learning, and graph processing. In this project, Spark's streaming capabilities are harnessed to process real-time bike and weather data efficiently.

### **Delta Tables**:
Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. This project employs delta tables to manage the bronze, silver, and gold levels of data quality and refinement, ensuring reliability and performance.

### **Databricks**:
Databricks is a platform that provides a unified analytics platform and is a commercial version of Apache Spark. The model registry of Databricks is used to manage the different stages of machine learning models (staging and production) and ensure seamless deployment.

### **MLflow**:
MLflow is an open-source platform to manage the machine learning lifecycle, including experimentation, reproducibility, and deployment. In this project, MLflow manages experiments, model registration, hyperparameter tuning, and artifact storage, ensuring traceability and version control.

### **DBFS (Databricks File System)**:
DBFS is a distributed file system installed on Databricks clusters. The data of this project, given its volume and structure, is securely stored in DBFS, ensuring scalability and high availability.

### **SQL**:
Structured Query Language (SQL) is used for managing and querying large datasets efficiently. In the context of this project, SQL aids in exploratory data analysis, providing insights on trip trends, effects of holidays, and weather patterns on system usage.

By utilizing this combination of cutting-edge tools and platforms, the project ensures robustness, scalability, and high performance in monitoring and forecasting bike availability in NYC's Citibike stations.

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
