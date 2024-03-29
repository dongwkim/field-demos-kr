# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow==1.29.0 cffi==1.14.6 cloudpickle==2.0.0 databricks-automl-runtime==0.2.11 defusedxml==0.7.1 holidays==0.15 lightgbm==3.3.2 matplotlib==3.4.3 pandas==1.3.4 psutil==5.8.0 scikit-learn==0.24.2 typing-extensions==3.10.0.2

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to load the wind turbine prediction model as a spark udf and save it as a SQL function
# MAGIC  
# MAGIC Make sure you add this notebook in your DLT job to have access to the `get_turbine_status` function. (Currently mixing python in a SQL DLT notebook won't run the python)
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_dlt_udf&dt=MANUFACTURING_WIND_TURBINE">

# COMMAND ----------

# MAGIC %python
# MAGIC import mlflow
# MAGIC #                                                                              Stage/version  
# MAGIC #                                                                 Model name         |        
# MAGIC #                                                                     |              |        
# MAGIC predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/dbdemos_turbine_maintenance/Production", "string")
# MAGIC spark.udf.register("predict_maintenance", predict_churn_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC 
# MAGIC This notebook must be included in your DLT "libraries" parameter:
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC     "id": "95f28631-1884-425e-af69-05c3f397dd90",
# MAGIC     "name": "xxxx",
# MAGIC     "storage": "/demos/dlt/lakehouse_iot_wind_turbine/xxxxx",
# MAGIC     "configuration": {
# MAGIC         "pipelines.useV2DetailsPage": "true"
# MAGIC     },
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.1-DLT-Wind-Turbine-SQL"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.2-DLT-Wind-Turbine-SQL-UDF"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "target": "retail_lakehouse_churn_xxxx",
# MAGIC     "continuous": false,
# MAGIC     "development": false
# MAGIC }
# MAGIC ```
