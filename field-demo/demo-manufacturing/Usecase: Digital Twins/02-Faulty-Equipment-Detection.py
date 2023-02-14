# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Vibration Fault Detection
# MAGIC 
# MAGIC Our first use-case will be fault detection in the Mixing step.  
# MAGIC Abnormal mixer vibrations can quickly lead to plant outages, having very high impact on revenue.  
# MAGIC To solve this issue, the business asked us to trigger alarms when anomalies are detected in the production line.
# MAGIC 
# MAGIC This is the flow we'll be implementing:  
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-flow-0.png" width="1000px" />
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fdigital_twins%2Fnotebook_fault&dt=MANUFACTURING_DIGITAL_TWINS">
# MAGIC <!-- [metadata={"description":"Fault detection with Digital Twins in real time. Build ML model to detect Mixing Station issue and trigger predictive maintenance.",
# MAGIC  "authors":["pawarit.laosunthara@databricks.com"]}] -->

# COMMAND ----------

# DBTITLE 1,Init data & resources. Hide this cell results
# MAGIC %run ./_resources/02-IOT-Data-Generator $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### 1 Ingesting our plant sensor information - Streaming
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-flow-small-1.png" width="700px" style="float: right" />
# MAGIC 
# MAGIC 
# MAGIC #### Scalable exactly-once data ingestion with Auto Loader
# MAGIC 
# MAGIC In this example, we'll use Databricks Auto Loader to process files uploaded by IoT systems. We could also process streams from Kafka, Event Hubs, IoT Hub, etc.
# MAGIC 
# MAGIC Auto Loader `cloudFiles` makes data ingestion easy without requiring strong Data Engineering expertise:
# MAGIC 
# MAGIC - Auto ingest new files exactly-once
# MAGIC - Scale to billions of files
# MAGIC - Infer & evolve schemas

# COMMAND ----------

# DBTITLE 1,Ingest IOT data with Autoloader (cloudFiles)
input_df = (
  spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", cloud_storage_path+"/digital-twins/landing_schema")
    .option("header", "true")
    .load(cloud_storage_path+"/digital-twins/landing_zone")
)

display(input_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Training a model to detect Mixing Anomalies
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-flow-small-3.png" width="700px" style="float: right" />
# MAGIC 
# MAGIC Our next step is to build a model to detect anomalies in the Mixing Step.
# MAGIC 
# MAGIC As an input, we'll take sensor data from the Mixer (e.g. vibration signals) to train our model to detect faulty mixers, preventing potential outages and defective batteries.  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Accelerating Anomaly detection using Databricks Auto-ML
# MAGIC 
# MAGIC <img style="float: right" width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC 
# MAGIC Bootstraping new ML projects can still be long and inefficient.<br/>
# MAGIC Instead of creating the same boilerplate for each new project, Databricks AutoML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC 
# MAGIC 
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC 
# MAGIC ### Using Databricks Auto ML for our Faulty Mixing Station detection
# MAGIC 
# MAGIC A dataset containing labels on faulty mixing station has been provided. 
# MAGIC All we have to do is start a new AutoML experiment and select this dataset `twins_vibration_labeled`.
# MAGIC 
# MAGIC Our prediction target is the `fault` column.
# MAGIC 
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC 
# MAGIC While this is done using the UI, you can also leverage the [Python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

# DBTITLE 1,Loading our training dataset in Databricks AutoML
import pandas as pd
labeled_dataset = spark.read.csv(cloud_storage_path+"/digital-twins/vibration_reports.csv", header=True, inferSchema=True)
labeled_dataset.withColumn("fault", F.when(F.col("fault").startswith("Ball"), "BALL_FAULT_PREDICTED").otherwise("NORMAL")) \
               .write.mode('overwrite').saveAsTable("twins_vibration_labeled")
# Our AutoML has already been running, we can check the results
display_automl_mixer_vibration_link()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Loading ML Model to detect anomalies in streaming
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-flow-small-2.png" width="700px" style="float: right" />
# MAGIC 
# MAGIC Our Data Science team have now finalized their ML Model for detecting potential vibration faults. They were able to take the *Best Model Notebook* from Databricks AutoML, then perform their own tweaks as appropriate, before publishing to our Model Registry.
# MAGIC 
# MAGIC All we now have to do is load this model from our Model Registry, and simply use it in our stream.
# MAGIC 
# MAGIC Note how simple this is, not requiring any ML knowledge or which framework was used to build it.

# COMMAND ----------

# DBTITLE 1,Load model and run inference in our data stream
# Load the current (latest) model from the PROD environment
model_production_uri = f"models:/field_demos_mixer_vibration/production"
print(f"Loading registered model version from URI: '{model_production_uri}'")

# Load the current (latest) model from the PROD environment into a User-Defined Function (UDF)
loaded_model = mlflow.pyfunc.spark_udf(spark, model_production_uri, result_type='string')
# getting a list of columns from our last run
input_column_names = loaded_model.metadata.get_input_schema().input_names()

# Apply our AutoML model immediately to our incoming data
prediction_df = input_df.withColumn("prediction", loaded_model(*input_column_names))

display(prediction_df.select('plant_id', 'line_id', 'station_id', 'prediction', '*'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Sending Live Predictions to our Digital Twins 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-flow-small-4.png" width="700px" style="float: right" />
# MAGIC 
# MAGIC The last step is now to send these predictions to Azure Digital Twins.
# MAGIC 
# MAGIC For this pipeline, we'll start a streaming job running every 30sec. We'll then get the ratio of faulty vs normal measurements based on analyzing the incoming vibration signals.
# MAGIC 
# MAGIC If we have more than 10% of measurements flagged as faulty, we'll consider the mixing station as faulty and send this tag to Azure Digital Twins.
# MAGIC 
# MAGIC *Note: this is a simple approach, more advanced solutions/models can be built, for example using a sequence of measurement instead of a point in time* 

# COMMAND ----------

credential = DefaultAzureCredential()
service_client = DigitalTwinsClient(adt_url, credential)

def publish_mixer_health(batch_df, batch_id):
  batch_df = batch_df.cache()
  # For more details, check get_mixer_health_ratio function in the _resources/00-setup notebook.
  ratio = get_mixer_health_ratio(batch_df)
  # If the ratio is > 10%, we'll consider the mixing station as faulty and update the status in our digital twins.
  for r in ratio.collect():
    twin = twin_dict[r["station_id"]]
    status = 'OK' if r['fault_ratio'] < 0.1 else 'FAULT_PREDICTED'
    #patch data for our Digital twin
    patch = {
        "HealthPrediction": status,
        "BallBearings": {
          "faultPredicted": status != "OK",
          "$metadata": {}
        }
      }
    twin.update(patch)
    print(f'Updating status of station id {r["station_id"]} to {status} on Azure Digital Twins...')
    service_client.upsert_digital_twin(r["station_id"], twin)

# COMMAND ----------

# For more details, check publish_mixer_health in the _resources/00-setup notebook.
prediction_df.writeStream.foreachBatch(publish_mixer_health).trigger(processingTime="30 second").start()

# COMMAND ----------

# DBTITLE 1,Stop all streams to release the cluster
stop_all_streams()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Visualizing anomalies on Azure Digital Twins 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-mixer-faulty.png" style="float: right" width="600px" />
# MAGIC 
# MAGIC Let's head back to our [Azure Digital Twins Explorer](https://explorer.digitaltwins.azure.net/?tid=9f37a392-f0ae-4280-9796-f1864a10effc&eid=battery-plant-digital-twin.api.eus2.digitaltwins.azure.net).
# MAGIC 
# MAGIC - Run ```SELECT * FROM digitaltwins WHERE HealthPrediction != 'OK'``` again, you should now see `MixingStep-Line1-Munich`
# MAGIC - Click the + sign (Expand tree) to show `HealthPrediction` and `BallBearings.faultPredicted`
# MAGIC - We've now made predictive maintenance a lot less ambiguous through the knowledge graph!

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion
# MAGIC 
# MAGIC Digital Twins is a powerful concept that can be used to add intelligence and semantic reasoning to many real world processes.
# MAGIC 
# MAGIC Keep in mind that this demo was a simple overview of a predictive maintenance model. Many other use-cases can be implemented, from energy optimization to root cause analysis.
# MAGIC 
# MAGIC ### Next step: Perform Root Cause Analysis & Troubleshooting with Databricks SQL
# MAGIC 
# MAGIC Our quality team reported issues with our recent batch of batteries.
# MAGIC 
# MAGIC Let's see how we can perform [root cause analysis]($./03-Root-Cause-Analysis) leveraging simple SQL queries and Databricks SQL dashboards.
