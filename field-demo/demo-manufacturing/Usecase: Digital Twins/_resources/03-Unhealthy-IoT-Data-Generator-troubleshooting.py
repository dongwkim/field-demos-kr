# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## IOT Data Generator for troubleshooting step

# COMMAND ----------

dbutils.widgets.text("max_stream_run", "1", "max_stream_run")
dbutils.widgets.text("insert_to_azure_digital_twin", "false", "insert_to_azure_digital_twin")
dbutils.widgets.text("database", "", "database")

# COMMAND ----------

# MAGIC %pip install azure-identity azure-digitaltwins-core

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Continuous messages/telemetry from IoT devices
# MAGIC 
# MAGIC - Telemetry streams
# MAGIC     - We'll also demonstrate that Databricks can also handle real-time processing with arbitrary logic (via Structured Streaming)
# MAGIC     - Using Structured Streaming's `foreachBatch` method, we can write the data to arbitrary sinks (e.g. Azure Digital Twins)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Generate & Simulate IoT Data

# COMMAND ----------

import numpy as np
from scipy import signal
import pandas as pd
import plotly.express as px

# COMMAND ----------

rng = np.random.default_rng()

AVERAGE_RPM = 500
HEALTHY_STDDEV_RPM = 0.001*AVERAGE_RPM
FAULTY_STDDEV_RPM = 0.005*AVERAGE_RPM
N_SAMPLES = 1000
N_PERIODS = 30

AVERAGE_TEMPERATURE = 25

sin_input = (np.array(range(N_SAMPLES)) / N_SAMPLES)*N_PERIODS*np.pi

def generate_healthy():
  healthy_sin_component = 0.001*AVERAGE_RPM*np.sin(sin_input + rng.random())
  healthy_fan_speeds = rng.normal(AVERAGE_RPM, HEALTHY_STDDEV_RPM, N_SAMPLES) + healthy_sin_component
  return healthy_fan_speeds

def generate_faulty():
  faulty_square_component = 0.01*AVERAGE_RPM*signal.square(sin_input + rng.random())
  faulty_fan_speeds = rng.normal(AVERAGE_RPM, FAULTY_STDDEV_RPM, N_SAMPLES) + faulty_square_component
  return faulty_fan_speeds

# COMMAND ----------

healthy_df = pd.DataFrame({"DryerFanSpeed": generate_healthy()})
healthy_df["status"] = "OK"
faulty_df = pd.DataFrame({"DryerFanSpeed": generate_faulty()})
faulty_df["status"] = "FAULTY_CONTROLLER"

complete_df = pd.concat([healthy_df, faulty_df], axis=0)
px.scatter(complete_df, y="DryerFanSpeed", color="status", title="Fan Speeds (rpm) for Coating & Drying Station")

# COMMAND ----------

healthy_stations = [
  "CoatingStep-Line1-Munich",
  "CoatingStep-Line1-Shanghai",
  "CoatingStep-Line2-Shanghai",
  "CoatingStep-Line1-Dallas",
  "CoatingStep-Line3-Dallas",
]
faulty_stations = [
  "CoatingStep-Line2-Dallas",  
]
all_stations = healthy_stations + faulty_stations

N_REPEATS = 1
coating_df = pd.DataFrame()

for r in range(N_REPEATS):
  for station in all_stations:
    healthy_df = pd.DataFrame({"dryerFanSpeed": generate_healthy()})
    healthy_df["dryerTemperature"] = (healthy_df["dryerFanSpeed"] / AVERAGE_RPM) * AVERAGE_TEMPERATURE
    healthy_df["station_id"] = station
    healthy_df["_index"] = healthy_df.index + healthy_df.index.max()*r
    coating_df = pd.concat([coating_df, healthy_df], axis=0)

for station in healthy_stations:
  healthy_df = pd.DataFrame({"dryerFanSpeed": generate_healthy()})
  healthy_df["dryerTemperature"] = (healthy_df["dryerFanSpeed"] / AVERAGE_RPM) * AVERAGE_TEMPERATURE
  healthy_df["station_id"] = station
  healthy_df["_index"] = healthy_df.index + healthy_df.index.max()*(r+1)
  coating_df = pd.concat([coating_df, healthy_df], axis=0)

for station in faulty_stations:
  faulty_df = pd.DataFrame({"dryerFanSpeed": generate_faulty()})
  faulty_df["dryerTemperature"] = (faulty_df["dryerFanSpeed"] / AVERAGE_RPM) * AVERAGE_TEMPERATURE
  faulty_df["station_id"] = station
  faulty_df["_index"] = faulty_df.index + healthy_df.index.max()*(r+1)
  coating_df = pd.concat([coating_df, faulty_df], axis=0)
  
database = dbutils.widgets.get('database')
if len(database) > 0:
  spark.sql(f"create database if not exists {database}")
  database = database+"." 
print("writting data to "+database+"twins_battery_coating_analysis")
spark.createDataFrame(coating_df).write.mode("overwrite").saveAsTable(database+"twins_battery_coating_analysis")

# COMMAND ----------

healthy_df_b = sc.broadcast(healthy_df)
faulty_df_b = sc.broadcast(faulty_df)
N_SAMPLES_b = sc.broadcast(N_SAMPLES)

def generate_fan_telemetry(status: str, index: int) -> float:
  df = healthy_df_b.value if (status == "OK") else faulty_df_b.value
  return float(df["dryerFanSpeed"].iloc[index % N_SAMPLES_b.value])

fan_telemetry_udf = F.udf(generate_fan_telemetry, returnType=FloatType())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Set up IoT Streams

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 1) # just for this demo

base_stream_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
base_stream_df = base_stream_df.withColumnRenamed("value", "index")

statuses = ["OK", "FAULTY_CONTROLLER"]
streams = []

for status in statuses:
  streams.append(
    base_stream_df
      .withColumn("dryerFanSpeed", fan_telemetry_udf(F.lit(status), F.col("index")))
      .withColumn("dryerTemperature", F.lit(AVERAGE_TEMPERATURE)*(F.col("dryerFanSpeed")/F.lit(AVERAGE_RPM)))
      .drop("index")
  )
  
healthy_telemetry_df, faulty_telemetry_df = streams

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Send IoT updates (properties or telemetry) from Databricks to Azure Digital Twin

# COMMAND ----------

healthy_stations = [
  "CoatingStep-Line1-Munich",
  "CoatingStep-Line1-Shanghai",
  "CoatingStep-Line2-Shanghai",
  "CoatingStep-Line1-Dallas",
  "CoatingStep-Line3-Dallas",
]
faulty_stations = [
  "CoatingStep-Line2-Dallas",  
]

insert_to_azure_digital_twin = dbutils.widgets.get('insert_to_azure_digital_twin') == 'true'
if insert_to_azure_digital_twin:
  credential = DefaultAzureCredential()
  service_client = DigitalTwinsClient(adt_url, credential)

def publish_fan_telemetry(batch_df, batch_id, stations):
  """
  This **specific demo example** uses Azure Digital Twins Properties instead of Telemetry,
  so that signals can be directly seen and edited in the Twin Explorer, rather than requiring a SignalR.
  """
  batch_pdf = batch_df.toPandas()

  for station_id in stations:
    for idx, row in batch_pdf.iterrows():
      temporary_twin = twin_dict[station_id]
      patch = {
        "DryerFanSpeed": row["dryerFanSpeed"],
        "DryerTemperature": row["dryerTemperature"],
      }
      temporary_twin.update(patch)
      service_client.upsert_digital_twin(station_id, temporary_twin)
  return

publish_healthy = lambda x, y: publish_fan_telemetry(x, y, healthy_stations)
publish_faulty = lambda x, y: publish_fan_telemetry(x, y, faulty_stations)

# COMMAND ----------

max_stream_run = int(dbutils.widgets.get("max_stream_run"))
once = max_stream_run == 1
if insert_to_azure_digital_twin:
  healthy_telemetry_df.writeStream.foreachBatch(publish_healthy).trigger(once=once).queryName("twins_healthy_stream").start()

# COMMAND ----------

if insert_to_azure_digital_twin:
  faulty_telemetry_df.writeStream.foreachBatch(publish_faulty).trigger(once=once).queryName("twins_faulty_stream").start()

# COMMAND ----------

import threading
import time

def kill_stream_if_active(start_with, max_stream_run):
  for s in spark.streams.active:
    progress = s.recentProgress
    if len(progress) > 0:
      last_p = progress[len(progress)-1]
      if last_p['name'] != None and last_p['name'].startswith(start_with):
        if progress[len(progress)-1]['batchId'] > max_stream_run:
          s.stop()

def kill_stream_after_loop(start_with, max_stream_run = 100):
  while len([s for s in spark.streams.active if s.name is not None and s.name.startswith(start_with)]) > 0:
    kill_stream_if_active(start_with, max_stream_run)
    time.sleep(1)

def kill_stream_after(start_with, max_stream_run = 100):
  x = threading.Thread(target=kill_stream_after_loop, args=(start_with, max_stream_run,))
  x.start()

# COMMAND ----------

#We'll stop the stream after 100 runs
kill_stream_after('twins_', 50)
