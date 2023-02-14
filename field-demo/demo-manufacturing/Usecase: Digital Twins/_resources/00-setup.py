# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Setup notebook
# MAGIC This notebook will ensure the cluster has the proper settings, and install required lib.
# MAGIC 
# MAGIC It'll also automatically download the data from kaggle if it's not available locally (please set your kaggle credential in the `_kaggle_credential` companion notebook)
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_setup&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"Companion notebook to setup libs and dependencies.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=manufacturing

# COMMAND ----------

# MAGIC %run ./_credential

# COMMAND ----------

import os
download = False
raw_data_location = cloud_storage_path + "/digital-twins"

try:
  path = "/mnt/field-demos/manufacturing/digital-twins"
  dbutils.fs.ls(path)
  print(f"Using default path {path} as raw data location")
  #raw data is set to the current user location where we just uploaded all the data
  src = path+"/vibration_reports.csv"
  dbutils.fs.mkdirs(raw_data_location+"/landing_zone/")
  dst = raw_data_location+"/vibration_reports.csv"
  print(f"Moving src {src} to {dst}")
  dbutils.fs.cp(src, dst)
except:
  download = True


if download:
  raw_data_location_exists = True
  try:
    dbutils.fs.ls(raw_data_location)
  except:
      raw_data_location_exists = False
 
  print(f"Couldn't find data saved in the default mounted bucket. Will download it from Kaggle instead under {raw_data_location}.")
  notebook = "./01_download"
  if not os.getcwd().endswith("_resources"):
    notebook = "./_resources/01_download"
  if dbutils.widgets.get('reset_all_data') == 'false' and raw_data_location_exists:
      print(f"Data already existing. Set reset all data to true to force a new download.")
  else:
    print(f"Forcing data download from kaggle.")
    print("Note: you need to specify your Kaggle Key under ./_resources/_kaggle_credential ...")
    result = dbutils.notebook.run(notebook, 3600, {"cloud_storage_path": raw_data_location})
    if result is not None and "ERROR" in result:
      print("-------------------------------------------------------------")
      print("---------------- !! ERROR DOWNLOADING DATASET !!-------------")
      print("-------------------------------------------------------------")
      print(result)
      print("-------------------------------------------------------------")
      raise RuntimeError(result)
    else:
      print(f"Success. Dataset downloaded from kaggle and saved under {raw_data_location}.")

# COMMAND ----------

import mlflow.pyfunc
import pyspark.sql.functions as F
import pandas as pd
def display_automl_mixer_vibration_link(): 
  display_automl_link("mixer_vibration_auto_ml", "field_demos_mixer_vibration", spark.table("twins_vibration_labeled"), "fault", 5, True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Helper functions

# COMMAND ----------

# DBTITLE 1,Get the ratio of (faulty/total) prediction for each mixing station
def get_mixer_health_ratio(batch_df):
  #Grouping by mixing station ID and prediction status
  predictions = batch_df.groupby('plant_id', 'line_id', 'station_id', 'prediction').agg(F.count('prediction').alias('count'))
  #Get the count of normal & faulty prediction over the last batch (=last few minutes)
  predictions_normal = predictions.withColumnRenamed('count', 'count_normal').filter("prediction = 'NORMAL'")
  predictions_fault = predictions.withColumnRenamed('count', 'count_fault').filter("prediction = 'BALL_FAULT_PREDICTED'")
  #Get a ratio of faulty points
  return predictions_fault.join(predictions_normal, ['station_id', 'plant_id', 'line_id'], 'outer') \
                           .withColumn('count_normal', F.coalesce(F.col('count_normal'), F.lit(0))) \
                           .withColumn('count_fault', F.coalesce(F.col('count_fault'), F.lit(0))) \
                           .withColumn('fault_ratio', F.col('count_fault') / (F.col('count_fault')+F.col('count_normal')))


# COMMAND ----------

# DBTITLE 1,Support to kill stream and prevent them from running too long
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
