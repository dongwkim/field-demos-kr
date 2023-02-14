# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## IOT Data Generator
# MAGIC 
# MAGIC We'll be primarily simulating or generating two types of data for our the Mixing Step of our plant:
# MAGIC 
# MAGIC - Vibration monitoring report files
# MAGIC     - IoT devices commonly upload discete files (e.g. CSV or JSON)
# MAGIC     - We can apply our logic and processing to each file as it lands on the lake
# MAGIC     - Auto Loader (perhaps with File Notification mode) is an ideal solution to handle these workflows
# MAGIC 
# MAGIC - Telemetry streams
# MAGIC     - We'll also demonstrate that Databricks can also handle real-time processing with arbitrary logic (via Structured Streaming)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### File received from IoT devices
# MAGIC 
# MAGIC Let's simulate vibration monitoring report from our mixing station. This information will be delivered in near-realtime in our Cloud Storage as JSON file.

# COMMAND ----------

# MAGIC %pip install azure-identity azure-digitaltwins-core

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

import pandas as pd
full_df = pd.read_csv("/dbfs"+cloud_storage_path+"/digital-twins/vibration_reports.csv")
full_df['plant_id']='Munich'
full_df['line_id']='Line1'
full_df['station_id']='MixingStep-Line1-Munich'

no_fault_df = full_df[full_df["fault"].str.contains("Normal")].drop("fault", axis=1)
fault_df = full_df[full_df["fault"].str.startswith("Ball")].drop("fault", axis=1)

# COMMAND ----------

# DBTITLE 1,Now let's upload our arbitrary files to DBFS
import time
import shutil
import threading

max_uploads = 50
upload_interval = 3
t_failure = 30 # seconds until failure
i_failure = int(t_failure / upload_interval) # iteration for failure to occur at
print(f"inserting data in {cloud_storage_path}/digital-twins/landing_zone/...")
dbutils.fs.mkdirs(cloud_storage_path+"/digital-twins/landing_zone")

def insert_data():
  dbutils.fs.mkdirs(f"{cloud_storage_path}/digital-twins/landing_zone/")
  for i in range(1, max_uploads + 1):
    chosen_df = no_fault_df if (i < i_failure) else fault_df
    feature_df = chosen_df.sample(n=1, random_state=i).reset_index(drop=True)
    feature_df.to_csv(f"/dbfs{cloud_storage_path}/digital-twins/landing_zone/device_upload_{str(i).zfill(4)}.csv", index=False)
    time.sleep(upload_interval)


x = threading.Thread(target=insert_data)
x.start()
time.sleep(1)
