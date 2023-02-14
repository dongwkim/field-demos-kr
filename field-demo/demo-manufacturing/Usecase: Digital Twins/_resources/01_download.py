# Databricks notebook source
dbutils.widgets.text('cloud_storage_path', '/demos/digital-twins', 'cloud_storage_path')

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC # Downloading the data from Kaggle
# MAGIC 
# MAGIC ## Obtain KAGGLE_USERNAME and KAGGLE_KEY for authentication
# MAGIC 
# MAGIC * Instructions on how to obtain this information can be found [here](https://www.kaggle.com/docs/api).
# MAGIC 
# MAGIC * This information will need to be entered below. Please use [secret](https://docs.databricks.com/security/secrets/index.html) to avoid having your key as plain text
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_setup_data&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"Companion notebook to setup data.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %run ./_kaggle_credential

# COMMAND ----------

if "KAGGLE_USERNAME" not in os.environ or os.environ['KAGGLE_USERNAME'] == "" or "KAGGLE_KEY" not in os.environ or os.environ['KAGGLE_KEY'] == "":
  print("You need to specify your KAGGLE USERNAME and KAGGLE KEY to download the data")
  print("Please open notebook under ./_resources/01_download and sepcify your Kaggle credential")
  dbutils.notebook.exit("ERROR: Kaggle credential is required to download the data. Please open notebook under ./_resources/kaggle_credential and specify your Kaggle credential")

# COMMAND ----------

try:
    cloud_storage_path
except NameError:
    cloud_storage_path = dbutils.widgets.get("cloud_storage_path")
print(f"Downloading data under {cloud_storage_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Airbus ship detection dataset
# MAGIC 
# MAGIC Get the dataset from kaggle

# COMMAND ----------

# MAGIC %sh
# MAGIC kaggle datasets download -q -d brjapon/cwru-bearing-datasets -p /tmp/

# COMMAND ----------

# MAGIC %md
# MAGIC Unzip it 

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -qo /tmp/cwru-bearing-datasets.zip -d /tmp/cwru-bearing-datasets/
# MAGIC ls /tmp/cwru-bearing-datasets/

# COMMAND ----------

src = "file:/tmp/cwru-bearing-datasets/feature_time_48k_2048_load_1.csv"
dbutils.fs.mkdirs(cloud_storage_path+"/landing_zone/")
dst = cloud_storage_path+"/vibration_reports.csv"
print(f"Moving src {src} to {dst}")
dbutils.fs.cp(src, dst)
