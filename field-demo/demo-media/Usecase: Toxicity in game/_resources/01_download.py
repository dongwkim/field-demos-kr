# Databricks notebook source
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Jigsaw Dataset
# MAGIC 
# MAGIC * The dataset used in this accelerator is from [Jigsaw](https://jigsaw.google.com/). Jigsaw is a unit within Google that does work to create a safer internet. Some of the areas that Jigsaw focuses on include: disinformation, censorship, and toxicity.
# MAGIC 
# MAGIC * Jigsaw posted this dataset on [Kaggle](https://www.kaggle.com/c/jigsaw-toxic-comment-classification-challenge/data) three years ago for the toxic comment classification challenge. This is a multilabel classification problem that includes the following labels:
# MAGIC   * Toxic, Severe Toxic, Obscene, Threat, Insult, and Identity Hate
# MAGIC   
# MAGIC * Further details about this dataset
# MAGIC   * Dataset title: Jigsaw Toxic Comment Classification Challenge
# MAGIC   * Dataset source URL: https://www.kaggle.com/c/jigsaw-toxic-comment-classification-challenge/data
# MAGIC   * Dataset source description: Kaggle competition
# MAGIC   * Dataset license: please see dataset source URL above

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Download jigsaw dataset
# MAGIC The Conversation AI team, a research initiative founded by Jigsaw and Google (both a part of Alphabet) are working on tools to help improve online conversation. One area of focus is the study of negative online behaviors, like toxic comments (i.e. comments that are rude, disrespectful, or otherwise likely to make someone leave a discussion).

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/toxicity_download
# MAGIC kaggle competitions download -c jigsaw-toxic-comment-classification-challenge  --force -p /tmp/toxicity_download

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unzip jigsaw dataset
# MAGIC Breakdown of the data downloaded: https://www.kaggle.com/c/jigsaw-toxic-comment-classification-challenge/overview

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/toxicity_download
# MAGIC unzip -o jigsaw-toxic-comment-classification-challenge.zip
# MAGIC unzip -o train.csv.zip
# MAGIC unzip -o test.csv.zip
# MAGIC ls .

# COMMAND ----------

# MAGIC %md
# MAGIC #### DOTA 2 Matches Dataset
# MAGIC [Dota 2](https://blog.dota2.com/?l=english)
# MAGIC 
# MAGIC 
# MAGIC <img src="https://cme-solution-accelerators-images.s3-us-west-2.amazonaws.com/toxicity/dota_2.jpg"; width="20%" />
# MAGIC 
# MAGIC This dataset is from is a multiplayer online battle arena (MOBA) video game developed and published by Valve. 
# MAGIC 
# MAGIC Dota 2 is played in matches between two teams of five players, with each team occupying and defending their own separate base on the map.
# MAGIC 
# MAGIC Further details about this dataset
# MAGIC   * Dataset title: Dota 2 Matches
# MAGIC   * Dataset source URL: https://www.kaggle.com/devinanzelmo/dota-2-matches
# MAGIC   * Dataset source description: Kaggle competition
# MAGIC   * Dataset license: please see dataset source URL above

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Download Dota 2 dataset
# MAGIC Dota 2 is a multiplayer online battle arena (MOBA) video game in which two teams of five players compete to collectively destroy a large structure defended by the opposing team known as the "Ancient", whilst defending their own.

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/toxicity_download
# MAGIC kaggle datasets download -d devinanzelmo/dota-2-matches --force -p /tmp/toxicity_download

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unzip Dota 2 dataset
# MAGIC Breakdown of the data included in the download: https://www.kaggle.com/devinanzelmo/dota-2-matches

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/toxicity_download
# MAGIC unzip -o dota-2-matches.zip

# COMMAND ----------

# MAGIC %md
# MAGIC ## Move Data to storage
# MAGIC 
# MAGIC Move the Jigsaw train and test data from the driver node to object storage so that it can be be ingested into Delta Lake.

# COMMAND ----------

for file in ['train','test','match','match_outcomes','player_ratings','players','chat','cluster_regions']:
  print(f"moving data from file:/tmp/toxicity_download/{file}.csv to {cloud_storage_path}/{file}/bronze.csv")
  dbutils.fs.mv(f"file:/tmp/toxicity_download/{file}.csv", f"{cloud_storage_path}/{file}/bronze.csv")
  print(file)

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Spark-nlp|Apache-2.0 License| https://github.com/JohnSnowLabs/spark-nlp/blob/master/LICENSE | https://github.com/JohnSnowLabs/spark-nlp/
# MAGIC |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle/kaggle-api|
