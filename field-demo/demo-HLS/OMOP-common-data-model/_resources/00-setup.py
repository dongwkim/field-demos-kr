# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=hls

# COMMAND ----------

from pyspark.sql.functions import to_date
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

from concurrent.futures import ThreadPoolExecutor
from collections import deque
import pandas as pd
import plotly.express as px

synthea_path='s3://hls-eng-data-public/data/rwe/all-states-90K'

vocab_s3_path = 's3://hls-eng-data-public/data/rwe/omop-vocabs'
