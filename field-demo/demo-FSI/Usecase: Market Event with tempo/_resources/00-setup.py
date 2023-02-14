# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=fsi

# COMMAND ----------

import re
from pyspark.sql.functions import col
import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from pyspark.sql.functions import struct, count, col
from sklearn.preprocessing import OneHotEncoder, LabelEncoder
from sklearn.ensemble import IsolationForest
from tempo.utils import *
from pyspark.sql.functions import * 

import plotly.graph_objs as go
import plotly.express as px
import pandas as pd


