# Databricks notebook source
import pyspark.sql.functions as F

import copy
import os
import json
import traceback

from azure.digitaltwins import *
from azure.core.exceptions import ResourceExistsError
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import *

# COMMAND ----------

try:
  azSubcriptionId = dbutils.secrets.get(scope = "common-sp", key = "az-sub-id")
  azTenantId = dbutils.secrets.get(scope = "common-sp", key = "az-tenant-id")
  spId = dbutils.secrets.get(scope = "common-sp", key = "common-sa-sp-client-id")
  spSecret = dbutils.secrets.get(scope = "common-sp", key = "common-sa-sp-client-secret")
except:
  azSubcriptionId = ""
  azTenantId = ""
  spId = ""
  spSecret = ""
  
os.environ["AZURE_TENANT_ID"] = azTenantId
os.environ["AZURE_CLIENT_ID"] = spId
os.environ["AZURE_CLIENT_SECRET"] = spSecret


# COMMAND ----------

import os
prefix = ''
if not os.getcwd().endswith('_resources'):
  prefix = './_resources/'
  
adt_url = "battery-plant-digital-twin.api.eus2.digitaltwins.azure.net"
models_path = prefix+'setup-digital-twins/models/'
twin_graph_path = prefix+'setup-digital-twins/twins_definition/TwinGraph.json'

with open(twin_graph_path) as fh:
  twin_graph = json.load(fh)["digitalTwinsGraph"]
  twins = twin_graph["digitalTwins"]

twin_dict = {twin["$dtId"]: twin for twin in twins}  
