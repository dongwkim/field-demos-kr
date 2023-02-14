# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Building the Digital Twin using Azure Digital Twin
# MAGIC 
# MAGIC 
# MAGIC In this notebook, we'll leverage Azure sdk to build our digital twin graph and produce the following output:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-full.png" width="800px">
# MAGIC 
# MAGIC For more details, the graph definition is available in the json files that you'll find under the setup folder.
# MAGIC 
# MAGIC We'll now load these files into Azure Digital Twin using the python SDK. 
# MAGIC 
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fdigital_twins%2Fnotebook_azure_setup&dt=MANUFACTURING_DIGITAL_TWINS">
# MAGIC <!-- [metadata={"description":"Setup Azure Digital Twin for Battery Plant Manufacturing demo.",
# MAGIC  "authors":["pawarit.laosunthara@databricks.com"]}] -->

# COMMAND ----------

# DBTITLE 1,Instaling Azure SDK
# MAGIC %pip install azure-identity azure-digitaltwins-core

# COMMAND ----------

# DBTITLE 1,Get the credential to connect
# MAGIC %run ./_credential

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up connection to the Azure Digital Twin instance

# COMMAND ----------

credential = DefaultAzureCredential()
service_client = DigitalTwinsClient(adt_url, credential)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload Digital Twin Definition Language (DTDL) models

# COMMAND ----------

model_jsons = os.listdir(models_path)
models = []
for fn in model_jsons:
  with open(models_path + fn, 'r') as fh:
    models.extend(json.load(fh))

# COMMAND ----------

try:
  created_models = service_client.create_models(models)
except Exception as e:
  if not isinstance(e, ResourceExistsError):
    print(traceback.format_exc()) # show stack trace if unexpected exception

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Upload instantiated Twins and Relationships

# COMMAND ----------

with open(twin_graph_path) as fh:
  
  twin_graph = json.load(fh)["digitalTwinsGraph"]
  twins = twin_graph["digitalTwins"]
  relationships = twin_graph["relationships"]
  
  for twin in twins:
    service_client.upsert_digital_twin(twin["$dtId"], twin)
  for rel in relationships:
    service_client.upsert_relationship(rel["$sourceId"], rel["$relationshipId"], rel)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's it! Our Digital Twin graph has been modelized! Open the [Azure Digital Explorer](https://explorer.digitaltwins.azure.net/?tid=9f37a392-f0ae-4280-9796-f1864a10effc&eid=battery-plant-digital-twin.api.eus2.digitaltwins.azure.net) to view the result.
# MAGIC 
# MAGIC We can now start generating data, simulating our plant IOT sensors and monitor our mixer status, for predictive maintenance.
