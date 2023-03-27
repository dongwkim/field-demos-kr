# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "streaming-sessionization",
  "category": "data-engineering",
  "title": "Spark Streaming - Advanced",
  "description": "Deep dive on Spark Streaming with Delta to build webapp user sessions from clicks, with custom aggregation state management.",
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data",
      "depends_on_previous": False
    },
    {
      "path": "01-Delta-session-BRONZE", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data from Kafka - Bronze", 
      "description": "Save kafka events in a Delta Lake table and start analysis.",
      "depends_on_previous": False
    },
    {
      "path": "02-Delta-session-SILVER", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta streaming, drop duplicate", 
      "description": "Clean events and remove duplicate in the Silver layer.",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "03-Delta-session-GOLD", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Streaming State / Aggregation", 
      "description": "Compute sessions with applyInPandasWithState and upsert (MERGE) output to sessions table.",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "_00-Delta-session-PRODUCER", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Data Injector", 
      "description": "Simulate user events on website",
      "depends_on_previous": False,
      "parameters": {"reset_all_data": "false"}
    },
    {
      "path": "scala/03-Delta-session-GOLD-scala", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Streaming state with Scala", 
      "description": "Compute sessions in scala using flatMapGroupsWithState."
    }
  ]
}
