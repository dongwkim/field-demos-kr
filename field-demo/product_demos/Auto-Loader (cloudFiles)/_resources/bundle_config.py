# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "auto-loader",
  "category": "data-engineering",
  "title": "Databricks Autoloader (cloudfile)",
  "description": "Incremental ingestion on your cloud storage folder.",
  "bundle": True,
  "tags": [{"autoloader": "Auto Loader"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Init data", 
      "description": "load data."
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Init data", 
      "description": "load data."
    }, 
    {
      "path": "01-Auto-loader-schema-evolution-Ingestion", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Databricks Autoloader", 
      "description": "Simplify incremental ingestion with Databricks Autoloader (cloud_file)."
    }
  ]
}
