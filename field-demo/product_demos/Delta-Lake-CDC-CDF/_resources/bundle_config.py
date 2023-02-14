# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "cdc-pipeline",
  "category": "data-engineering",
  "title": "CDC Pipeline with Delta",
  "description": "Process CDC data to build an entire pipeline and materialize your operational tables in your lakehouse.",
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Setup", 
      "description": "Setup."
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Data initialization", 
      "description": "Data initialization"
    },
    {
      "path": "01-CDC-CDF-simple-pipeline", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Implement CDC flow with Delta Lake", 
      "description": "Ingest CDC data and materialize your tables and propagate changes downstream."
    },
    {
      "path": "02-CDC-CDF-full-multi-tables", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Performance & operation", 
      "description": "Programatically ingest multiple CDC flows to synch all your database."
    }
  ]
}
