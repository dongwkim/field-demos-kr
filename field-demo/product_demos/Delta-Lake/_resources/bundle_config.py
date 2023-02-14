# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "delta-lake",
  "category": "data-engineering",
  "title": "Delta Lake",
  "description": "Store your table with Delta Lake & discover how Delta Lake can simplify your Data Pipelines.",
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data"
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data"
    },
    {
      "path": "00-Delta-Lake-Introduction", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Introduction to Delta Lake", 
      "description": "Create your first table, DML operation, time travel, RESTORE, CLONE and more.",
      "parameters": {"raw_data_location": "/Users/quentin.ambard@databricks.com/demos/retail/delta"}
    },
    {
      "path": "01-Delta-Lake-Performance", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Performance & operation", 
      "description": "Faster queries with OPTIMIZE, ZORDERS and partitions."
    },
    {
      "path": "02-Delta-Lake-CDF", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Change Data Flow (CDF)", 
      "description": "Capture and propagate table changes."
    },
    {
      "path": "03-Advanced-Delta-Lake-Internal", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Internals", 
      "description": "Deep dive in Delta Lake file format."
    }    
  ]
}
