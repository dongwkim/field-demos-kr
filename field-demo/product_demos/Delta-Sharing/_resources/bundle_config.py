# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "delta-sharing-airlines",
  "category": "governance",
  "title": "Delta Sharing - Airlines",
  "description": "Share your data to external organization using Delta Sharing.",
  "bundle": True,
  "tags": [{"delta-sharing": "Delta Sharing"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Setup data", 
      "description": "Create the catalog for the demo."
    },
    {
      "path": "01-Delta-Sharing-presentation", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False, 
      "title":  "Delta Sharing - Introduction", 
      "description": "Discover Delta-Sharing and explore your sharing capabilities."
    },
    {
      "path": "02-provider-delta-sharing-demo", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Share data as Provider", 
      "description": "Discover how to create SHARE and RECIPIENT to share data with external organization."
    },
    {
      "path": "03-receiver-delta-sharing-demo", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data (external system)", 
      "description": "Read data shared from Delta Sharing using any external system."
    },
    {
      "path": "04-share-data-within-databricks", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Consume data with Databricks UC", 
      "description": "Simplify data access from another Databricks Workspace with Unity Catalog."
    },
    {
      "path": "05-extra-delta-sharing-rest-api",
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Sharing Internals - REST API", 
      "description": "Extra: Deep dive in Delta Sharing REST API."
    }    
  ],
  "cluster": {
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0,
    "single_user_name": "{{CURRENT_USER}}",
    "data_security_mode": "SINGLE_USER"
  }
}
