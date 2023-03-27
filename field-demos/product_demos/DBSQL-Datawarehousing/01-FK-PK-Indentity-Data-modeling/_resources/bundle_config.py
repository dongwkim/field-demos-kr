# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "identity-pk-fk",
  "category": "DBSQL",
  "title": "Data Warehousing with Identity, Primary Key & Foreign Key",
  "description": "Define your schema with auto incremental column and Primary + Foreign Key. Ideal for Data Warehouse & BI support!",
  "bundle": True,
  "tags": [{"dbsql": "BI/DW/DBSQL"}],
  "notebooks": [
    {
      "path": "_resources/00-setup",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "00-Identity_PK_FK", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Identify column, PK & FK", 
      "description": "Define your schema with auto incremental column and Primary + Foreign Key."
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
