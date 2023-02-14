# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "pandas-on-spark",
  "category": "data-science",
  "title": "Pandas API with spark backend (Koalas)",
  "description": "Let you Data Science team scale to TB of data while working with Pandas API, without having to learn & move to another framework.",
  "bundle": True,
  "tags": [{"ds": "Data Science"}],
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
      "path": "_resources/01-load-data",
      "pre_run": False,
      "publish_on_website": False,
      "add_cluster_setup_cell": False,
      "title":  "Setup",
      "description": "Init data for demo."
    },
    {
      "path": "01-pyspark-pandas-api-koalas", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Pandas on Spark (Koalas)", 
      "description": "Scale & accelerate your Pandas transformations"
    }
  ]
}
