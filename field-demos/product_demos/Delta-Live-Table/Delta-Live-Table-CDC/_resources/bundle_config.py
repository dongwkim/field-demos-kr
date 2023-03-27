# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "dlt-cdc",
  "category": "data-engineering",
  "title": "CDC pipeline with Delta Live Table.",
  "description": "Ingest Change Data Capture flow with APPLY INTO and simplify SCDT2 implementation.",
  "bundle": True,
  "tags": [{"dlt": "Delta Live Table"}],
  "notebooks": [
    {
      "path": "_resources/00-Data_CDC_Generator", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "CDC data generator", 
      "description": "Generate data for the pipeline."
    },
    {
      "path": "_resources/01-load-data-quality-dashboard", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Data quality expectation load", 
      "description": "Creates data from expectation for DBSQL dashboard."
    },
    {
      "path": "01-Retail_DLT_CDC_SQL", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (SQL)", 
      "description": "CDC flow in SQL with Delta Live Table"
    },
    {
      "path": "02-Retail_DLT_CDC_Python", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (Python)", 
      "description": "CDC flow in Python with Delta Live Table"
    },
    {
      "path": "03-Retail_DLT_CDC_Monitoring", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Pipeline expectation monitoring", 
      "description": "Extract data from expectation for DBSQL dashboard.",
      "parameters": {"storage_path": "/demos/dlt/cdc/quentin_ambard"}
    },
    {
      "path": "04-Retail_DLT_CDC_Full", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "Programatically handle multiple CDC flow", 
      "description": "Use python to create a dynamic CDC pipelines with N tables."
    }
  ],
  "init_job": {
    "settings": {
        "name": "demos_dlt_cdc_init_{{CURRENT_USER_NAME}}",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "init_data",
                "notebook_task": {
                    "notebook_path": "{{DEMO_FOLDER}}/_resources/01-load-data-quality-dashboard",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Shared_job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Shared_job_cluster",
                "new_cluster": {
                    "spark_version": "11.1.x-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
    }
  },
  "cluster": {
      "spark_conf": {
        "spark.master": "local[*]",
        "spark.databricks.cluster.profile": "singleNode"
    },
    "custom_tags": {
        "ResourceClass": "SingleNode"
    },
    "num_workers": 0
  },
  "pipelines": [
    {
      "id": "dlt-cdc",
      "run_after_creation": True,
      "definition": {
        "clusters": [
            {
                "label": "default",
                "num_workers": 1
            }
        ],
        "development": True,
        "continuous": False,
        "channel": "CURRENT",
        "edition": "ADVANCED",
        "photon": False,
        "libraries": [
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/_resources/00-Data_CDC_Generator"
                }
            },
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-Retail_DLT_CDC_SQL"
                }
            }
        ],
        "name": "demos_dlt_cdc_{{CURRENT_USER_NAME}}",
        "storage": "/demos/dlt/cdc/{{CURRENT_USER_NAME}}",
        "target": "demos_dlt_cdc_{{CURRENT_USER_NAME}}"
      }
    }
  ]
}
