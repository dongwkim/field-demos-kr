# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "dlt-loans",
  "category": "data-engineering",
  "title": "Full Delta Live Table pipeline - Loan.",
  "description": "Ingest loan data and implement a DLT pipeline with quarantine.",
  "bundle": True,
  "tags": [{"dlt": "Delta Live Table"}],
  "notebooks": [
    {
      "path": "_resources/00-Loan-Data-Generator", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False,
      "title":  "Loan data generator", 
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
      "path": "01-DLT-Loan-pipeline-SQL", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (SQL)", 
      "description": "Loan ingestion with DLT & quarantine"
    },
    {
      "path": "02-DLT-Loan-pipeline-PYTHON", 
      "pre_run": False, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": False,
      "title":  "DLT pipeline definition (SQL)", 
      "description": "Loan ingestion with DLT & quarantine"
    },
    {
      "path": "03-Log-Analysis", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True,
      "title":  "Pipeline expectation monitoring", 
      "description": "Extract data from expectation for DBSQL dashboard.",
      "parameters": {"storage_path": "/demos/dlt/loans/quentin_ambard"}
    }
  ],
  "init_job": {
    "settings": {
        "name": "demos_dlt_loans_init_{{CURRENT_USER_NAME}}",
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
      "id": "dlt-loans",
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
                    "path": "{{DEMO_FOLDER}}/_resources/00-Loan-Data-Generator"
                }
            },
            {
                "notebook": {
                    "path": "{{DEMO_FOLDER}}/01-DLT-Loan-pipeline-SQL"
                }
            }
        ],
        "name": "demos_dlt_loans_{{CURRENT_USER_NAME}}",
        "storage": "/demos/dlt/loans/{{CURRENT_USER_NAME}}",
        "target": "demos_dlt_loans_{{CURRENT_USER_NAME}}"
      }
    }
  ]
}
