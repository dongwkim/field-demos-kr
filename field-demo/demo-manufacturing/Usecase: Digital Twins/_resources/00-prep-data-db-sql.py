# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #Init tables for DBSQL dashboard
# MAGIC 
# MAGIC Run this notebook only once.

# COMMAND ----------

# MAGIC %run ./03-Unhealthy-IoT-Data-Generator-troubleshooting $database=field_demos_manufacturing $insert_to_azure_digital_twin=false $max_stream_run=1
