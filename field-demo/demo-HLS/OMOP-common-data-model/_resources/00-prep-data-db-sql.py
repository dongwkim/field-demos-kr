# Databricks notebook source
# MAGIC %md ### Setup database required for DBSQL Dashboard
# MAGIC This database is shared globally, no need to run it per user, only once per workspace

# COMMAND ----------

# MAGIC %sql create database if not exists field_demos_hls_omop ;

# COMMAND ----------

# MAGIC %py
# MAGIC for t in dbutils.fs.ls("/mnt/field-demos/hls/omop/"):
# MAGIC   print(f"loading table {t.name[:-1]}")
# MAGIC   spark.sql(f"CREATE TABLE IF NOT EXISTS field_demos_hls_omop.{t.name[:-1]} LOCATION '{t.path}' ")
