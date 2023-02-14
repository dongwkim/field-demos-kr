# Databricks notebook source
# MAGIC %md # Setup table for dashboard
# MAGIC 
# MAGIC Field Demo dashboard. Please do not edit
# MAGIC 
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffsi%2Ftempo%2Fnotebook_tempo_init&dt=FSI_TEMPO">
# MAGIC <!-- [metadata={"description":"Financial timeseries analysis with tempo - analysis", "authors":["layla.yang@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists field_demos_fsi;
# MAGIC use field_demos_fsi;

# COMMAND ----------

# MAGIC %python
# MAGIC for folder in dbutils.fs.ls('/mnt/field-demos/fsi/tempo/dashboard'):
# MAGIC   if "raw" not in folder.path:
# MAGIC     sql = f"create table if not exists field_demos_fsi.tempo_{folder.name[:-1]} location '{folder.path}'"
# MAGIC     print(sql)
# MAGIC     spark.sql(sql)

# COMMAND ----------

# MAGIC %sql show tables
