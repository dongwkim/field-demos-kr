# Databricks notebook source
# MAGIC %md # Setup table for dashboard
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fproduct_recommender_stadium_init&dt=MEDIA_USE_CASE">

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists field_demos_media;
# MAGIC use field_demos_media;

# COMMAND ----------

# MAGIC %python
# MAGIC for file in dbutils.fs.ls('/mnt/field-demos/media/stadium/dashboard'):
# MAGIC   #sql = f"drop table field_demos_media.{folder.name[:-1]}_{file.name[:-1]}"
# MAGIC   sql = f"create table if not exists field_demos_media.stadium_{file.name[:-1]} location '{file.path}'"
# MAGIC   print(sql)
# MAGIC   spark.sql(sql)

# COMMAND ----------

# MAGIC %sql show tables
