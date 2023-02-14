# Databricks notebook source
# MAGIC %md ### Setup database required for DBSQL Dashboard
# MAGIC This database is shared globally, no need to run it per user, only once per workspace

# COMMAND ----------

# MAGIC %sql create database if not exists field_demo_media ;
# MAGIC create table if not exists field_demo_media.qoe_predictions location '/mnt/field-demos/media/qoe/qoe_dashboard_predictions';

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- GRANT ALL PRIVILEGES ON DATABASE field_demo_media TO `quentin.ambard@databricks.com`;
# MAGIC -- REVOKE ALL PRIVILEGES ON DATABASE field_demo_media FROM admins;
# MAGIC -- GRANT USAGE, SELECT ON DATABASE field_demo_media TO admins;

# COMMAND ----------

# MAGIC %sql
# MAGIC use field_demo_media;
# MAGIC show tables;
