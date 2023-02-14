-- Databricks notebook source
use catalog payments 

-- COMMAND ----------

use database default

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC 
-- MAGIC drop table if exists silver_pacs; 
-- MAGIC 
-- MAGIC create table silver_pacs as 
-- MAGIC select txinfandsts.ChrgsInf.agt.brnchid.id, split(trim(split(grphdr.InstgAgt.FinInstnId.pstladr.adrline, ',')[1]), ' ')[0] province, 
-- MAGIC cast(cast(txinfandsts.accptncdttm as timestamp) as date) acceptance_dt,
-- MAGIC cast(txinfandsts.accptncdttm as timestamp) acceptance_ts,
-- MAGIC txinfandsts['ChrgsInf']['Amt']['_VALUE'] txn_amount
-- MAGIC from bronze_pacs x

-- COMMAND ----------


