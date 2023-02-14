// Databricks notebook source
// MAGIC %sql 
// MAGIC 
// MAGIC drop table if exists silver_acmt; 
// MAGIC 
// MAGIC create table silver_acmt as 
// MAGIC select txinfandsts, split(trim(split(grphdr.InstgAgt.FinInstnId.pstladr.adrline, ',')[1]), ' ')[0] province, 
// MAGIC cast(cast(txinfandsts.accptncdttm as timestamp) as date) acceptance_dt,
// MAGIC cast(txinfandsts.accptncdttm as timestamp) acceptance_ts,
// MAGIC txinfandsts['ChrgsInf']['Amt']['_VALUE'] txn_amount
// MAGIC from payments.default.bronze_acmt x

// COMMAND ----------

// MAGIC %sql select * from payments.default.bronze_acmt

// COMMAND ----------


