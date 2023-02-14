-- Databricks notebook source
-- MAGIC %md 
-- MAGIC 
-- MAGIC # Use Delta Live Tables Change-Data-Capture Features to Apply Latest Changes (`APPLY CHANGES`)
-- MAGIC 
-- MAGIC <img src='https://raw.githubusercontent.com/rportilla-databricks/dbt-asset-mgmt/main/images/SILVER_loan_perf.png' >

-- COMMAND ----------

CREATE STREAMING LIVE TABLE  silver_origination
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customer behavior";

APPLY CHANGES INTO live.silver_origination
FROM stream(live.bronze_origination)
  KEYS (loan_sequence_number)
  APPLY AS DELETE WHEN op = "d"
  SEQUENCE BY FIRST_PAYMENT_DATE

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_svcg
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customer behavior";

APPLY CHANGES INTO live.silver_svcg
FROM stream(live.bronze_svcg)
  KEYS (loan_sequence_number, monthly_reporting_period)
  APPLY AS DELETE WHEN op = "d"
  SEQUENCE BY CURRENT_ACTUAL_UPB
