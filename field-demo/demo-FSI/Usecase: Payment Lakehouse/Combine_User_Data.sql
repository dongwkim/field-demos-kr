-- Databricks notebook source
use catalog payments

-- COMMAND ----------

use database default

-- COMMAND ----------

drop table if exists joined_users
; 

create table joined_Users 
as 
select a.id, province, acceptance_dt, txn_amount, credttm, authntcddata
from silver_pacs a join silver_card_acquirer b on a.id = b.acctid join silver_fraud c on b.acctid = c.id

-- COMMAND ----------


