-- Databricks notebook source
-- DBTITLE 1,Use Catalog to Isolate Teams and Product Areas - Highest Level Container in Unity Catalog
use catalog payments

-- COMMAND ----------

use database default

-- COMMAND ----------

-- DBTITLE 1,Fraud reporting Data Format
drop table if exists silver_fraud
; 

create table silver_fraud 
as 
select 
body.cntxt.txcntxt.CardPrgrmmApld.id, body.envt.Crdhldr.ctctinf.prsnlEmailAdr
from payments.default.bronze_cafr x

-- COMMAND ----------

-- DBTITLE 1,Using Dynamic Views, Admins Can Encrypt and Mask Data Fields (Restricting Rows or Columns)
create or replace view silver_fraud_vw 
as 
select id, case when is_member("data-science") then base64(aes_encrypt(prsnlemailadr, 'key')) else prsnlemailadr end prsnlemailadr
from silver_fraud

-- COMMAND ----------

select * from silver_fraud_vw

-- COMMAND ----------


