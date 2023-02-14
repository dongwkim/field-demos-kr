# Databricks notebook source
# MAGIC %sql 
# MAGIC 
# MAGIC drop table if exists gold_txn_agg
# MAGIC ;
# MAGIC  
# MAGIC create table gold_txn_agg 
# MAGIC as
# MAGIC select acceptance_dt, province, min(txn_amount) min_amt, max(txn_amount) max_amt, percentile_approx(txn_amount, 0.5) median_amt, sum(txn_amount) agg_amount 
# MAGIC from silver_pacs
# MAGIC group by acceptance_dt, province

# COMMAND ----------

# MAGIC %sql select * from gold_txn_agg

# COMMAND ----------


