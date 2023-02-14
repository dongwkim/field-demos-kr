// Databricks notebook source
// MAGIC %sql drop table if exists payments.default.bronze_cafr;

// COMMAND ----------

import com.databricks.spark.xml._ 

val cafr_df = spark.read.option("rowTag", "FrdRptgInitn").xml("dbfs:/FileStore/shared_uploads/ricardo.portilla@databricks.com/iso_20022/xml/cafr/cafr.xml")
cafr_df.write.mode("overwrite").saveAsTable("payments.default.bronze_cafr")
