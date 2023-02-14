// Databricks notebook source
import com.databricks.spark.xml._ 

val admi_df = spark.read.option("rowTag", "SysEvtNtfctn").xml("dbfs:/FileStore/shared_uploads/ricardo.portilla@databricks.com/iso_20022/xml/admi/admi.xml")
admi_df.write.mode("overwrite").saveAsTable("payments.default.bronze_admi")
