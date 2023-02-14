// Databricks notebook source
import com.databricks.spark.xml._ 

// comment
val df = spark.read.option("rowTag", "FIToFIPmtStsRpt").xml("dbfs:/FileStore/shared_uploads/ricardo.portilla@databricks.com/xml_files")
df.write.mode("overwrite").saveAsTable("payments.default.bronze_pacs")
