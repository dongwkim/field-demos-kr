// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # Ingesting and Curating an ISO20022 Payments Data Lakehouse
// MAGIC 
// MAGIC <p></p>
// MAGIC <p></p>
// MAGIC <p></p>
// MAGIC 
// MAGIC <img src='files/shared_uploads/ricardo.portilla@databricks.com/Screen_Shot_2022_10_28_at_12_01_28_AM.png' width=1000>
// MAGIC 
// MAGIC <img src='https://www.databricks.com/wp-content/uploads/2021/09/DLT_graphic_tiers.jpg'>

// COMMAND ----------

// MAGIC %sql drop table if exists payments.default.bronze_pacs
// MAGIC ;

// COMMAND ----------

// DBTITLE 1,Databricks Has XML Integration (with Schema validation from XSD)
import com.databricks.spark.xml._ 

// COMMAND ----------

// DBTITLE 1,XML Reader Allows Parsing to Scale Out to Millions of Files
val df = spark.read.option("rowTag", "FIToFIPmtStsRpt").xml("dbfs:/FileStore/shared_uploads/ricardo.portilla@databricks.com/iso_20022/xml/pacs/")
df.write.mode("overwrite").saveAsTable("payments.default.bronze_pacs")

// COMMAND ----------



// COMMAND ----------


