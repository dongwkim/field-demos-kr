# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Root Cause Analysis and Troubleshooting
# MAGIC 
# MAGIC While predictive maintenance is an extremely popular use case, there are many instances where issues could not have been pre-empted.  
# MAGIC Nevertheless, being able to troubleshoot or understand *when, how, or why* things failed can provide substantial business value.
# MAGIC 
# MAGIC For example:
# MAGIC - Diagnosing the correct subsystem, equipment, or sensor at fault minimizes downtime, maintenance costs
# MAGIC - Avoiding No Fault Found (NFF) errors
# MAGIC   - NFF is thought to cost the United States Department of Defense in excess of US$2 billion per year [(Reference)](https://en.wikipedia.org/wiki/No_fault_found#cite_note-5)
# MAGIC - Understanding the underlying behaviour can help inform appropriate design or operating conditions in the future
# MAGIC 
# MAGIC ## Databricks SQL for Analysis
# MAGIC 
# MAGIC Because Databricks is used to ingest IoT data, we're storing all of our IoT sensor information as Delta tables in our Catalog.  
# MAGIC This make data analysis simple using plain SQL. We can build an interactive dashboard to run deeper analysis in our historical data, potentially even involving custom models.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-flow-5.png" width="1000px" />
# MAGIC 
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fdigital_twins%2Fnotebook_root_cause_analysis&dt=MANUFACTURING_DIGITAL_TWINS">
# MAGIC <!-- [metadata={"description":"Root cause analysis with digital twins / IOT. Build SQL dashboard to analyze issue in battery plant manufacturing.",
# MAGIC  "authors":["pawarit.laosunthara@databricks.com"]}] -->

# COMMAND ----------

# DBTITLE 1,Generate data for DBSQL and Azure Digital Twin
# MAGIC %run ./_resources/03-Unhealthy-IoT-Data-Generator-troubleshooting $max_stream_run=1

# COMMAND ----------

# DBTITLE 1,Our data is stored in our Delta Tables:
# MAGIC %sql SELECT * FROM twins_battery_coating_analysis

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Battery Coating - Root Cause Analysis
# MAGIC 
# MAGIC Coating is a core technology in the manufacturing process of lithium-ion batteries.  
# MAGIC Our quality department discovered that several batteries from the Dallas plant have defects.  
# MAGIC Our production planning systems indicate that all of these units came from not only the same production line *but also the same coating station.*  
# MAGIC 
# MAGIC Let's see if we can analyse any potential issues with our coating process.  
# MAGIC We'll plot our coating information such as dryer fan speeds and temperatures and compare this to historical trends or other production lines to dig deeper into the problem.
# MAGIC 
# MAGIC [Let's see the Databricks SQL Coating Root Cause Analysis Dashboard](https://adb-984752964297111.11.azuredatabricks.net/sql/dashboards/fc746636-65c3-4340-a14a-608c935b2bdc-digital-twin---battery-coating---root-cause-analysis?o=984752964297111)
# MAGIC 
# MAGIC <br/>
# MAGIC <br/>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/digital-twin-azure/azure-digital-twin-root-cause-analysis.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Closing the loop - Recap
# MAGIC 
# MAGIC Databricks enables advanced analytics use cases such as custom logic, predictions, virtual (aka soft) sensing, etc.
# MAGIC   - Enriches & brings intelligence to raw data and knowledge graphs on Azure Digital Twins
# MAGIC   - Accommodates DevOps and MLOps best practices
# MAGIC   
# MAGIC Databricks unlocks a wide ecosystem through flexible, open programmatic capabilities
# MAGIC   - Orchestrate workflows either as real-time streams or scheduled jobs
# MAGIC   - This allows you to integrate insights directly into your live production environment
