# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Synthea Records to Delta
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/hls-omop-flow-3.png" width="1000" >
# MAGIC 
# MAGIC In this notebook we ingest synthetic patient records, generated using [synthea](https://github.com/synthetichealth/synthea/wiki), into deltalake's bronze layer.
# MAGIC 
# MAGIC Synthea is a Synthetic Patient Population Simulator. The goal is to output synthetic, realistic (but not real), patient data and associated health records in a variety of formats.
# MAGIC 
# MAGIC We'll use this data as our input data. 
# MAGIC 
# MAGIC As the raw data is in csv format, our fist step is to ingest this information and save them as Delta table. We'll then be able to run SQL command on top of the table to load them in the OMOP model.
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fhls%2Fomop%2Fbuild_model%2Fnotebook_03&dt=HLS_OMOP">

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,display raw synthea data
display(dbutils.fs.ls(synthea_path+"/"))

# COMMAND ----------

display(dbutils.fs.ls(synthea_path+"/allergies"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest CSV files and write to delta bronze layer
# MAGIC The first step is to ingest all the `csv` files and write the resulting tables to delta (bronze layer).
# MAGIC 
# MAGIC We'll be using the Autoloader under the hood to incrementally load the data. If we re-run the process, we'll only capture the new information.
# MAGIC 
# MAGIC Note that the autoloader and Delta will also handle schema evolution and inference: if you receive csv data with wrong schema or a new column, the Autoloader will automatically handle that for you.

# COMMAND ----------

# DBTITLE 1,read raw data and write to delta
datasets = [k.name[:-1] for k in dbutils.fs.ls(synthea_path)]

def load_folder_as_table(dataset):
  print(f'loading {synthea_path}/{dataset} as a delta table {dataset}...')
  spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schemas_omop/{dataset}") \
            .option("cloudFiles.inferColumnTypes", "true") \
            .load(f'{synthea_path}/{dataset}/*.csv.gz')\
        .writeStream \
            .option("checkpointLocation", f"{cloud_storage_path}/chekpoints_omop/{dataset}") \
            .trigger(once=True) \
            .table(dataset).awaitTermination()

#note: we speed up a little bit the ingestion starting 3 tables at a time with a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=3) as executor:
    deque(executor.map(load_folder_as_table, datasets))

# COMMAND ----------

# DBTITLE 1,Let's see how many rows have been ingested in our bronze tables
table_counts=[(table, spark.table(table).count()) for table in datasets]
display(pd.DataFrame(table_counts, columns=['dataset','n_records']).sort_values(by=['n_records'], ascending=False))

# COMMAND ----------

# MAGIC %sql SELECT * FROM observations

# COMMAND ----------

# MAGIC %md 
# MAGIC ## What's next
# MAGIC 
# MAGIC With Databricks, it's super easy to ingest your own dataset and make them SQL-ready. 
# MAGIC 
# MAGIC In a next step, we'll see how we can transform this data to the OMOP model using standard SQL queries.
# MAGIC 
# MAGIC Next: [Create the OMOP model]($./4-omop531-etl-synthea)

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Smolder |Apache-2.0 License| https://github.com/databrickslabs/smolder | https://github.com/databrickslabs/smolder/blob/master/LICENSE|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
# MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
# MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|
