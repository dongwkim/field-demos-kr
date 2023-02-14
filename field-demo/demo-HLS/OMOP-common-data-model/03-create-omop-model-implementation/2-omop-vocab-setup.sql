-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Loading OMOP common Vocabulary
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/hls-omop-flow-2.png" width="1000" >
-- MAGIC 
-- MAGIC OMOP contains vocabulary tables. Vocabulary content can be downloaded from [Athena](https://athena.ohdsi.org/search-terms/start) website. We also made them available under `s3://hls-eng-data-public/data/rwe/omop-vocabs/`
-- MAGIC 
-- MAGIC If you like to download a different dataset, downoad the vocabularies from [Athena](https://athena.ohdsi.org/search-terms/start) and
-- MAGIC use [databricks dbfs api](https://docs.databricks.com/dev-tools/api/latest/dbfs.html#dbfs-api) utilities to upload downloaded vocabularies to `dbfs` under your `vocab_path`.
-- MAGIC 
-- MAGIC ## Adding Vocabulary to our OMOP model
-- MAGIC 
-- MAGIC We'll be reading the raw CSV files and load the data in our vocab tables.
-- MAGIC 
-- MAGIC Vocab tables can then be query using standard SQL queries to start analysing the data. Let's see how Databricks can easily load these files as Tables.
-- MAGIC 
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fhls%2Fomop%2Fbuild_model%2Fnotebook_02&dt=HLS_OMOP">

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- DBTITLE 1, Vocabulary data are available as csv file in our blob storage
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(vocab_s3_path))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Loading vocabularies as delta tables
-- MAGIC 
-- MAGIC We'll open all the Local csv file and save the as delta table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC tablelist = ["CONCEPT", "VOCABULARY", "CONCEPT_ANCESTOR", "CONCEPT_RELATIONSHIP", "RELATIONSHIP", "CONCEPT_SYNONYM", "DOMAIN", "CONCEPT_CLASS", "DRUG_STRENGTH"]
-- MAGIC 
-- MAGIC def load_table(table):
-- MAGIC   print(f"Ingesting table {table}...")
-- MAGIC   if not spark._jsparkSession.catalog().tableExists(dbName, table) or spark.table(table).count() == 0:
-- MAGIC     df = spark.read.csv(f'{vocab_s3_path}/{table}.csv.gz', inferSchema=True, header=True, dateFormat="yyyy-MM-dd")
-- MAGIC     if table in ["CONCEPT", "CONCEPT_RELATIONSHIP", "DRUG_STRENGTH"] :
-- MAGIC       df = df.withColumn('valid_start_date', to_date(df.valid_start_date,'yyyy-MM-dd')).withColumn('valid_end_date', to_date(df.valid_end_date,'yyyy-MM-dd'))
-- MAGIC 
-- MAGIC     df.write.mode('overwrite').option('overwriteSchema','true').saveAsTable(table)
-- MAGIC   
-- MAGIC   
-- MAGIC #note: we speed up a little bit the ingestion by starting 5 tables at a time with a ThreadPoolExecutor
-- MAGIC with ThreadPoolExecutor(max_workers=5) as executor:
-- MAGIC     deque(executor.map(load_table, tablelist))

-- COMMAND ----------

-- MAGIC %md ### Our data is now available to query using SQL:

-- COMMAND ----------

-- DBTITLE 1,display tables and counts of records
-- MAGIC %python
-- MAGIC tablecount = "SELECT '-' AS table, 0 as recs"
-- MAGIC for table in tablelist:
-- MAGIC   tablecount += " UNION SELECT '"+table+"', COUNT(1) FROM "+table
-- MAGIC tablecount += " ORDER BY 2 DESC"
-- MAGIC 
-- MAGIC display(spark.sql(tablecount))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create vocab map tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### source_to_standard_vocab_map

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS source_to_standard_vocab_map AS WITH CTE_VOCAB_MAP AS (
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.concept_name AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.INVALID_REASON AS SOURCE_INVALID_REASON,
    c1.concept_id AS TARGET_CONCEPT_ID,
    c1.concept_name AS TARGET_CONCEPT_NAME,
    c1.VOCABULARY_ID AS TARGET_VOCABULARY_ID,
    c1.domain_id AS TARGET_DOMAIN_ID,
    c1.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c1.INVALID_REASON AS TARGET_INVALID_REASON,
    c1.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    CONCEPT C
    JOIN CONCEPT_RELATIONSHIP CR ON C.CONCEPT_ID = CR.CONCEPT_ID_1
    AND CR.invalid_reason IS NULL
    AND lower(cr.relationship_id) = 'maps to'
    JOIN CONCEPT C1 ON CR.CONCEPT_ID_2 = C1.CONCEPT_ID
    AND C1.INVALID_REASON IS NULL
  UNION
  SELECT
    source_code,
    SOURCE_CONCEPT_ID,
    SOURCE_CODE_DESCRIPTION,
    source_vocabulary_id,
    c1.domain_id AS SOURCE_DOMAIN_ID,
    c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id,
    c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
    target_vocabulary_id,
    c2.domain_id AS TARGET_DOMAIN_ID,
    c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON,
    c2.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    source_to_concept_map stcm
    LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
  WHERE
    stcm.INVALID_REASON IS NULL
)
SELECT
  *
FROM
  CTE_VOCAB_MAP
  ;
SELECT * FROM source_to_standard_vocab_map LIMIT 100
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### source_to_source_vocab_map

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS source_to_source_vocab_map AS WITH CTE_VOCAB_MAP AS (
  SELECT
    c.concept_code AS SOURCE_CODE,
    c.concept_id AS SOURCE_CONCEPT_ID,
    c.CONCEPT_NAME AS SOURCE_CODE_DESCRIPTION,
    c.vocabulary_id AS SOURCE_VOCABULARY_ID,
    c.domain_id AS SOURCE_DOMAIN_ID,
    c.concept_class_id AS SOURCE_CONCEPT_CLASS_ID,
    c.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    c.invalid_reason AS SOURCE_INVALID_REASON,
    c.concept_ID as TARGET_CONCEPT_ID,
    c.concept_name AS TARGET_CONCEPT_NAME,
    c.vocabulary_id AS TARGET_VOCABULARY_ID,
    c.domain_id AS TARGET_DOMAIN_ID,
    c.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c.INVALID_REASON AS TARGET_INVALID_REASON,
    c.STANDARD_CONCEPT AS TARGET_STANDARD_CONCEPT
  FROM
    CONCEPT c
  UNION
  SELECT
    source_code,
    SOURCE_CONCEPT_ID,
    SOURCE_CODE_DESCRIPTION,
    source_vocabulary_id,
    c1.domain_id AS SOURCE_DOMAIN_ID,
    c2.CONCEPT_CLASS_ID AS SOURCE_CONCEPT_CLASS_ID,
    c1.VALID_START_DATE AS SOURCE_VALID_START_DATE,
    c1.VALID_END_DATE AS SOURCE_VALID_END_DATE,
    stcm.INVALID_REASON AS SOURCE_INVALID_REASON,
    target_concept_id,
    c2.CONCEPT_NAME AS TARGET_CONCEPT_NAME,
    target_vocabulary_id,
    c2.domain_id AS TARGET_DOMAIN_ID,
    c2.concept_class_id AS TARGET_CONCEPT_CLASS_ID,
    c2.INVALID_REASON AS TARGET_INVALID_REASON,
    c2.standard_concept AS TARGET_STANDARD_CONCEPT
  FROM
    source_to_concept_map stcm
    LEFT OUTER JOIN CONCEPT c1 ON c1.concept_id = stcm.source_concept_id
    LEFT OUTER JOIN CONCEPT c2 ON c2.CONCEPT_ID = stcm.target_concept_id
  WHERE
    stcm.INVALID_REASON IS NULL
)
SELECT
  *
FROM
  CTE_VOCAB_MAP;
  
SELECT * FROM source_to_source_vocab_map LIMIT 100;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## What's next
-- MAGIC 
-- MAGIC In a next step, we'll see how we can load our dataset to the Delta Lake
-- MAGIC 
-- MAGIC Next: [Ingest records to the Delta Lake]($./3-data-ingest)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
-- MAGIC 
-- MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
-- MAGIC | :-: | :-:| :-: | :-:|
-- MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
-- MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
-- MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
-- MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
-- MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|
