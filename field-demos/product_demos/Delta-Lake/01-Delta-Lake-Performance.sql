-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-lake-perf-bench.png" width="500" style="float: right; margin-left: 50px"/>
-- MAGIC 
-- MAGIC # Delta Lake: Performance & maintenance operations
-- MAGIC 
-- MAGIC ## Blazing fast query at scale
-- MAGIC 
-- MAGIC Delta Lake saves all your table metadata in an efficient format, ranging from efficient queries on small tables (GB) to massive PB-scale tables. 
-- MAGIC 
-- MAGIC Delta Lake is designed to be smart and do all the hard job for you. It'll automatically tune your table and read the minimum data required to be able to satisfied your query.
-- MAGIC 
-- MAGIC This result in **fast read query**, even with a growing number of data/partitions!
-- MAGIC 
-- MAGIC 
-- MAGIC In this notebook, we'll see how we can leverage Delta Lake unique capabilities to speedup requests and simplify maintenance operation. For more details, we recommend to read the [documentation](https://docs.databricks.com/delta/file-mgmt.html).
-- MAGIC 
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Fperf&dt=FEATURE_DELTA">
-- MAGIC <!-- [metadata={"description":"Quick introduction to Delta Lake. <br/><i>Use this content for quick Delta demo.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- DBTITLE 1,Init the demo data under ${raw_data_location}/user_parquet.
-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Solving the "Small file" problem with COMPACT
-- MAGIC 
-- MAGIC When working with a Delta Table, your data is being saved in a cloud storage (S3/ADLS/GCS). Adding data to the table results in new file creation, and your table can quickly have way too many small files which is going to impact performances over time.
-- MAGIC 
-- MAGIC This becomes expecially true with streaming operation where you add new data every few seconds, in near realtime.
-- MAGIC 
-- MAGIC Delta Lake solves this operation with the `OPTIMIZE` command, which is going to optimize the file layout for you, picking the proper file size based on heuristics.

-- COMMAND ----------

-- let's compact our table. Note that the engine decided to compact 8 files into 1 ("numFilesAdded": 1, "numFilesRemoved": 8)
OPTIMIZE user_delta 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC These maintenance operation have to be triggered frequently to keep our table properly optimized.
-- MAGIC 
-- MAGIC Using Databricks, you can have your table automatically optimized out of the box, without having you to worry about it. All you have to do is set the [proper table properties](https://docs.databricks.com/optimizations/auto-optimize.html), and the engine will optimize your table when needed, without having you to run manual OPTIMIZE operation.
-- MAGIC 
-- MAGIC We strongly recommend to enable this option for all your tables.

-- COMMAND ----------

ALTER TABLE user_delta SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Adding indexes (ZORDER) to your table
-- MAGIC 
-- MAGIC If you request your table using a specific predicat (ex: username), you can speedup your request by adding an index on these columns. We call this operation ZORDER.
-- MAGIC 
-- MAGIC You can ZORDER on any column, especially the one having high cardinality (id, firstname etc). 
-- MAGIC 
-- MAGIC *Note: We recommand to stay below 4 ZORDER columns for better query performance.*

-- COMMAND ----------

OPTIMIZE user_delta ZORDER BY (id, firstname);

-- our next queries using a filter on id or firstname will be much faster
SELECT * FROM user_delta where id = 4 or firstname = 'Quentin';

-- COMMAND ----------

-- MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Generated columns for dynamic partitions
-- MAGIC 
-- MAGIC Adding partitions to your table is a way of saving data having the same column under the same location. Our engine will then be able to read less data and have better read performances.
-- MAGIC 
-- MAGIC Using Delta Lake, partitions can be generated based on expression, and the engine will push-down your predicate applying the same expression even if the request is on the original field.
-- MAGIC 
-- MAGIC A typical use-case is to partition per a given time (ex: year, month or even day). 
-- MAGIC 
-- MAGIC Our user table has a `creation_date` field. We'll generate a `creation_day` field based on an expression and use it as partition for our table with `GENERATED ALWAYS`.
-- MAGIC 
-- MAGIC In addition, we'll let the engine generate incremental ID.
-- MAGIC 
-- MAGIC *Note: Remember that partition will also create more files under the hood. You have to be careful using them. Make sure you don't over-partition your table (aim for 100's of partition max, having at least 1GB of data). We don't recommend creating partition on table smaller than 1TB. Use ZORDER instead.*

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC CREATE TABLE IF NOT EXISTS user_delta_partition (
-- MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 10000 INCREMENT BY 1 ), 
-- MAGIC   firstname STRING, 
-- MAGIC   lastname STRING, 
-- MAGIC   email STRING, 
-- MAGIC   address STRING, 
-- MAGIC   gender INT, 
-- MAGIC   age_group INT,
-- MAGIC   creation_date timestamp, 
-- MAGIC   creation_day date GENERATED ALWAYS AS ( CAST(creation_date AS DATE) ) )
-- MAGIC PARTITIONED BY (creation_day);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Note that we don't insert data for the creation_day field or id. The engine will handle that for us:
-- MAGIC INSERT INTO user_delta_partition (firstname, lastname, email, address, gender, age_group, creation_date) SELECT
-- MAGIC   firstname,
-- MAGIC   lastname,
-- MAGIC   email,
-- MAGIC   address,
-- MAGIC   gender,
-- MAGIC   age_group,
-- MAGIC   creation_date
-- MAGIC FROM user_delta;

-- COMMAND ----------

SELECT * FROM user_delta_partition where creation_day = CAST(NOW() as DATE) ;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC That's it! You know how to have super fast queries on top of your Delta Lake tables!
-- MAGIC 
-- MAGIC 
-- MAGIC Next: Discover how to do capture your table changes with [Delta Lake Change Data Flow (CDF)]($./02-Delta-Lake-CDF).
