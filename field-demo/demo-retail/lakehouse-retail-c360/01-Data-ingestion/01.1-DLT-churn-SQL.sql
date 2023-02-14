-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Databricks를 사용한 데이터 엔지니어링 - C360 데이터베이스 구축
-- MAGIC 
-- MAGIC C360 데이터베이스를 구축하려면 여러 데이터 소스를 수집해야 합니다.
-- MAGIC 
-- MAGIC 개인화 및 마케팅 타겟팅에 사용되는 실시간 인사이트를 지원하기 위해 배치 로드 및 스트리밍 수집이 필요한 복잡한 프로세스입니다.
-- MAGIC 
-- MAGIC 다운스트림 사용자(데이터 분석가 및 데이터 과학자)를 위해 정형화된 SQL 테이블을 생성하기 위해 데이터를 수집, 변환 및 정리하는 것은 복잡합니다.
-- MAGIC 
-- MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
-- MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
-- MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
-- MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
-- MAGIC       73%
-- MAGIC     </div>
-- MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">엔터프라이즈 데이터는 분석 및 의사 결정에 사용되지 않습니다.</div>
-- MAGIC   </div>
-- MAGIC   <div style="color: #bfbfbf; padding-top: 5px">출처: Forrester</div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC <br>
-- MAGIC 
-- MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John은 데이터 엔지니어로서 엄청난 시간을 보냅니다…
-- MAGIC 
-- MAGIC 
-- MAGIC * 데이터 수집 및 변환 수동 코딩 및 기술 문제 처리:<br>
-- MAGIC   *스트리밍 및 배치 지원, 동시 작업 처리, 작은 파일 읽기 문제, GDPR 요구 사항, 복잡한 DAG 종속성...*<br><br>
-- MAGIC * 품질 및 테스트 시행을 위한 맞춤형 프레임워크 구축<br><br>
-- MAGIC * 관찰 및 모니터링 기능을 갖춘 확장 가능한 인프라 구축 및 유지<br><br>
-- MAGIC * 다른 시스템에서 호환되지 않는 거버넌스 모델 관리
-- MAGIC <br style="clear: both">
-- MAGIC 
-- MAGIC 이로 인해 **운영 복잡성**과 오버헤드가 발생하여 별도의 전문가를 필요로하고 궁극적으로 **데이터 프로젝트를 위험에 빠뜨립니다**.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # 델타 라이브 테이블(DLT)로 수집 및 변환 간소화
-- MAGIC 
-- MAGIC <img style="float: right" width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/v0214/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-1.png" />
-- MAGIC 
-- MAGIC 이 노트북에서 우리는 데이터 엔지니어로 c360 데이터베이스를 구축할 것입니다. <br>
-- MAGIC BI 및 ML 워크로드에 필요한 테이블을 준비하기 위해 원시 데이터 소스를 사용하고 정리합니다.
-- MAGIC 
-- MAGIC Blob 스토리지(`/demos/retail/churn/`)에 새로운 파일을 보내는 3개의 데이터 소스가 있으며 이 데이터를 Datawarehousing 테이블에 점진적으로 로드하려고 합니다.
-- MAGIC 
-- MAGIC - 고객 프로필 데이터 *(이름, 나이, 주소 등)*
-- MAGIC - 주문 내역 *(시간이 지남에 따라 고객이 구입하는 것)*
-- MAGIC - 애플리케이션의 스트리밍 이벤트 *(고객이 애플리케이션을 마지막으로 사용한 시간, 일반적으로 Kafka 대기열의 스트림)*
-- MAGIC 
-- MAGIC Databricks는 모두가 데이터 엔지니어링에 액세스할 수 있도록 DLT(Delta Live Table)를 사용하여 이 작업을 단순화합니다.
-- MAGIC 
-- MAGIC DLT를 사용하면 데이터 분석가가 일반 SQL로 고급 파이프라인을 만들 수 있습니다.
-- MAGIC 
-- MAGIC ## Delta Live Table: 최신 고품질 데이터를 위한 데이터 파이프라인을 구축하고 관리하는 간단한 방법!
-- MAGIC 
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>ETL 개발 가속화</strong> <br/>
-- MAGIC       분석가와 데이터 엔지니어가 간단한 파이프라인 개발 및 유지 관리를 통해 빠르게 혁신할 수 있도록 지원 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>운영 복잡성 제거</strong> <br/>
-- MAGIC       복잡한 관리 작업을 자동화하고 파이프라인 작업에 대한 더 넓은 가시성을 확보함
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>데이터에 대한 신뢰도 확장</strong> <br/>
-- MAGIC       내장된 품질 제어 및 품질 모니터링을 통해 정확하고 유용한 BI, 데이터 과학 및 ML을 보장합니다.
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>배치 및 스트리밍 간소화</strong> <br/>
-- MAGIC       일괄 처리 또는 스트리밍 처리를 위한 자체 최적화 및 자동 확장 데이터 파이프라인 포함
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC <br style="clear:both">
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC 
-- MAGIC ## Delta Lake
-- MAGIC 
-- MAGIC Lakehouse에서 생성할 모든 테이블은 Delta Lake 테이블로 저장됩니다. Delta Lake는 안정성과 성능을 위한 개방형 스토리지 프레임워크이며 많은 기능을 제공합니다(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Building a Delta Live Table pipeline to analyze and reduce churn
-- MAGIC 
-- MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our customers information. We'll use the medaillon architecture but we could build star schema, data vault or any other modelisation.
-- MAGIC 
-- MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our customer churn prediction.
-- MAGIC 
-- MAGIC This information will then be used to build our DBSQL dashboard to track customer behavior and churn.
-- MAGIC 
-- MAGIC Let's implement the following flow: 
-- MAGIC  
-- MAGIC <div><img width="1100px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de.png"/></div>
-- MAGIC 
-- MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-churn-prediction) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your DLT Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Churn Delta Live Table pipeline</a> to see it in action.<br/>
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- DBTITLE 1,Let's explore our raw incoming data data: users (json)
-- MAGIC %python
-- MAGIC display(spark.read.json('/demos/retail/churn/users'))

-- COMMAND ----------

-- DBTITLE 1,Raw incoming orders (json)
-- MAGIC %python
-- MAGIC display(spark.read.json('/demos/retail/churn/orders'))

-- COMMAND ----------

-- DBTITLE 1,Raw incoming clickstream (csv)
-- MAGIC %python
-- MAGIC display(spark.read.csv('/demos/retail/churn/events', header=True))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC 
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
-- MAGIC 
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/retail/churn/...`. 

-- COMMAND ----------

-- DBTITLE 1,Ingest raw app events stream in incremental mode 
CREATE STREAMING LIVE TABLE churn_app_events (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Application events and sessions"
AS SELECT * FROM cloud_files("/demos/retail/churn/events", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,Ingest raw orders from ERP
CREATE STREAMING LIVE TABLE churn_orders_bronze (
  CONSTRAINT orders_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Spending score from raw data"
AS SELECT * FROM cloud_files("/demos/retail/churn/orders", "json")

-- COMMAND ----------

-- DBTITLE 1,Ingest raw user data
CREATE STREAMING LIVE TABLE churn_users_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/demos/retail/churn/users", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC 
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboard are relevant and easily spot potential errors due to data anomaly.
-- MAGIC 
-- MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
-- MAGIC 
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- DBTITLE 1,Clean and anonymise User data
CREATE STREAMING LIVE TABLE churn_users (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "id")
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT
  id as user_id,
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  initcap(firstname) as firstname, 
  initcap(lastname) as lastname, 
  address, 
  canal, 
  country,
  cast(gender as int),
  cast(age_group as int), 
  cast(churn as int) as churn
from STREAM(live.churn_users_bronze)

-- COMMAND ----------

-- DBTITLE 1,Clean orders
CREATE STREAMING LIVE TABLE churn_orders (
  CONSTRAINT order_valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW, 
  CONSTRAINT order_valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date

from STREAM(live.churn_orders_bronze)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Aggregate and join data to create our ML features
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC We're now ready to create the features required for our Churn prediction.
-- MAGIC 
-- MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, sucj as:
-- MAGIC 
-- MAGIC * last command date
-- MAGIC * number of item bought
-- MAGIC * number of actions in our website
-- MAGIC * device used (ios/iphone)
-- MAGIC * ...

-- COMMAND ----------

CREATE LIVE TABLE churn_features
COMMENT "Final user table with all information for Analysis / ML"
AS 
  WITH 
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM live.churn_orders GROUP BY user_id),  
    churn_app_events_stats as (
      SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
        FROM live.churn_app_events GROUP BY user_id)

  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity,
         datediff(now(), last_event) as days_last_event
       FROM live.churn_users
         INNER JOIN churn_orders_stats using (user_id)
         INNER JOIN churn_app_events_stats using (user_id)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ Enriching the gold data with a ML model
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-4.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC Our Data scientist team has build a churn prediction model using Auto ML and saved it into Databricks Model registry. 
-- MAGIC 
-- MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline. 
-- MAGIC 
-- MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

-- COMMAND ----------

-- DBTITLE 1,Load the model as SQL function
-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC #                                                                              Stage/version    output
-- MAGIC #                                                                 Model name         |            |
-- MAGIC #                                                                     |              |            |
-- MAGIC predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/dbdemos_customer_churn/Production", "int")
-- MAGIC spark.udf.register("predict_churn", predict_churn_udf)

-- COMMAND ----------

-- DBTITLE 1,Call our model and predict churn in our pipeline
CREATE STREAMING LIVE TABLE churn_prediction 
COMMENT "Customer at risk of churn"
  AS SELECT predict_churn(struct(user_id, age_group, canal, country, gender, order_count, total_amount, total_item, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * FROM STREAM(live.churn_features)

-- COMMAND ----------

-- MAGIC %md ## Our pipeline is now ready!
-- MAGIC 
-- MAGIC As you can see, building Data Pipeline with databricks let you focus on your business implementation while the engine solves all hard data engineering work for you.
-- MAGIC 
-- MAGIC Open the <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Churn Delta Live Table pipeline</a> and click on start to visualize your lineage and consume the new data incrementally!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: secure and share data with Unity Catalog
-- MAGIC 
-- MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
-- MAGIC 
-- MAGIC Jump to the [Governance with Unity Catalog notebook]($../00-churn-introduction-lakehouse) or [Go back to the introduction]($../00-churn-introduction-lakehouse)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optional: Checking your data quality metrics with Delta Live Tables
-- MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC 
-- MAGIC <a href="/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats" target="_blank">Data Quality Dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Building our first business dashboard with Databricks SQL
-- MAGIC 
-- MAGIC Our data now is available! we can start building dashboards to get insights from our past and current business.
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 50px;" width="500px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png" />
-- MAGIC 
-- MAGIC <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
-- MAGIC 
-- MAGIC <a href="/sql/dashboards/19394330-2274-4b4b-90ce-d415a7ff2130" target="_blank">Open the DBSQL Dashboard</a>
