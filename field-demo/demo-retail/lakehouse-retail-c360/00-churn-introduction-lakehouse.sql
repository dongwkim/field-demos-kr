-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Lakehouse for retail - Reducing Churn with a Customer 360 platform
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn.png" style="float: left; margin-right: 30px" width="600px" />
-- MAGIC 
-- MAGIC <br/>
-- MAGIC 
-- MAGIC ## Retail 에서의 Databricks Lakehouse는 무엇입니까?
-- MAGIC 
-- MAGIC 모든 워크로드에서 모든 소스의 모든 데이터를 활용하여 항상 최저 비용과 실시간 데이터로 구동되는 보다 매력적인 고객 경험을 제공할 수 있는 유일한 엔터프라이즈 데이터 플랫폼입니다.
-- MAGIC 
-- MAGIC Lakehouse for Retail 통합 분석 및 AI 기능을 사용하면 이전에는 불가능했던 규모로 개인화된 참여, 직원 생산성, 운영 속도 및 효율성을 달성할 수 있습니다. 이는 미래 보장형 소매 혁신 및 데이터 정의 기업의 기반입니다.
-- MAGIC 
-- MAGIC 
-- MAGIC ### 단순한
-- MAGIC    데이터 웨어하우징 및 AI를 위한 단일 플랫폼 및 거버넌스/보안 계층으로 **혁신을 가속화**하고 **위험을 줄입니다**. 이질적인 거버넌스와 고도로 복잡한 여러 솔루션을 함께 연결할 필요가 없습니다.
-- MAGIC 
-- MAGIC ### 열려 있는
-- MAGIC    오픈 소스 및 개방형 표준을 기반으로 합니다. 외부 솔루션과 쉽게 통합하여 데이터를 소유하고 벤더 종속을 방지합니다. 개방적이면 데이터 스택/공급업체에 관계없이 모든 외부 조직과 데이터를 공유할 수 있습니다.
-- MAGIC 
-- MAGIC ### 멀티클라우드
-- MAGIC    클라우드 전반에서 일관된 단일 데이터 플랫폼. 필요한 곳에서 데이터를 처리하십시오.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 데모: c360 데이터베이스를 구축하고 Databricks Lakehouse로 고객 이탈을 줄입니다.
-- MAGIC 
-- MAGIC 이 데모에서는 반복적인 사업을 통해 상품을 판매하는 소매 회사의 입장이 되어 보겠습니다.
-- MAGIC 
-- MAGIC 경영진들은 회사의 비즈니스는 고객 이탈에 중점을 두어야 한다고 결정했으며 우리는 다음을 요청받습니다.
-- MAGIC 
-- MAGIC * 현재 고객 이탈 분석 및 설명: 이탈, 추세 및 비즈니스에 미치는 영향을 정량화
-- MAGIC * 대상 이메일, 전화 등 자동화된 조치를 취하여 이탈을 예측하고 줄이기 위한 능동적인 시스템을 구축합니다.
-- MAGIC 
-- MAGIC 
-- MAGIC ### 우리가 만들어야 할 것
-- MAGIC 
-- MAGIC 이를 위해 Lakehouse와 함께 종단 간 솔루션을 구축할 것입니다. 고객 이탈을 적절하게 분석하고 예측할 수 있으려면 다양한 외부 시스템에서 오는 정보가 필요합니다. 웹사이트에서 오는 고객 프로필, ERP 시스템에서 오는 주문 세부 정보, 고객 활동을 분석하기 위한 모바일 애플리케이션 클릭 스트림.
-- MAGIC 
-- MAGIC 대략적으로 구현할 흐름은 다음과 같습니다.
-- MAGIC 
-- MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-0.png" />
-- MAGIC 
-- MAGIC <img width="1000px" src="/Workspace/Repos/dongwook.kim@databricks.com/field-demos-kr/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-0.png"/>
-- MAGIC 
-- MAGIC 1. 데이터를 수집하고 c360 데이터베이스를 생성하며, SQL에서 쉽게 쿼리할 수 있는 테이블을 사용해야 합니다.
-- MAGIC 2. 데이터를 보호하고 데이터 분석가 및 데이터 과학 팀에 대한 읽기 액세스 권한을 부여합니다.
-- MAGIC 3. BI 쿼리를 실행하여 기존 이탈 분석
-- MAGIC 4. ML 모델을 구축하여 이탈할 고객과 그 이유를 예측합니다.
-- MAGIC 
-- MAGIC 그 결과, 유지율을 높이기 위해 사용자 지정 작업을 트리거하는 데 필요한 모든 정보(맞춤형 이메일, 특별 제안, 전화 통화...)를 갖게 됩니다.
-- MAGIC 
-- MAGIC ### 보유한 데이터 세트
-- MAGIC 
-- MAGIC 이 데모를 단순화하기 위해 외부 시스템이 주기적으로 Blob Storage(S3/ADLS/GCS)로 데이터를 전송한다고 가정합니다.
-- MAGIC 
-- MAGIC - 고객 프로필 데이터 *(이름, 나이, 주소 등)*
-- MAGIC - 주문 내역 *(시간이 지남에 따라 고객이 구입하는 것)*
-- MAGIC - 당사 애플리케이션의 이벤트 *(고객이 애플리케이션을 마지막으로 사용한 시기, 클릭, 일반적으로 스트리밍)*
-- MAGIC 
-- MAGIC *기술적 수준에서 데이터는 모든 소스에서 가져올 수 있습니다. Databricks는 모든 시스템(SalesForce, Fivetran, kafka와 같은 대기열 메시지, Blob 저장소, SQL 및 NoSQL 데이터베이스...)에서 데이터를 수집할 수 있습니다.*
-- MAGIC 
-- MAGIC Lakehouse 내에서 이 데이터를 사용하여 고객 이탈을 분석하고 줄이는 방법을 살펴보겠습니다!

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting and preparing the data (Data Engineering)
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-2.png" />
-- MAGIC 
-- MAGIC 
-- MAGIC <br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC Our first step is to ingest and clean the raw data we received so that our Data Analyst team can start running analysis on top of it.
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
-- MAGIC 
-- MAGIC ### Delta Lake
-- MAGIC 
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake table. [Delta Lake](https://delta.io) is an open storage framework for reliability and performance. <br/>
-- MAGIC It provides many functionalities *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
-- MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`
-- MAGIC 
-- MAGIC ### Simplify ingestion with Delta Live Tables (DLT)
-- MAGIC 
-- MAGIC Databricks simplifies data ingestion and transformation with Delta Live Tables by allowing SQL users to create advanced pipelines, in batch or streaming. The engine will simplify pipeline deployment and testing and reduce operational complexity, so that you can focus on your business transformation and ensure data quality.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the customer churn 
-- MAGIC   <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Delta Live Table pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-churn-SQL) *(Alternatives: [DLT Python version]($./01-Data-ingestion/01.3-DLT-churn-python) - [plain Delta+Spark version]($./01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-churn))*. <br>
-- MAGIC   For more details on DLT: `dbdemos.install('dlt-load')` or `dbdemos.install('dlt-cdc')`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ Securing data & governance (Unity Catalog)
-- MAGIC 
-- MAGIC <img style="float: left" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-6.png" />
-- MAGIC 
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC   Now that our first tables have been created, we need to grant our Data Analyst team READ access to be able to start alayzing our Customer churn information.
-- MAGIC   
-- MAGIC   Let's see how Unity Catalog provides Security & governance across our data assets with, including data lineage and audit log.
-- MAGIC   
-- MAGIC   Note that Unity Catalog integrates Delta Sharing, an open protocol to share your data with any external organization, regardless of their stack. For more details:  `dbdemos.install('delta-sharing-airlines')`
-- MAGIC  </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-security-churn) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ Analysing churn analysis  (BI / Data warehousing / SQL) 
-- MAGIC 
-- MAGIC <img width="300px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png"  style="float: right; margin: 100px 0px 10px;"/>
-- MAGIC 
-- MAGIC 
-- MAGIC <img width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-3.png"  style="float: left; margin-right: 10px"/>
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
-- MAGIC 
-- MAGIC Data Analysts are now ready to run BI interactive queries, with low latencies & high througput, including Serverless Datawarehouses providing instant stop & start.
-- MAGIC 
-- MAGIC Let's see how we Data Warehousing can done using Databricks, including with external BI solutions like PowerBI, Tableau and other!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Datawarehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing) to start running your BI queries or access or directly open the <a href="/sql/dashboards/19394330-2274-4b4b-90ce-d415a7ff2130" target="_blank">Churn Analysis Dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Predict churn with Data Science & Auto-ML
-- MAGIC 
-- MAGIC <img width="400px" style="float: left; margin-right: 10px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-4.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC Being able to run analysis on our past data already gave us a lot of insight to drive our business. We can better understand which customers are churning evaluate the churn impact.
-- MAGIC 
-- MAGIC However, knowing that we have churn isn't enough. We now need to take it to the next level and build a predictive model to determine our customers at risk of churn and increase our revenue.
-- MAGIC 
-- MAGIC This is where the Lakehouse value comes in. Within the same platform, anyone can start building ML model to run such analysis, including low code solution with AutoML.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how to train an ML model within 1 click with the [04.1-automl-churn-prediction notebook]($./04-Data-Science-ML/04.1-automl-churn-prediction)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Automate action to reduce churn based on predictions
-- MAGIC 
-- MAGIC 
-- MAGIC <img style="float: right" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
-- MAGIC 
-- MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
-- MAGIC 
-- MAGIC - Send targeting email campaign to the customer the most likely to churn
-- MAGIC - Phone campaign to discuss with our customers and understand what's going
-- MAGIC - Understand what's wrong with our line of product and fixing it
-- MAGIC 
-- MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
-- MAGIC 
-- MAGIC ## Track churn impact over the next month and campaign impact
-- MAGIC 
-- MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
-- MAGIC 
-- MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $129,914 / month!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Open the <a href='/sql/dashboards/1e236ef7-cf58-4bfc-b861-5e6a0c105e51' target="_blank">Churn prediction DBSQL dashboard</a> to have a complete view of your business, including churn prediction and proactive analysis.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ Deploying and orchestrating the full workflow
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 10px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-5.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC 
-- MAGIC With Databricks Lakehouse, no need to manage an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./05-Workflow-orchestration/05-Workflow-orchestration-churn) to schedule our pipeline (data ingetion, model re-training etc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC We demonstrated how to implement an end 2 end pipeline with the Lakehouse, using a single, unified and secured platform:
-- MAGIC 
-- MAGIC - Data ingestion
-- MAGIC - Data analysis / DW / BI 
-- MAGIC - Data science / ML
-- MAGIC - Workflow & orchestration
-- MAGIC 
-- MAGIC As result, our analyst team was able to simply build a system to not only understand but also forecast future churn and take action accordingly.
-- MAGIC 
-- MAGIC This was only an introduction to the Databricks Platform. For more details, contact your account team and explore more demos with `dbdemos.list()`
