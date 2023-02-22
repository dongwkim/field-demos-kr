-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Lakehouse를 사용한 풍력 터빈 예측 유지 관리
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-photo-open-license.jpg" width="500px" style="float:right; margin-left: 20px"/>
-- MAGIC 
-- MAGIC 예측 유지보수는 제조 산업의 핵심 기능입니다. 실제 오류가 발생하기 전에 수리할 수 있으면 생산성과 효율성이 크게 향상되어 길고 비용이 많이 드는 정전을 방지할 수 있습니다. <br/>
-- MAGIC 
-- MAGIC 일반적인 사용 사례는 다음과 같습니다.
-- MAGIC 
-- MAGIC - 산업재해 예방을 위한 가스/휘발유 파이프라인의 밸브 고장 예측
-- MAGIC - 생산 라인의 이상 동작을 감지하여 제품의 제조 결함을 제한하고 방지합니다.
-- MAGIC - 더 큰 고장이 발생하기 전에 조기에 수리하여 더 많은 수리 비용과 잠재적인 제품 중단으로 이어짐
-- MAGIC 
-- MAGIC 이 데모에서 비즈니스 분석가는 고장이 나기 전에 풍력 터빈을 사전에 식별하고 수리할 수 있다면 에너지 생산량이 20% 증가할 수 있다고 판단했습니다.
-- MAGIC 
-- MAGIC 또한 비즈니스는 Turbine 유지관리팀 터빈을 모니터링하고 결함이 있는 터빈을 식별할 수 있는 예측 대시보드를 요청했습니다. 이를 통해 ROI를 추적하고 연간 20%의 추가 생산성 향상에 도달할 수 있습니다.
-- MAGIC 
-- MAGIC <!-- <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_dlt&dt=MANUFACTURING_WIND_TURBINE"> -->
-- MAGIC 
-- MAGIC ## Data & AI Maturity: toward automated decisions
-- MAGIC 
-- MAGIC Being able to predict which wind turbine will potentially fail is only the first step to increase our wind turbine farm efficiency.
-- MAGIC 
-- MAGIC Once we're able to build a model predicting potential maintenance, we can dynamically adapt our spare part stock and even automatically dispatch maintenance team with the proper equipment.
-- MAGIC 
-- MAGIC Let's see how the lakehouse can do that
-- MAGIC 
-- MAGIC 
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_dlt&dt=MANUFACTURING_WIND_TURBINE">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Demo: build a c360 database and reduce customer churn with Databricks Lakehouse.
-- MAGIC 
-- MAGIC In this demo, we'll step in the shoes of a retail company selling goods with a recurring business.
-- MAGIC 
-- MAGIC The business has determined that the focus must be placed on churn. We're asked to:
-- MAGIC 
-- MAGIC * Analyse and explain current customer churn: quantify churn, trends and the impact for the business
-- MAGIC * Build a proactive system to forecast and reduce churn by taking automated action: targeted email, phoning etc.
-- MAGIC 
-- MAGIC 
-- MAGIC ### What we'll build
-- MAGIC 
-- MAGIC To do so, we'll build an end-to-end solution with the Lakehouse. To be able to properly analyse and predict our customer churn, we need information coming from different external systems: Customer profiles coming from our website, order details from our ERP system and mobile application clickstream to analyse our customers activity.
-- MAGIC 
-- MAGIC At a very high level, this is the flow we'll implement:
-- MAGIC 
-- MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-0.png" />
-- MAGIC 
-- MAGIC 1. Ingest and create our c360 database, with tables easy to query in SQL
-- MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
-- MAGIC 3. Run BI queries to analyse existing churn
-- MAGIC 4. Build ML model to predict which customer is going to churn and why
-- MAGIC 
-- MAGIC As a result, we'll have all the information required to trigger custom actions to increase retention (email personalized, special offers, phone call...)
-- MAGIC 
-- MAGIC ### Our dataset
-- MAGIC 
-- MAGIC To simplify this demo, we'll consider that an external system is periodically sending data into our blob storage (S3/ADLS/GCS):
-- MAGIC 
-- MAGIC - Customer profile data *(name, age, adress etc)*
-- MAGIC - Orders history *(what our customer bough over time)*
-- MAGIC - Events from our application *(when was the last time customers used the application, clicks, typically in streaming)*
-- MAGIC 
-- MAGIC *Note that at a technical level, our data could come from any source. Databricks can ingest data from any system (SalesForce, Fivetran, queuing message like kafka, blob storage, SQL & NoSQL databases...).*
-- MAGIC 
-- MAGIC Let's see how this data can be used within the Lakehouse to analyse and reduce our customer churn!  

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
