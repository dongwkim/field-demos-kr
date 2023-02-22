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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## 데이터 파이프라인 델타 라이브 테이블 구축
-- MAGIC ### 새로운 고품질 데이터를 위한 데이터 파이프라인을 구축하고 관리하는 간단한 방법!
-- MAGIC 
-- MAGIC 스트리밍에서 데이터를 수집하고 그 위에 ML을 적용하는 것은 정말 어려운 일입니다. <br/>
-- MAGIC Delta Live Table과 Lakehouse는 모든 복잡성을 처리하여 데이터 엔지니어링을 단순화하는 동시에 비즈니스 혁신에 집중할 수 있습니다.
-- MAGIC 
-- MAGIC **ETL 개발 가속화** <br/>
-- MAGIC 분석가와 데이터 엔지니어가 간단한 파이프라인 개발 및 유지 관리를 통해 빠르게 혁신할 수 있도록 지원
-- MAGIC 
-- MAGIC **운영 복잡성 제거** <br/>
-- MAGIC 복잡한 관리 작업을 자동화하고 파이프라인 작업에 대한 더 넓은 가시성을 확보함으로써
-- MAGIC 
-- MAGIC **데이터를 믿으세요** <br/>
-- MAGIC 내장된 품질 제어 및 품질 모니터링을 통해 정확하고 유용한 BI, 데이터 과학 및 ML을 보장합니다.
-- MAGIC 
-- MAGIC **배치 및 스트리밍 간소화** <br/>
-- MAGIC 일괄 처리 또는 스트리밍 처리를 위한 자체 최적화 및 자동 확장 데이터 파이프라인 포함
-- MAGIC 
-- MAGIC ### 예측 유지 관리를 위한 델타 라이브 테이블 파이프라인 구축
-- MAGIC 
-- MAGIC 이 예에서는 센서 데이터를 소비하고 추론을 수행하는 종단간 DLT 파이프라인을 구현합니다.
-- MAGIC 
-- MAGIC - 먼저 ML 모델을 교육하는 데 사용할 데이터 세트를 만듭니다.
-- MAGIC - 모델이 훈련되고 배포되면 델타 라이브 테이블 파이프라인에서 모델을 호출하여 실시간으로 규모에 따라 추론을 실행합니다.
-- MAGIC - 궁극적으로 이 데이터를 사용하여 DBSQL로 추적 대시보드를 구축하고 풍력 터빈 상태를 추적합니다.
-- MAGIC 
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-0.png" width="1000px"/></div>
-- MAGIC 
-- MAGIC Open the [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/c72782ae-982a-4308-8d00-93dcf36a3519/updates/2d250b66-42fc-43c9-98df-d67653b00f12) to see this flow in action
-- MAGIC 
-- MAGIC *Note: run the init cell in the [02-Wind Turbine SparkML Predictive Maintenance]($./02-Wind Turbine SparkML Predictive Maintenance) notebook to load the data.*
-- MAGIC 
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_ingestion_sql&dt=MANUFACTURING_WIND_TURBINE">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 1/ Bronze layer: ingest data using Auto Loader (cloudFiles)
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-1.png" width="600px" style="float: right"/></div>
-- MAGIC 
-- MAGIC 스트림 소스에서 데이터를 수집하는 것은 어려울 수 있습니다. 이 예에서는 클라우드 스토리지에서 파일을 점진적으로 로드하고 새 파일만 가져옵니다(거의 실시간으로 또는 X시간마다 트리거됨).
-- MAGIC 
-- MAGIC 오토로더는 다음을 제공합니다.
-- MAGIC 
-- MAGIC - 스키마 추론 및 진화
-- MAGIC - 백만 개의 파일을 처리하는 확장성
-- MAGIC - 단순성: 수집 폴더를 정의하기만 하면 Databricks가 나머지를 처리합니다!

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE sensors_bronze_dlt (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/demos/manufacturing/iot_turbine/incoming-data-json", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 2/ Silver layer: clean bronze sensors, power data and read in status
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-2.png" width="600px" style="float: right"/></div>
-- MAGIC 
-- MAGIC Let's cleanup a little bit the bronze sensor.
-- MAGIC 
-- MAGIC We'll use DLT Expectations to enforce Data Quality.

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE sensors_silver_dlt (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0) 
)
COMMENT "Cleaned data for analysis."
AS SELECT AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10, ID, SPEED, TIMESTAMP
  from STREAM(live.sensors_bronze_dlt)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 3/ Build the Training Dataset for the DS team
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-3.png" width="600px" style="float: right"/></div>
-- MAGIC 
-- MAGIC As next step, we'll join the sensor data with our labeled dataset to know which Turbine was damaged.
-- MAGIC 
-- MAGIC This table will then be used by the Data Science team to build our ML model detecting damaged turbine.

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE status_silver_dlt (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0)
)
  COMMENT "Turbine status"
AS SELECT * FROM cloud_files('/demos/manufacturing/iot_turbine/status', "parquet", map("schema", "id int, status string"))

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE training_dataset_sensor_dlt 
  COMMENT "Final sensor table with all information for Analysis / ML"
AS SELECT * FROM STREAM(live.sensors_silver_dlt) LEFT JOIN live.status_silver_dlt USING (id)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 4/ Load model from MLFlow registry and apply inference
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-5.png" width="600px" style="float: right"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Our DataScientist team built a ML model using the training dataset that we deployed.
-- MAGIC 
-- MAGIC They used Databricks AutoML to bootstrap their project and accelerate the time to deployment. The model has been packaged in Databricks Model registry and we can now easily load it within our DLT pipeline.
-- MAGIC 
-- MAGIC For more detail on the model creation steps and how Databricks can accelerate Data Scientists, open the model training notebook (`02-Wind Turbine SparkML Predictive Maintainance`)
-- MAGIC 
-- MAGIC As you can see, loading the model is a simple call to our Model Registry. 
-- MAGIC 
-- MAGIC We don't need to know how the model designed by our Data Scientist team is working, Databricks take care of that for you.
-- MAGIC 
-- MAGIC *Note: for this demo we create only 1 pipeline for both training and inference, a production-grade project will likely have 2 pipeline: 1 for training the dataset and 1 for the inference.*

-- COMMAND ----------

-- DBTITLE 1,Load Model from registry and register it as SQL function
-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC get_cluster_udf = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_wind_turbine_maintenance/Production", "string")
-- MAGIC spark.udf.register("get_turbine_status", get_cluster_udf)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE turbine_gold_dlt
  COMMENT "Final sensor table with all information for Analysis / ML"
AS SELECT get_turbine_status(struct(AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10)) as status_prediction, * FROM STREAM(live.sensors_silver_dlt)

-- COMMAND ----------

-- DBTITLE 1,Let's request our final gold table containing the predictions as example
-- MAGIC %sql 
-- MAGIC select * from field_demos_manufacturing.turbine_gold_dlt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC Our [DLT Data Pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/c72782ae-982a-4308-8d00-93dcf36a3519/updates/2d250b66-42fc-43c9-98df-d67653b00f12) is now ready using purely SQL. We have an end 2 end cycle, and our ML model has been integrated seamlessly by our Data Engineering team.
-- MAGIC 
-- MAGIC 
-- MAGIC For more details on model training, open the [model training notebook]($./02-Wind Turbine SparkML Predictive Maintenance)
-- MAGIC 
-- MAGIC Our final dataset includes our ML prediction for our Predictive Maintenance use-case. 
-- MAGIC 
-- MAGIC We are now ready to build our [DBSQL Dashboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/90b2c9c7-e5c7-46d8-b7fe-5b98ed84d5d1-turbine-demo-predictions?o=1444828305810485) to track the main KPIs and status of our entire Wind Turbine Farm.
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/wind-turbine-dashboard.png" width="1000px">
