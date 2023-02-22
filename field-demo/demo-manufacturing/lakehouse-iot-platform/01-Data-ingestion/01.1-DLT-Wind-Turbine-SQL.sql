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
-- MAGIC <img style="float: right" width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-1.png" />
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
-- MAGIC ## IOT 센서를 수집하고 결함이 있는 장비를 감지하기 위한 Delta Live Table 파이프라인 구축
-- MAGIC 
-- MAGIC 이 예에서는 풍력 터빈 센서 데이터를 사용하는 종단 2 종단 DLT 파이프라인을 구현합니다. <br/>
-- MAGIC medaillon 아키텍처를 사용하지만 스타 스키마, 데이터 저장소 또는 기타 모델링을 구축할 수 있습니다.
-- MAGIC 
-- MAGIC 자동 로더로 새 데이터를 점진적으로 로드하고 이 정보를 보강한 다음 MLFlow에서 모델을 로드하여 예측 유지 관리 분석을 수행합니다.
-- MAGIC 
-- MAGIC 그런 다음 이 정보는 DBSQL 대시보드를 구축하여 풍력 터빈 농장 상태, 결함이 있는 장비 영향 및 잠재적 가동 중지 시간을 줄이기 위한 권장 사항을 추적하는 데 사용됩니다.
-- MAGIC ### 데이터셋:
-- MAGIC 
-- MAGIC * <strong>터빈 메타데이터</strong>: 터빈 ID, 위치 (터빈 당 하나의 행)
-- MAGIC * <strong>터빈 센서 스트림</strong>: 풍력 터빈 센서로 부터 발생하는 실시간 스트리밍(진동, 에너지 소모, 속도 등)
-- MAGIC * <strong>터빈 상태</strong>: 어떤 부품에 결함이 있는지 분석하기 위한 과거 터빈 상태(ML 모델에서 레이블로 사용됨)
-- MAGIC 
-- MAGIC 
-- MAGIC 다음 흐름을 구현해 보겠습니다:
-- MAGIC  
-- MAGIC <div><img width="1100px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-full.png"/></div>
-- MAGIC 
-- MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-predictive-maintenance-turbine) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC DLT 파이프라인이 설치되고 시작되었습니다! <a dbdemos-pipeline-id="dlt-iot-wind-turbine" href="#joblist/pipelines/0919af77-76cd-40b9-98cd-f4c726cd0e33" target="_blank">IOT 풍력 터빈 델타 라이브 테이블 파이프라인 열기 </a> 작동 상태를 확인하세요.<br/>

-- COMMAND ----------

-- DBTITLE 1,Wind Turbine metadata:
-- MAGIC %python
-- MAGIC display(spark.read.json('/demos/manufacturing/iot_turbine/turbine'))
-- MAGIC display(spark.read.json('/demos/manufacturing/iot_turbine/historical_turbine_status')) #Historical turbine status analyzed

-- COMMAND ----------

-- DBTITLE 1,Wind Turbine sensor data
-- MAGIC %python
-- MAGIC display(spark.read.json('/demos/manufacturing/iot_turbine/incoming_data'))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 1/ Ingest data: ingest data using Auto Loader (cloudFiles)
-- MAGIC 
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-1.png" width="700px" style="float: right"/></div>
-- MAGIC 
-- MAGIC 스트림 소스에서 데이터를 수집하는 것은 어려울 수 있습니다. 이 예에서는 클라우드 스토리지에서 파일을 점진적으로 로드하고 새 파일만 가져옵니다(거의 실시간으로 또는 X시간마다 트리거됨).
-- MAGIC 
-- MAGIC 스트리밍 데이터가 클라우드 스토리지에 추가되는 동안 kafka에서 직접 쉽게 수집 할수도 있습니다. `.format(kafka)`
-- MAGIC 
-- MAGIC 오토 로더는 다음을 제공합니다.
-- MAGIC 
-- MAGIC - 스키마 추론 및 진화
-- MAGIC - 백만 개의 파일을 처리하는 확장성
-- MAGIC - 단순성: 수집 폴더를 정의하기만 하면 Databricks가 나머지를 처리합니다!
-- MAGIC 
-- MAGIC 
-- MAGIC 이를 파이프라인에 사용하고 blob 스토리지 `/demos/manufacturing/iot_turbine/...`에서 전달되는 원시 JSON 및 CSV 데이터를 수집해 보겠습니다.

-- COMMAND ----------

-- DBTITLE 1,Turbine metadata
CREATE STREAMING LIVE TABLE turbine (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "위치, 풍력 터빈 모델 유형 등이 포함된 터빈 세부 정보"
AS SELECT * FROM cloud_files("/demos/manufacturing/iot_turbine/turbine", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- DBTITLE 1,Wind Turbine sensor 
CREATE STREAMING LIVE TABLE sensor_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL),
  CONSTRAINT correct_energy EXPECT (energy IS NOT NULL and energy > 0) ON VIOLATION DROP ROW
)
COMMENT "오토 로더를 사용하여 점진적으로 수집된 json 파일에서 가져온 원시 센서 데이터: 진동, 생성된 에너지 등 센서당 X초마다 1포인트."
AS SELECT * FROM cloud_files("/demos/manufacturing/iot_turbine/incoming_data", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- DBTITLE 1,Historical status
CREATE STREAMING LIVE TABLE historical_turbine_status (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "예측 유지보수 모델에서 레이블로 사용되는 터빈 상태(잠재적으로 결함이 있는 터빈을 알기 위해)"
AS SELECT * FROM cloud_files("/demos/manufacturing/iot_turbine/historical_turbine_status", "json", map("cloudFiles.inferColumnTypes" , "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 2/ 집계 계산: 시간 단위로 센서데이터 머지 
-- MAGIC 
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-2.png" width="700px" style="float: right"/></div>
-- MAGIC 
-- MAGIC 데이터를 분석할 수 있도록 표준 편차 및 사분위수와 같은 통계 메트릭을 매번 계산합니다.
-- MAGIC 
-- MAGIC *이 예제를 단순하게 유지하기 위해 모든 테이블을 다시 계산할 것입니다. 대신 상태 저장 집계* 를 사용하여  
-- MAGIC 현재 시간을 UPSERT할 수 있습니다.

-- COMMAND ----------

CREATE LIVE TABLE sensor_hourly (
  CONSTRAINT turbine_id_valid EXPECT (turbine_id IS not NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT timestamp_valid EXPECT (hourly_timestamp IS not NULL)  ON VIOLATION DROP ROW
)
COMMENT "신호를 설명하고 이상을 감지하는 데 사용되는 시간별 센서 통계"
AS
SELECT turbine_id,
      date_trunc('hour', from_unixtime(timestamp)) AS hourly_timestamp, 
      avg(energy)          as avg_energy,
      stddev_pop(sensor_A) as std_A,
      stddev_pop(sensor_B) as std_B,
      stddev_pop(sensor_C) as std_C,
      stddev_pop(sensor_D) as std_D,
      stddev_pop(sensor_E) as std_E,
      stddev_pop(sensor_F) as std_F,
      percentile_approx(sensor_A, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_A,
      percentile_approx(sensor_B, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_B,
      percentile_approx(sensor_C, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_C,
      percentile_approx(sensor_D, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_D,
      percentile_approx(sensor_E, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_E,
      percentile_approx(sensor_F, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_F
  FROM LIVE.sensor_bronze GROUP BY hourly_timestamp, turbine_id

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 3/ ML 엔지니어가 사용하는 테이블 작성: 센서 집계를 풍력 터빈 메타데이터 및 과거 상태와 조인
-- MAGIC 
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-3.png" width="700px" style="float: right"/></div>
-- MAGIC 
-- MAGIC 다음으로, 우리는 터빈 정보와 센서 집합체를 연결하는 파이널 테이블을 만들 것입니다.
-- MAGIC 
-- MAGIC 이 표에는 잠재적인 터빈 고장을 추론하는 데 필요한 모든 데이터가 포함됩니다.

-- COMMAND ----------

CREATE LIVE TABLE turbine_training_dataset 
COMMENT "신호를 설명하고 이상을 감지하는 데 사용되는 시간별 센서 통계"
AS
SELECT * except(t._rescued_data, s._rescued_data, m.turbine_id) FROM LIVE.sensor_hourly m
    INNER JOIN LIVE.turbine t USING (turbine_id)
    INNER JOIN LIVE.historical_turbine_status s ON m.turbine_id = s.turbine_id AND from_unixtime(s.start_time) < m.hourly_timestamp AND from_unixtime(s.end_time) > m.hourly_timestamp

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 4/ 레지스트리에서 모델 가져오기 및 플래그 결함 터빈 추가
-- MAGIC 
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-4.png" width="700px" style="float: right"/></div>
-- MAGIC 
-- MAGIC 데이터 과학자 팀은 이전 테이블에서 데이터를 읽고 Auto ML을 사용하여 예측 유지 관리 모델을 구축하고 Databricks 모델 레지스트리에 저장할 수 있었습니다(다음에 수행 방법을 살펴보겠습니다).
-- MAGIC 
-- MAGIC Lakehouse의 핵심 가치 중 하나는 이 모델을 쉽게 로드하고 파이프라인에 직접 결함이 있는 터빈을 예측할 수 있다는 것입니다.
-- MAGIC 
-- MAGIC 모델 프레임워크(sklearn 또는 기타)에 대해 걱정할 필요가 없으며 MLFlow가 이를 추상화합니다.
-- MAGIC 
-- MAGIC 모델을 로드하고 SQL 함수(또는 Python)로 호출하기만 하면 됩니다.

-- COMMAND ----------

-- DBTITLE 1,학습 모델을 로딩해서 SQL 함수로 저장 
-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC #                                                                              Stage/version  
-- MAGIC #                                                                 Model name         |        
-- MAGIC #                                                                     |              |        
-- MAGIC predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, "models:/dbdemos_turbine_maintenance/Production", "string") #, env_manager='virtualenv'
-- MAGIC spark.udf.register("predict_maintenance", predict_maintenance_udf)
-- MAGIC #Note that this cell is just as example (dlt will ignore it), python needs to be on a separate notebook and the real udf is declared in the companion UDF notebook

-- COMMAND ----------

CREATE LIVE TABLE turbine_current_status 
COMMENT "모델 예측에 기반한 풍력 터빈 마지막 상태"
AS
WITH latest_metrics AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY turbine_id, hourly_timestamp ORDER BY hourly_timestamp DESC) AS row_number FROM LIVE.sensor_hourly
)
SELECT * EXCEPT(m.row_number), 
    predict_maintenance(turbine_id, hourly_timestamp, avg_energy, std_A, percentiles_A, std_B, percentiles_B, std_C, percentiles_C, std_D, percentiles_D, std_E, percentiles_E, std_F, percentiles_F, location, model, state) as prediction 
  FROM latest_metrics m
   INNER JOIN LIVE.turbine t USING (turbine_id)
   WHERE m.row_number=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 결론
-- MAGIC 우리의 <a dbdemos-pipeline-id="dlt-iot-wind-turbine" href="#joblist/pipelines/119923a5-e6e5-49c9-98e1-116d4a4e22b3/updates/f4034c1c-ffe6-4cc0-bb51-ac0186368bcd" target="_blank">DLT 데이터 파이프라인</a>은 이제 순전히 SQL을 사용하여 준비되었습니다. 종단간의 파이프라인이 있고 ML 모델이 데이터 엔지니어링 팀에 의해 원활하게 통합되었습니다.
-- MAGIC 
-- MAGIC 모델 학습에 대한 자세한 내용은 [모델 학습 노트북]($../04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance)을 여세요.
-- MAGIC 
-- MAGIC 최종 데이터 세트에는 예측 유지 관리 사용 사례에 대한 ML 예측이 포함됩니다.
-- MAGIC 
-- MAGIC 이제 <a href="/sql/dashboards/cbe651dd-d990-400a-9484-0329671427d7?o=1444828305810485">DBSQL 대시보드</a>를 구축하여 전체 풍력 터빈 농장의 주요 KPI 및 상태를 추적하고 완전한 구축을 완료할 준비가 되었습니다. <a href="/sql/dashboards/73c61ddc-cf11-48e1-88ee-ad2f04ee6b9f">예측 유지 관리 DBSQL 대시보드</a>.
-- MAGIC 
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-1.png" width="1000px">

-- COMMAND ----------


