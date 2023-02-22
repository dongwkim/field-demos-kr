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
-- MAGIC 어떤 풍력 터빈이 잠재적으로 고장날지 예측할 수 있는 것은 풍력 터빈 농장 효율성을 높이는 첫 번째 단계에 불과합니다.
-- MAGIC 
-- MAGIC 잠재적 유지보수를 예측하는 모델을 구축할 수 있게 되면 예비 부품 재고를 동적으로 조정하고 적절한 장비를 갖춘 유지보수 팀을 자동으로 파견할 수도 있습니다.
-- MAGIC 
-- MAGIC 레이크하우스가 어떻게 그렇게 할 수 있는지 봅시다

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 데모:  IoT 데이터베이스를 구축하고 Databricks Lakehouse로 불량을 사전에 예측합니다.
-- MAGIC 
-- MAGIC 이 데모에서는 전국에 걸쳐서 풍력 발전기를 관리하는 기업이 되어 보겠습니다. 
-- MAGIC 
-- MAGIC 경영진들은 회사의 비즈니스는 부품 불량율을 줄이고 사전에 이를 예측하는데 중점을 두어야 한다고 결정했으며 우리는 다음을 요청받습니다.
-- MAGIC 
-- MAGIC * 현재 불량 부품 분석 및 설명: 불량율, 추세 및 비즈니스에 미치는 영향을 정량화
-- MAGIC * 재고, 전력량등을 예측하고 선재적으로 대응하기 위한 시스템을 구축합니다.
-- MAGIC 
-- MAGIC 
-- MAGIC ### 우리가 만들어야 할 것
-- MAGIC 
-- MAGIC 이를 위해 Lakehouse와 함께 종단 간 솔루션을 구축할 것입니다. 풍력 발전기에서 발생하는 데이터를 분석하고 예측할 수 있으려면 다양한 외부 시스템에서 오는 정보가 필요합니다. 터빈 메타데이터, 터빈 센서 스트림, 과거 터빈 상태 기록
-- MAGIC 
-- MAGIC 대략적으로 구현할 흐름은 다음과 같습니다.
-- MAGIC 
-- MAGIC <img width="900px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-0.png"/>
-- MAGIC 
-- MAGIC <!-- <img width="900px" src="#Workspace/Repos/dongwook.kim@databricks.com/field-demos-kr/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-0.png"/> -->
-- MAGIC 
-- MAGIC 1. 데이터를 수집하고 c360 데이터베이스를 생성하며, SQL에서 쉽게 쿼리할 수 있는 테이블을 사용해야 합니다.
-- MAGIC 2. 데이터를 보호하고 데이터 분석가 및 데이터 과학 팀에 대한 읽기 액세스 권한을 부여합니다.
-- MAGIC 3. BI 쿼리를 실행하여 기존 이탈 분석
-- MAGIC 4. ML 모델을 구축하여 이탈할 고객과 그 이유를 예측합니다.
-- MAGIC 
-- MAGIC 그 결과, 터빈의 장애율을 낮추기 위해 작업을 트리거하는 데 필요한 모든 정보(부품 번호, 재고량, 영향도 ...)를 갖게 됩니다.
-- MAGIC 
-- MAGIC ### 보유한 데이터 세트
-- MAGIC 
-- MAGIC 이 데모를 단순화하기 위해 외부 시스템이 주기적으로 Blob Storage(S3/ADLS/GCS)로 데이터를 전송한다고 가정합니다.
-- MAGIC 
-- MAGIC - 터빈 메타데이터: 터빈 ID, 위치 (터빈 당 하나의 행)
-- MAGIC - 터빈 센서 스트림: 풍력 터빈 센서로 부터 발생하는 실시간 스트리밍(진동, 에너지 소모, 속도 등)
-- MAGIC - 터빈 상태: 어떤 부품에 결함이 있는지 분석하기 위한 과거 터빈 상태(ML 모델에서 레이블로 사용됨)
-- MAGIC 
-- MAGIC *기술적으로 데이터는 모든 소스에서 가져올 수 있습니다. Databricks는 모든 시스템(SalesForce, Fivetran, kafka와 같은 대기열 메시지, Blob 저장소, SQL 및 NoSQL 데이터베이스...)에서 데이터를 수집할 수 있습니다.*
-- MAGIC 
-- MAGIC Lakehouse 내에서 이 데이터를 사용하여 풍력 발전기 상태를 분석하고 발전기 중단을 줄이는 방법을 살펴보겠습니다!

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ 데이터 수집 및 준비(데이터 엔지니어링)
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-2.png" />
-- MAGIC 
-- MAGIC 
-- MAGIC <br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC 첫 번째 단계는 데이터 분석가 팀이 분석을 수행할 수 있도록 원시 데이터를 수집하고 클린징 하는 것 입니다. 
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
-- MAGIC 
-- MAGIC ### 델타 레이크 (Delta Lake)
-- MAGIC 
-- MAGIC Lakehouse에서 생성할 모든 테이블은 Delta Lake 테이블로 저장됩니다. <br> [Delta Lake](https://delta.io)는 안정성과 성능을 위한 개방형 스토리지 프레임워크이며 많은 기능을 제공합니다 *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
-- MAGIC 
-- MAGIC ### 델타 라이브 테이블(DLT)로 수집 간소화
-- MAGIC 
-- MAGIC Databricks는 SQL 사용자가 배치 또는 스트리밍으로 고급 파이프라인을 만들 수 있도록 허용하여 Delta Live Tables로 데이터 수집 및 변환을 간소화합니다. 이 엔진은 파이프라인 배포 및 테스트를 간소화하고 운영 복잡성을 줄여주므로 비즈니스 혁신에 집중하고 데이터 품질을 보장할 수 있습니다.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the customer churn 
-- MAGIC   <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/119923a5-e6e5-49c9-98e1-116d4a4e22b3/updates/e84095da-4d5a-4b4a-abf5-1ec55e7de373" target="_blank">Delta Live Table pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-Wind-Turbine-SQL)  <br>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ 데이터 보호 및 거버넌스 (Unity Catalog)
-- MAGIC 
-- MAGIC <img style="float: left" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-6.png" />
-- MAGIC 
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC   이제 첫 번째 테이블이 생성되었으므로 데이터 분석가 팀에 고객 이탈 정보 분석을 시작할 수 있도록 읽기(READ) 액세스 권한을 부여해야 합니다.
-- MAGIC   
-- MAGIC    Unity Catalog가 데이터 리니지 및 감사 로그를 포함하여 데이터 자산 전체에 **보안 및 거버넌스** 를 제공하는 방법을 살펴보겠습니다.
-- MAGIC   
-- MAGIC    Unity Catalog는 스택에 관계없이 모든 외부 조직과 데이터를 공유하는 개방형 프로토콜인 Delta Sharing을 통합합니다.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-security-churn) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ 발전기 상태 분석  (BI / Data warehousing / SQL) 
-- MAGIC 
-- MAGIC <img width="600px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/manufacturing/lakehouse-iot/lakehouse-iot-dbsql-dashboard.png"  style="float: right; margin: 50px 5px 5px;"/>
-- MAGIC 
-- MAGIC 
-- MAGIC <!-- <img width="400px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/manufacturing/lakehouse-iot/lakehouse-iot-faulty-dbsql-dashboard.png"  style="float: left; margin-right: 10px"/> -->
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC 우리의 데이터 세트는 이제 고품질로 적절하게 수집되고 보호되며 조직 내에서 쉽게 검색할 수 있습니다.
-- MAGIC 
-- MAGIC **데이터 분석가**는 이제 즉각적인 중지 및 시작을 제공하는 Serverless Datawarehouse를 포함하여 대기 시간이 짧고 처리량이 높은 BI 대화형 쿼리를 실행할 준비가 되었습니다.
-- MAGIC 
-- MAGIC PowerBI, Tableau 등과 같은 외부 BI 솔루션을 포함하여 Databricks를 사용하여 데이터 웨어하우징을 수행하는 방법을 살펴보겠습니다!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Datawarehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing-iot-turbine) to start running your BI queries or access or directly open the <a href="/sql/dashboards/2fb6a294-0233-4ae5-8edd-9d25fbd94074?edit&o=1444828305810485" target="_blank">Churn Analysis Dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Data Science 및 Auto-ML 을 이용한 이탈 예측
-- MAGIC 
-- MAGIC <img width="400px" style="float: left; margin-right: 10px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-4.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC 과거 데이터에 대한 분석을 실행할 수 있어 이미 비즈니스를 추진할 수 있는 많은 통찰력을 얻었습니다. 어떤 고객이 이탈하는지 더 잘 이해할 수 있고 이탈 영향을 평가할 수 있습니다.
-- MAGIC 
-- MAGIC 그러나 불량이 있다는 사실을 아는 것만으로는 충분하지 않습니다. 이제 이를 한 단계 더 발전시켜 불량 위험이 있는 발전기를 파악하고 장애율을 줄이기 위한 **예측 모델** 을 구축해야 합니다.
-- MAGIC 
-- MAGIC 여기에서 Lakehouse의 가치가 나타납니다. 동일한 플랫폼 내에서 누구나 **AutoML**을 사용한 로우 코드 솔루션을 포함하여 이러한 분석을 실행하기 위해 ML 모델 구축을 시작할 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how to train an ML model within 1 click with the [04.1-automl-churn-prediction notebook]($./04-Data-Science-ML/04.1-automl-churn-prediction)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 예측을 기반으로 장애를 줄이기 위한 작업 자동화
-- MAGIC 
-- MAGIC 
-- MAGIC <img style="float: right;margin-left: 10px" width="400px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/manufacturing/lakehouse-iot/lakehouse-iot-faulty-dbsql-dashboard.png">
-- MAGIC 
-- MAGIC 이제 부품 불량을 분석하고 예측하는 엔드 투 엔드 데이터 파이프라인이 있습니다. 이제 비즈니스를 기반으로 장애를 줄이기 위한 조치를 쉽게 트리거할 수 있습니다.
-- MAGIC 
-- MAGIC - 산업재해 예방을 위한 가스/휘발유 파이프라인의 밸브 고장 예측
-- MAGIC - 생산 라인의 이상 동작을 감지하여 제품의 제조 결함을 제한하고 방지합니다.
-- MAGIC - 더 큰 고장이 발생하기 전에 조기에 수리하여 더 많은 수리 비용과 잠재적인 제품 중단으로 이어짐
-- MAGIC 
-- MAGIC 이러한 작업은 이 데모의 범위를 벗어나며 단순히 ML 모델의 Churn 예측 필드를 활용합니다.
-- MAGIC 
-- MAGIC ## 기후데이터를 기반으로 다음 달 에너지 영향도 추적
-- MAGIC 
-- MAGIC 기상 데이터와 과거 발전량 데이터를 결합하여 다음달 예상되는 에너지 생산량을 예측 가능합니다.
-- MAGIC 
-- MAGIC Lakehouse로 생성된 파이프라인은 강력한 ROI를 제공할 것입니다. 이 파이프라인 엔드 투 엔드를 설정하는 데 몇 시간이 걸렸고 월 $129,914의 잠재적인 이익을 얻었습니다!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <a href='/sql/dashboards/2fb6a294-0233-4ae5-8edd-9d25fbd94074?o=1444828305810485' target="_blank">불량 예측 DBSQL 대쉬보드</a> 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ 전체 워크플로 배포 및 오케스트레이션
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 10px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-5.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC 데이터 파이프라인이 거의 완성되었지만 프로덕션에서 전체 워크플로를 오케스트레이션 하는 마지막 단계가 누락되었습니다.
-- MAGIC 
-- MAGIC Databricks Lakehouse를 사용하면 작업을 실행하기 위해 외부 오케스트레이터를 관리할 필요가 없습니다. **Databricks Workflows**는 고급 경고, 모니터링, 분기 옵션 등을 통해 모든 작업을 단순화합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./05-Workflow-orchestration/05-Workflow-orchestration-churn) to schedule our pipeline (data ingetion, model re-training etc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 결론
-- MAGIC 
-- MAGIC 우리는 통합되고 안전한 단일 플랫폼을 사용하여 Lakehouse로 엔드 투 엔드 파이프라인을 구현하는 방법을 시연했습니다.
-- MAGIC 
-- MAGIC - 데이터 수집
-- MAGIC - 데이터 분석 / DW / BI
-- MAGIC - 데이터 사이언스/ML
-- MAGIC - 워크플로우 및 오케스트레이션
-- MAGIC 
-- MAGIC 그 결과 분석가 팀은 단순히 시스템을 구축하여 향후 변동을 이해하고 예측하고 그에 따라 조치를 취할 수 있었습니다.
-- MAGIC 
-- MAGIC 이것은 Databricks 플랫폼에 대한 소개일 뿐입니다. 보더 자세한 내용은 영업팀에 문의하고 `dbdemos.list()`로 더 많은 데모를 살펴보십시오.

-- COMMAND ----------


