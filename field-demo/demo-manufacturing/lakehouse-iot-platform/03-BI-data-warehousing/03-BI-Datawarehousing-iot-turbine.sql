-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC # Your Lakehouse is the best Warehouse
-- MAGIC 
-- MAGIC 기존 데이터 웨어하우스는 다양한 데이터 및 사용 사례를 따라갈 수 없습니다. 비즈니스 민첩성을 위해서는 ML 모델의 통찰력을 갖춘 신뢰할 수 있는 실시간 데이터가 필요합니다.
-- MAGIC 
-- MAGIC 레이크하우스와 함께 작업하면 기존 BI 분석의 잠금을 해제할 수 있을 뿐만 아니라 완전한 보안을 유지하면서 전체 데이터에 직접 연결하는 실시간 애플리케이션도 사용할 수 있습니다.
-- MAGIC 
-- MAGIC <br>
-- MAGIC 
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/dbsql.png" width="700px" style="float: left" />
-- MAGIC 
-- MAGIC <div style="float: left; margin-top: 240px; font-size: 23px">
-- MAGIC   즉각적이고 유연한 컴퓨트 자원<br>
-- MAGIC   서버리스로 TCO 절감<br>
-- MAGIC   운영 관리 최소화<br><br>
-- MAGIC 
-- MAGIC   거버넌스 레이어 - row level<br><br>
-- MAGIC 
-- MAGIC   데이터 및 스키마 (star, data vault…)
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # BI & Datawarehousing with Databricks SQL
-- MAGIC 
-- MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-3.png" />
-- MAGIC 
-- MAGIC 우리의 데이터 세트는 이제 고품질로 적절하게 수집되고 보호되며 조직 내에서 쉽게 검색할 수 있습니다.
-- MAGIC 
-- MAGIC Databricks SQL이 대화형 BI로 데이터 분석가 팀을 어떻게 지원하고 고객 이탈 분석을 시작하는지 살펴보겠습니다.
-- MAGIC 
-- MAGIC Databricks SQL을 시작하려면 왼쪽 상단 메뉴에서 SQL 보기를 엽니다.
-- MAGIC 
-- MAGIC 다음을 수행할 수 있습니다.
-- MAGIC 
-- MAGIC - 쿼리를 실행할 SQL Warehouse 생성
-- MAGIC - DBSQL을 사용하여 나만의 대시보드 구축
-- MAGIC - 모든 BI 도구(Tableau/PowerBI/..)를 연결하여 분석 실행

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Databricks SQL Warehouse: 동급 최고의 BI 엔진
-- MAGIC 
-- MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://www.databricks.com/wp-content/uploads/2022/06/how-does-it-work-image-5.svg" />
-- MAGIC 
-- MAGIC Databricks SQL은 모든 도구, 쿼리 유형 및 실제 응용 프로그램에 대해 최고의 성능을 제공하기 위해 수천 가지의 최적화가 포함된 웨어하우스 엔진입니다. <a href='https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html'>데이터 웨어하우징 성능 기록을 달성했습니다.</ 가>
-- MAGIC 
-- MAGIC 여기에는 SQL 웨어하우스와 함께 다른 클라우드 데이터 웨어하우스보다 최대 12배 더 나은 가격 대비 성능을 제공하는 차세대 벡터화 쿼리 엔진인 Photon이 포함됩니다.
-- MAGIC 
-- MAGIC **서버리스 웨어하우스**는 스토리지와 분리된 즉각적이고 탄력적인 SQL 컴퓨팅을 제공하며 높은 동시성 사용 사례를 위해 중단 없이 무제한 동시성을 제공하도록 자동 확장됩니다.
-- MAGIC 
-- MAGIC 타협하지 마십시오. 최고의 Datawarehouse는 Lakehouse입니다.
-- MAGIC 
-- MAGIC ### SQL 웨어하우스 생성
-- MAGIC 
-- MAGIC SQL Wharehouse는 databricks에서 관리합니다. [웨어하우스 만들기](/sql/warehouses)는 원클릭 단계입니다.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Creating your first Query
-- MAGIC 
-- MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-query.png" />
-- MAGIC 
-- MAGIC 이제 사용자는 SQL 편집기를 사용하여 SQL 쿼리 실행을 시작하고 새로운 시각화를 추가할 수 있습니다.
-- MAGIC 
-- MAGIC 자동 완성 및 스키마 브라우저를 활용하여 데이터 위에서 임시 쿼리 실행을 시작할 수 있습니다.
-- MAGIC 
-- MAGIC 이는 Data Analyst가 고객 이탈 분석을 시작하는 데 이상적이지만 다른 페르소나도 DBSQL을 활용하여 데이터 수집 파이프라인, 데이터 품질, 모델 동작 등을 추적할 수 있습니다.
-- MAGIC 
-- MAGIC [쿼리 메뉴](/sql/queries)를 열어 첫 번째 분석 작성을 시작합니다.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Creating our IoT Dashboard
-- MAGIC 
-- MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/manufacturing/lakehouse-iot/lakehouse-iot-faulty-dbsql-dashboard.png" />
-- MAGIC 
-- MAGIC 다음 단계는 이제 비즈니스에서 추적할 수 있는 포괄적인 SQL 대시보드에서 쿼리와 해당 시각화를 조합하는 것입니다.
-- MAGIC 
-- MAGIC 대쉬보드는 이미 만들어져 있습니다. [DBSQL Churn Dashboard](/sql/dashboards/2fb6a294-0233-4ae5-8edd-9d25fbd94074?edit&o=1444828305810485) 

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Using Third party BI tools
-- MAGIC 
-- MAGIC <iframe style="float: right" width="560" height="315" src="https://www.youtube.com/embed/EcKqQV0rCnQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
-- MAGIC 
-- MAGIC SQL 웨어하우스는 Tableau 또는 PowerBI와 같은 외부 BI 도구와 함께 사용할 수도 있습니다.
-- MAGIC 
-- MAGIC 이렇게 하면 통합 보안 모델 및 Unity 카탈로그(예: SSO를 통해)를 사용하여 테이블 위에서 직접 쿼리를 실행할 수 있습니다. 이제 분석가는 선호하는 도구를 사용하여 가장 완전하고 최신 데이터에서 새로운 비즈니스 통찰력을 발견할 수 있습니다.
-- MAGIC 
-- MAGIC 창고를 타사 BI 도구와 함께 사용하려면 왼쪽 하단의 "파트너 연결"을 클릭하고 공급자를 선택하십시오.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## DBSQL 및 Databricks Warehouse로 더 나아가기
-- MAGIC 
-- MAGIC Databricks SQL은 훨씬 더 많은 기능을 제공하며 전체 웨어하우스 기능을 제공합니다.
-- MAGIC 
-- MAGIC <img style="float: right" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-pk-fk.png" />
-- MAGIC 
-- MAGIC ### 데이터 모델링
-- MAGIC 
-- MAGIC 포괄적인 데이터 모델링. 요구사항에 따라 데이터 저장: Data vault, Star schema, Inmon...
-- MAGIC 
-- MAGIC Databricks를 사용하면 PK/FK, ID 컬럼(자동 증가)을 만들 수 있습니다. `dbdemos.install('identity-pk-fk')`
-- MAGIC 
-- MAGIC ### DBSQL 및 DBT로 쉽게 데이터 수집
-- MAGIC 
-- MAGIC 턴키 기능을 통해 분석가와 분석 엔지니어는 Fivetran을 사용하여 Salesforce, Google Analytics 또는 Marketo와 같은 엔터프라이즈 애플리케이션에 클라우드 스토리지와 같은 모든 데이터를 쉽게 수집할 수 있습니다. 클릭 한 번이면 됩니다.
-- MAGIC 
-- MAGIC 그런 다음 Lakehouse(Delta Live Table)의 기본 제공 ETL 기능을 사용하거나 동급 최고의 성능을 위해 Databricks SQL의 dbt와 같은 즐겨 사용하는 도구를 사용하여 종속성을 관리하고 데이터를 변환할 수 있습니다.
-- MAGIC 
-- MAGIC ### Federation Query 
-- MAGIC 
-- MAGIC 시스템 간 데이터에 액세스해야 합니까? Databricks SQL 쿼리 페더레이션을 사용하면 Databricks 외부에서 데이터 원본을 정의할 수 있습니다(예: PostgreSQL).
-- MAGIC 
-- MAGIC ### Materialized View
-- MAGIC 
-- MAGIC 비용이 많이 드는 쿼리를 피하고 테이블을 구체화하십시오. 엔진은 데이터가 업데이트될 때 필요한 항목만 다시 계산합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # 분석을 한 단계 더 발전시켜 보기: 부품 불량 예측
-- MAGIC 
-- MAGIC 과거 데이터에 대한 분석을 실행할 수 있다는 것은 이미 많은 통찰력을 제공합니다. 어떤 부품에서 불량이 발생하는지 더 잘 이해할 수 있고 불량에 대한 영향도를 평가할 수 있습니다.
-- MAGIC 
-- MAGIC 그러나 불량이 있다는 사실을 아는 것만으로는 충분하지 않습니다. 이제 우리는 이를 한 단계 더 발전시키고 불량 위험이 있는 부품을 선별하여 사전에 예방할 수 있는  예측 모델을 구축해야 합니다.
-- MAGIC 
-- MAGIC [Databricks 기계 학습 노트북]($../04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance) | [서론으로 돌아가기]($../00-IOT-wind-turbine-introduction-lakehouse)

-- COMMAND ----------


