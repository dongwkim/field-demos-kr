-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # IOT 플랫폼의 거버넌스 및 보안 보장
-- MAGIC 
-- MAGIC 
-- MAGIC 완전한 데이터 플랫폼의 경우 데이터 거버넌스와 보안이 어렵습니다. 테이블에 대한 SQL GRANT로는 충분하지 않으며 여러 데이터 자산(대시보드, 모델, 파일 등)에 대해 보안을 적용해야 합니다.
-- MAGIC 
-- MAGIC 위험을 줄이고 혁신을 추진하기 위해 Emily의 팀은 다음을 수행해야 합니다.
-- MAGIC 
-- MAGIC - 모든 데이터 자산(테이블, 파일, ML 모델, 기능, 대시보드, 쿼리) 통합
-- MAGIC - 여러 팀의 온보딩 데이터
-- MAGIC - 외부 조직과 자산 공유 및 수익화
-- MAGIC 
-- MAGIC <style>
-- MAGIC .box{
-- MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
-- MAGIC .badge {
-- MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
-- MAGIC .badge_b { 
-- MAGIC   height: 35px}
-- MAGIC </style>
-- MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
-- MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
-- MAGIC   <div style="width:200px; float: left; text-align: center">
-- MAGIC     <div class="box" style="">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team A</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/da.png" style="" width="60px"> <br/>
-- MAGIC         데이터 분석가<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="" width="60px"> <br/>
-- MAGIC         데이터 과학자<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="" width="60px"> <br/>
-- MAGIC         데이터 엔지니어
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team B</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">...</div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">쿼리, 대시보드에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">테이블 , 컬럼, 로우에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">피쳐, ML 모델, 엔드포인트, 노트북 ... 에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">파일, 잡에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   <div class="box" style="width:550px; float: left">
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/gov.png" style="float: left; margin-right: 10px;" width="80px"> 
-- MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />거버넌스 및 보안</div>
-- MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
-- MAGIC       <ul style="line-height: 2px;">
-- MAGIC         <li>중앙 카탈로그 - 모든 데이터 자산</li>
-- MAGIC         <li>데이터 탐색 & 새로운 사용 사례를 발견 </li>
-- MAGIC         <li>팀간의 권한</li>
-- MAGIC         <li>오딧 로그로 위험 감소</li>
-- MAGIC         <li>lineage(계보)로 영향도 분석</li>
-- MAGIC       </ul>
-- MAGIC       + 수익 창출 및 외부 조직과 데이터 공유(Delta Sharing)
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Unity Catalog로 글로벌 데이터 거버넌스 및 보안 구현
-- MAGIC 
-- MAGIC <img style="float: right; margin-top: 30px" width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-2.png" />
-- MAGIC 
-- MAGIC Lakehouse가 Unity Catalog를 활용하여 이 문제를 어떻게 해결할 수 있는지 살펴보겠습니다.
-- MAGIC 
-- MAGIC 우리의 데이터는 데이터 엔지니어링 팀에 의해 델타 테이블로 저장되었습니다. 다음 단계는 팀 간 액세스를 허용하면서 이 데이터를 보호하는 것입니다. <br>
-- MAGIC 일반적인 설정은 다음과 같습니다.
-- MAGIC 
-- MAGIC * 데이터 엔지니어/Job은 기본 데이터/스키마(ETL 부분)를 읽고 업데이트할 수 있습니다.
-- MAGIC * 데이터 과학자는 최종 테이블을 읽고 피쳐 테이블을 업데이트할 수 있습니다.
-- MAGIC * 데이터 분석가는 데이터 엔지니어링 및 피쳐 테이블에 대한 읽기 액세스 권한이 있으며 별도의 스키마에서 추가 데이터를 수집/변환할 수 있습니다.
-- MAGIC * 데이터는 각 사용자 액세스 수준에 따라 동적으로 마스킹/익명화됩니다.
-- MAGIC 
-- MAGIC 이것은 Unity Catalog로 가능합니다. 테이블이 Unity 카탈로그에 저장되면 전체 조직, 교차 워크스페이스 및 교차 사용자가 테이블에 액세스할 수 있습니다.
-- MAGIC 
-- MAGIC Unity Catalog는 데이터 제품을 생성하거나 datamesh를 중심으로 팀을 구성하는 것을 포함하여 데이터 거버넌스의 핵심입니다. 다음을 제공합니다.
-- MAGIC 
-- MAGIC * 세분화된 ACL
-- MAGIC * 감사 로그
-- MAGIC * 데이터 계보
-- MAGIC * 데이터 탐색 및 발견
-- MAGIC * 외부 기관과 데이터 공유(Delta Sharing)

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup-uc $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## IoT 데이터베이스 탐색
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC 생성된 데이터를 검토해 보겠습니다.
-- MAGIC 
-- MAGIC Unity Catalog는 3개의 레이어로 작동합니다.
-- MAGIC 
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC 
-- MAGIC 모든 통합 카탈로그는 SQL에서 사용 가능 (`CREATE CATALOG IF NOT EXISTS my_catalog` ...)
-- MAGIC 
-- MAGIC 하나의 테이블에 액세스하려면 전체 경로를 지정하면 됩니다.: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`

-- COMMAND ----------

--CREATE CATALOG IF NOT EXISTS dongwook_demos;
USE CATALOG dongwook_demos;
SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 스키마 아래에서 생성한 테이블을 검토해 보겠습니다.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC Unity Catalog는 왼쪽 메뉴에서 액세스할 수 있는 포괄적인 Data Explorer를 제공합니다.
-- MAGIC 
-- MAGIC 모든 테이블을 찾을 수 있으며 이를 사용하여 테이블에 액세스하고 관리할 수 있습니다.
-- MAGIC 
-- MAGIC 이 스키마에 추가 테이블을 만들 수 있습니다.
-- MAGIC 
-- MAGIC ### 스키마 검색 가능
-- MAGIC 
-- MAGIC 또한 Unity 카탈로그는 스키마 탐색과 찾기도 제공합니다.
-- MAGIC 
-- MAGIC 테이블에 액세스할 수 있는 사람은 누구나 테이블을 검색하고 주요 용도를 분석할 수 있습니다. <br>
-- MAGIC 검색 메뉴(⌘ + P)를 사용하여 데이터 자산(테이블, 노트북, 쿼리...)을 탐색할 수 있습니다..)

-- COMMAND ----------

-- DBTITLE 1,테이블은 카탈로그상에서 사용할 수 있습니다.
CREATE SCHEMA IF NOT EXISTS lakehouse_iot;
USE lakehouse_iot;
SHOW TABLES IN lakehouse_iot;

-- COMMAND ----------

-- DBTITLE 1,분석가 및 데이터 엔지니어에게 액세스 권한 부여
-- Let's grant our ANALYSTS a SELECT permission:
-- Note: make sure you created an analysts and dataengineers group first.
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.turbine TO `analysts`;
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.turbine_status TO `analysts`;
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.parts TO `analysts`;
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.sensor_bronze TO `analysts`;
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.turbine_metrics_hourly TO `analysts`;
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.current_turbine_metrics TO `analysts`;
GRANT SELECT ON TABLE dbdemos.lakehouse_iot.historical_turbine_status TO `analysts`;

-- We'll grant an extra MODIFY to our Data Engineer
GRANT SELECT, MODIFY ON SCHEMA dbdemos.lakehouse_iot TO `dataengineers`;



-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Going further with Data governance & security
-- MAGIC 
-- MAGIC By bringing all your data assets together, Unity Catalog let you build a complete and simple governance to help you scale your teams.
-- MAGIC 
-- MAGIC Unity Catalog can be leveraged from simple GRANT to building a complete datamesh organization.
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
-- MAGIC 
-- MAGIC ### Fine-grained ACL
-- MAGIC 
-- MAGIC Need more advanced control? You can chose to dynamically change your table output based on the user permissions: `dbdemos.intall('uc-01-acl')`
-- MAGIC 
-- MAGIC ### Secure external location (S3/ADLS/GCS)
-- MAGIC 
-- MAGIC Unity Catatalog let you secure your managed table but also your external locations:  `dbdemos.intall('uc-02-external-location')`
-- MAGIC 
-- MAGIC ### Lineage 
-- MAGIC 
-- MAGIC UC automatically captures table dependencies and let you track how your data is used, including at a row level: `dbdemos.intall('uc-03-data-lineage')`
-- MAGIC 
-- MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
-- MAGIC 
-- MAGIC 
-- MAGIC ### Audit log
-- MAGIC 
-- MAGIC UC captures all events. Need to know who is accessing which data? Query your audit log:  `dbdemos.intall('uc-04-audit-log')`
-- MAGIC 
-- MAGIC This leat you analyze downstream impact, or monitor sensitive information across the entire organization (GDPR).
-- MAGIC 
-- MAGIC ### Upgrading to UC
-- MAGIC 
-- MAGIC Already using Databricks without UC? Upgrading your tables to benefit from Unity Catalog is simple:  `dbdemos.intall('uc-05-upgrade')`
-- MAGIC 
-- MAGIC ### Sharing data with external organization
-- MAGIC 
-- MAGIC Sharing your data outside of your Databricks users is simple with Delta Sharing, and doesn't require your data consumers to use Databricks:  `dbdemos.intall('delta-sharing-airlines')`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: Start building analysis with Databricks SQL
-- MAGIC 
-- MAGIC Now that these tables are available in our Lakehouse and secured, let's see how our Data Analyst team can start leveraging them to run BI workloads
-- MAGIC 
-- MAGIC Jump to the [BI / Data warehousing notebook]($../03-BI-data-warehousing/03-BI-Datawarehousing) or [Go back to the introduction]($../00-churn-introduction-lakehouse)
