# Databricks notebook source
# MAGIC %md
# MAGIC # IoT Faulty Prediction Inference - Batch or serverless real-time
# MAGIC 
# MAGIC 
# MAGIC AutoML을 사용하면 최상의 모델이 MLFlow 레지스트리에 자동으로 저장됩니다.
# MAGIC 
# MAGIC 이제 우리가 해야 할 일은 이 모델을 사용하여 추론을 실행하는 것입니다. 간단한 해결책은 데이터 엔지니어링 팀과 모델 이름을 공유하는 것입니다. 그러면 그들이 유지 관리하는 파이프라인 내에서 이 모델을 호출할 수 있습니다. 이것이 Delta Live Table 파이프라인에서 수행한 작업입니다!
# MAGIC 
# MAGIC 또는 별도의 작업에서 예약할 수 있습니다. 다음은 MLFlow를 직접 사용하여 모델을 검색하고 추론을 실행하는 방법을 보여주는 예입니다.
# MAGIC 
# MAGIC 
# MAGIC ## Environment Recreation
# MAGIC 
# MAGIC 아래 셀은 `conda.yaml` 및 `requirements.txt` 파일을 포함하는 원격 레지스트리의 모델과 연결된 모델 아티팩트를 다운로드합니다. 이 노트북에서 `pip`는 기본적으로 종속성을 다시 설치하는 데 사용됩니다.

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

local_path = ModelsArtifactRepository("models:/dongwook_lakehouse_iot_turbine_faulty/Production").download_artifacts("") # 원격 레지스트리에서 모델 다운로드

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC 
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC 
# MAGIC 이제 모델을 레지스트리에서 사용할 수 있으므로 모델을 로드하여 추론을 계산하고 테이블에 저장하여 대시보드 구축을 시작할 수 있습니다.
# MAGIC 
# MAGIC MLFlow 기능을 사용하여 pyspark UDF를 로드하고 추론을 전체 클러스터에 배포합니다. 데이터가 작은 경우 일반 Python으로 모델을 로드하고 pandas Dataframe을 사용할 수도 있습니다.
# MAGIC 
# MAGIC 시작 방법을 모르는 경우 Databricks는 모델 레지스트리에서 클릭 한 번으로 배치 추론 노트북을 생성할 수 있습니다. MLFlow 모델 레지스트리를 열고 "추론을 위한 사용자 모델" 버튼을 클릭하세요!

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC 먼저 Spark UDF로 로드하고 SQL 함수에서 직접 호출하는 방법을 살펴보겠습니다.

# COMMAND ----------

import mlflow
#                                                                                       Stage/version
#                                                                      Model name             |
#                                                                           |                 |
predict_maintenance = mlflow.pyfunc.spark_udf(spark, "models:/dongwook_lakehouse_iot_turbine_faulty/Production", "string")
#We can use the function in SQL
spark.udf.register("predict_maintenance", predict_maintenance)

# COMMAND ----------

# DBTITLE 1,Run inferences
columns = predict_maintenance.metadata.get_input_schema().input_names()
spark.table(f'dongwook_demos.turbine_hourly_features').withColumn("dbdemos_turbine_maintenance", predict_maintenance(*columns)).display()

# COMMAND ----------

# DBTITLE 1,Or in SQL directly
# MAGIC %sql
# MAGIC SELECT turbine_id, predict_maintenance(turbine_id, hourly_timestamp, avg_energy, std_sensor_A, percentiles_A, std_sensor_B, percentiles_B, std_sensor_C, percentiles_C, 
# MAGIC std_sensor_D, percentiles_D, std_sensor_E, percentiles_E, std_sensor_F, percentiles_F, location, model, state) as prediction FROM dongwook_demos.turbine_hourly_features

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC 작은 데이터 세트가 있는 경우 단일 노드 및 pandas API를 사용하여 세그먼트를 계산할 수도 있습니다.

# COMMAND ----------

model = mlflow.pyfunc.load_model("models:/dongwook_lakehouse_iot_turbine_faulty/Production")
df = spark.table(f'dongwook_demos.turbine_hourly_features').select(*columns).limit(10).toPandas()
df['churn_prediction'] = model.predict(df)
df

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Realtime model serving with Databricks serverless serving
# MAGIC 
# MAGIC <img style="float: right; margin-left: 20px" width="800" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-model-serving.gif" />
# MAGIC 
# MAGIC Databricks는 서버리스 서비스도 제공합니다.
# MAGIC 
# MAGIC 모델 서빙을 클릭하고 실시간 서버리스를 활성화하면 엔드포인트가 생성되어 한 번의 클릭으로 REST API를 통해 서비스를 제공합니다.
# MAGIC 
# MAGIC Databricks Serverless는 대기 시간이 짧은 모델 서비스를 유지하면서 동급 최고의 TCO를 제공하기 위해 트래픽이 없을 때 0으로 축소하는 것을 포함하여 자동 크기 조정을 제공합니다.

# COMMAND ----------

import  pandas
dataset = spark.table(f'dongwook_demos.turbine_hourly_features').select(*columns).limit(1).toPandas()
pandas.DataFrame.to_json(dataset, orient='records')
# dataset.to_json()

# COMMAND ----------

# DBTITLE 1,Call the REST API deployed using standard python
import os
import requests
import numpy as np
import pandas as pd
import json

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}


def score_model(dataset):
  url = f'https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/model-endpoint/dongwook_lakehouse_iot_turbine_faulty/Production/invocations'
  headers = {'Authorization': f'Bearer {dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}', 'Content-Type': 'application/json'}
  ds_dict = {'dataframe_split': dataset.to_dict(orient='split')} if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  data_json = json.dumps(ds_dict, allow_nan=True)
  response = requests.request(method='POST', headers=headers, url=url, data=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

#Start your model endpoint in the model menu and uncomment this line to test realtime serving!
# score_model(dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Next step: Leverage inferences and automate action to lower cost
# MAGIC 
# MAGIC ## Automate action to reduce churn based on predictions
# MAGIC 
# MAGIC We now have an end 2 end data pipeline analizing and predicting churn. We can now easily trigger actions to reduce the churn based on our business:
# MAGIC 
# MAGIC - Send targeting email campaign to the customer the most likely to churn
# MAGIC - Phone campaign to discuss with our customers and understand what's going
# MAGIC - Understand what's wrong with our line of product and fixing it
# MAGIC 
# MAGIC These actions are out of the scope of this demo and simply leverage the Churn prediction field from our ML model.
# MAGIC 
# MAGIC ## Track churn impact over the next month and campaign impact
# MAGIC 
# MAGIC Of course, this churn prediction can be re-used in our dashboard to analyse future churn and measure churn reduction. 
# MAGIC 
# MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $129,914 / month!
# MAGIC 
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/manufacturing/lakehouse-iot/lakehouse-iot-faulty-dbsql-dashboard.png">
# MAGIC 
# MAGIC <a href='/sql/dashboards/1e236ef7-cf58-4bfc-b861-5e6a0c105e51'>Open the Churn prediction DBSQL dashboard</a> | [Go back to the introduction]($../00-churn-introduction-lakehouse)

# COMMAND ----------


