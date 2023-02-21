# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Data Science with Databricks
# MAGIC 
# MAGIC ## ML 은 개인화의 핵심
# MAGIC 
# MAGIC C360 데이터베이스를 수집하고 쿼리할 수 있는 것이 첫 번째 단계이지만 경쟁이 치열한 시장에서 경쟁우위를 하기에는 충분하지 않습니다.
# MAGIC 
# MAGIC 이제 고객은 실시간 개인화와 새로운 형태의 커뮤니케이션을 기대합니다. 현재적인 데이터 회사는 AI를 통해 이를 달성합니다.
# MAGIC 
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:650px;height:300px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 35px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="font-family: 'DM Sans'">
# MAGIC   <div style="width: 700px; color: #1b3139; margin-left: 50px; float: left">
# MAGIC     <div style="color: #ff5f46; font-size:80px">90%</div>
# MAGIC     <div style="font-size:20px;  margin-top: -20px; line-height: 30px;">
# MAGIC       엔터프라이즈 애플리케이션은 2025년까지 AI로 강화될 예정 —IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:80px">$10T+</div>
# MAGIC     <div style="font-size:20px;  margin-top: -20px; line-height: 30px;">
# MAGIC        2030년 AI가 창출할 것으로 예상되는 비즈니스 가치 —PWC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC   <div class="right_box" style="font-size:30px">
# MAGIC       그러나 ML을 대규모로 작동시키는데는 <br/>큰 어려움이 있습니다!<br/><br/>
# MAGIC       대부분의 ML 프로젝트는 <br/>프로덕션에 들어가기 전에 여전히 실패합니다.
# MAGIC   </div>
# MAGIC   
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC ## Machine learning is data + transforms.
# MAGIC 
# MAGIC 비즈니스 라인에 가치를 제공하는 것은 모델 구축에 관한 것만이 아니기 때문에 ML은 어렵습니다. <br>
# MAGIC ML 수명 주기는 데이터 파이프라인으로 구성됩니다. 데이터 사전 처리, 피쳐 엔지니어링, 학습, 추론, 모니터링, 재학습...<br>
# MAGIC 모든 파이프라인은 데이터 + 코드입니다.
# MAGIC 
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-4.png" />
# MAGIC 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="float: left;" width="80px"> 
# MAGIC <h3 style="padding: 10px 0px 0px 5px">Marc는 데이터 과학자로서 모든 ML 및 DS 단계를 가속화하는 데이터 + ML 플랫폼이 필요합니다.:</h3>
# MAGIC 
# MAGIC <div style="font-size: 19px; margin-left: 73px; clear: left">
# MAGIC <div class="badge_b"><div class="badge">1</div> 실시간 처리를 지원하는 데이터 파이프라인 구축(with DTL)</div>
# MAGIC <div class="badge_b"><div class="badge">2</div> 데이터 탐색</div>
# MAGIC <div class="badge_b"><div class="badge">3</div> 피쳐 생성</div>
# MAGIC <div class="badge_b"><div class="badge">4</div> 빌드 & 모델 학습</div>
# MAGIC <div class="badge_b"><div class="badge">5</div> 모델 배포(배치 또는 실시간)</div>
# MAGIC <div class="badge_b"><div class="badge">6</div> 모니터링</div>
# MAGIC </div>
# MAGIC 
# MAGIC **Marc는 데이터 레이크 하우스가 필요합니다**.  Lakehouse 내 프로덕션에서 Churn 모델을 배포하는 방법을 살펴보겠습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Churn Prediction - Single click deployment with AutoML
# MAGIC 
# MAGIC Let's see how we can now leverage the C360 data to build a model predicting and explaining customer Churn.
# MAGIC 
# MAGIC Our first step as Data Scientist is to analyze and build the features we'll use to train our model.
# MAGIC 
# MAGIC The users table enriched with churn data has been saved within our Delta Live Table pipeline. All we have to do is read this information, analyze it and start an Auto-ML run.
# MAGIC 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-churn-ds-flow.png" width="1000px">
# MAGIC 
# MAGIC *Note: Make sure you switched to the "Machine Learning" persona on the top left menu.*
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fautoml&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %run ../_resources/02-create-churn-tables $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data exploration and analysis
# MAGIC 
# MAGIC Let's review our dataset and start analyze the data we have to predict our churn

# COMMAND ----------

# DBTITLE 1,Read our churn gold table
# Read our churn_features table
churn_dataset = spark.table("churn_features")
display(churn_dataset)

# COMMAND ----------

# DBTITLE 1,Data Exploration and analysis
import seaborn as sns
g = sns.PairGrid(churn_dataset.sample(0.01).toPandas()[['age_group','gender','order_count']], diag_sharey=False)
g.map_lower(sns.kdeplot)
g.map_diag(sns.kdeplot, lw=3)
g.map_upper(sns.regplot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Further data analysis and preparation using pandas API
# MAGIC 
# MAGIC Because our Data Scientist team is familiar with Pandas, we'll use `pandas on spark` to scale `pandas` code. The Pandas instructions will be converted in the spark engine under the hood and distributed at scale.
# MAGIC 
# MAGIC Typicaly Data Science project would involve more advanced preparation and likely require extra data prep step, including more complex feature preparation. We'll keep it simple for this demo.
# MAGIC 
# MAGIC *Note: Starting from `spark 3.2`, koalas is builtin and we can get an Pandas Dataframe using `pandas_api()`.*

# COMMAND ----------

# DBTITLE 1,Custom pandas transformation / code on top of your entire dataset
# Convert to koalas
dataset = churn_dataset.pandas_api()
dataset.describe()  
# Drop columns we don't want to use in our model
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'last_event'])
# Drop missing values
dataset = dataset.dropna()   

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Write to Feature Store (Optional)
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC Once our features are ready, we'll save them in Databricks Feature Store. Under the hood, features store are backed by a Delta Lake table.
# MAGIC 
# MAGIC This will allow discoverability and reusability of our feature accross our organization, increasing team efficiency.
# MAGIC 
# MAGIC Feature store will bring traceability and governance in our deployment, knowing which model is dependent of which set of features. It also simplify realtime serving.
# MAGIC 
# MAGIC Make sure you're using the "Machine Learning" menu to have access to your feature store using the UI.

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table(f'{database}.churn_user_features')
except:
  pass
#Note: You might need to delete the FS table using the UI
churn_feature_table = fs.create_table(
  name=f'{database}.churn_user_features',
  primary_keys='user_id',
  schema=dataset.spark.schema(),
  description='These features are derived from the churn_bronze_customers table in the lakehouse.  We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not.  No aggregations were performed.'
)

fs.write_table(df=dataset.to_spark(), name=f'{database}.churn_user_features', mode='overwrite')
features = fs.read_table(f'{database}.churn_user_features')
display(features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Accelerating Churn model creation using MLFlow and Databricks Auto-ML
# MAGIC 
# MAGIC MLflow is an open source project allowing model tracking, packaging and deployment. Everytime your datascientist team work on a model, Databricks will track all the parameter and data used and will save it. This ensure ML traceability and reproductibility, making it easy to know which model was build using which parameters/data.
# MAGIC 
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC 
# MAGIC While Databricks simplify model deployment and governance (MLOps) with MLFlow, bootstraping new ML projects can still be long and inefficient. 
# MAGIC 
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC 
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC 
# MAGIC 
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC 
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC ### Using Databricks Auto ML with our Churn dataset
# MAGIC 
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`churn_features`)
# MAGIC 
# MAGIC Our prediction target is the `churn` column.
# MAGIC 
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC 
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

# DBTITLE 1,We have already started a run for you, you can explore it here:
#This calls databricks.automl.classify(...) under the hood or . See companion notebook for more detail.
display_automl_churn_link(dataset = fs.read_table(f'{database}.churn_user_features'), model_name = "dbdemos_customer_churn", force_refresh = False)

# COMMAND ----------

# MAGIC %md
# MAGIC AutoML saved our best model in the MLFlow registry. [Open the dbdemos_customer_churn model](#mlflow/models/dbdemos_customer_churn) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.
# MAGIC 
# MAGIC If we're ready, we can move this model into Production stage in a click, or using the API.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### The model generated by AutoML is ready to be used in our DLT pipeline to detect customers about to churn.
# MAGIC 
# MAGIC Our Data Engineer can now easily retrive the model `dbdemos_customer_churn` from our Auto ML run and predict churn within our Delta Live Table Pipeline.<br>
# MAGIC Re-open the DLT pipeline to see how this is done.
# MAGIC 
# MAGIC #### Track churn impact over the next month and campaign impact
# MAGIC 
# MAGIC This churn prediction can be re-used in our dashboard to analyse future churn, take actiond and measure churn reduction. 
# MAGIC 
# MAGIC The pipeline created with the Lakehouse will offer a strong ROI: it took us a few hours to setup this pipeline end 2 end and we have potential gain for $129,914 / month!
# MAGIC 
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
# MAGIC 
# MAGIC <a href='/sql/dashboards/1e236ef7-cf58-4bfc-b861-5e6a0c105e51'>Open the Churn prediction DBSQL dashboard</a> | [Go back to the introduction]($../00-churn-introduction-lakehouse)
# MAGIC 
# MAGIC #### More advanced model deployment (batch or serverless realtime)
# MAGIC 
# MAGIC We can also use the model `dbdemos_custom_churn` and run our predict in a standalone batch or realtime inferences! 
# MAGIC 
# MAGIC Next step:  [Explore the generated Auto-ML notebook]($./04.2-automl-generated-notebook) and [Run inferences in production]($./04.3-running-inference)
