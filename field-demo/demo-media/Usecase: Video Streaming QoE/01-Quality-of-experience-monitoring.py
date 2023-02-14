# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md #Monitoring Video Streaming Quality of Experience
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fnotebook_qoe&dt=MEDIA_USE_CASE">
# MAGIC <!-- [metadata={"description":"Quality of Experience end 2 end demo.</i>",
# MAGIC  "authors":["dan.morris@databricks.com"],
# MAGIC   "db_resources":{"Dashboards": ["Video QoE: Health"], "Queries": ["Root Cause Analysis"], "Alerts": ["Playback failure alert"]},
# MAGIC   "search_tags":{"vertical": "media", "step": "Data Engineering", "components": ["sklearn", "mlflow", "streaming"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC **Use Case Overview**
# MAGIC * User engagement is the lifeblood of every video viewing experience and outside of the content itself, there is nothing more harmful to user engagement than quality of experience issues like video start failures, playback failures, and rebuffering. At the same time, content delivery is extremely complex and is dependent upon a myriad of factors - some within control (e.g. content rendition) and others not (e.g. viewer's network bandwidth, internet service provider (ISP) / content delivery network (CDN) issues). Data is THE solution for managing this effectively and minimizing the impact that quality of experience issues have on the video viewing experience.
# MAGIC 
# MAGIC **Business Impact of Solution**
# MAGIC * **Mitigate User Churn:** companies risk losing users/subscribers when video streaming quality of experiences issues are severe or recurring.
# MAGIC * **Protect Advertising Revenue:** companies risk losing advertising revenue when video viewing is hindered by a poor experience or an outage.
# MAGIC 
# MAGIC **About the Demo**
# MAGIC * In this demo, we simulate the occurence of an internet service provider (ISP) outage by showing an automated email trigged by anomaly detection. Upon receiving the alert, we deeplink into a Databricks SQL Dashboard to take a closer look. We then hop over to the SQL IDE to conduct root cause analysis. After simulating this scenario, we then walk through the process of how it all works using the capabilities provided by Databricks.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/video_qoe_isp_outage.png"; width=100%>
# MAGIC <a href='https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/bd205017-3a2a-401b-81c4-f0e04cf3e8e6-video-qoe-health?o=1444828305810485'>Link to Dashboard</a>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Setting up Video Quality of Experience Monitoring
# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/qoe-full.png" width="80%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Stream Data Into Delta Lake
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/qoe-full-1.png" width="80%">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Delta Lake mitigates common challenges associated with streaming
# MAGIC %md
# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/video_qoe_structured_streaming.png"; width=40%>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Read Stream: Autoloader
source_data_df = (
  spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", cloud_storage_path+"/schema_bronze")
        .option("cloudFiles.schemaHints", "average_bitrate_kbps int, avg_bandwidth_kbps int, rebuffer_stats struct<severity:string, ratio:float, seconds:float, count:int>, video_start_failure long, playback_failure long, exit_before_video_start long, time_to_first_frame float, downshift_cnt long")
        .option("cloudFiles.maxFilesPerTrigger", "1")  #demo only, remove in real stream
        .load("/mnt/field-demos/media/qoe/incoming_data")
)
display(source_data_df)

# COMMAND ----------

# DBTITLE 1,Write to Bronze Table
source_data_df.writeStream \
              .format("delta") \
              .option("checkpointLocation", cloud_storage_path+"/checkpoints/bronze") \
              .table("qoe_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Apply In-Stream Transformations
# MAGIC 
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/qoe-full-2.png" width="80%">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Explode Struct Columns and Write to Silver Table 
bronze_df_exploded = (
  spark.readStream.table("qoe_bronze")
       .withColumn("rebuffer_count", col("rebuffer_stats.count"))
       .withColumn("rebuffer_ratio", col("rebuffer_stats.ratio"))
       .withColumn("rebuffer_seconds", col("rebuffer_stats.seconds"))
       .withColumn("rebuffer_severity", col("rebuffer_stats.severity"))
       .drop("rebuffer_stats")
       .na.fill(0)
)

bronze_df_exploded.writeStream \
  .option("checkpointLocation", cloud_storage_path+"/checkpoints/silver") \
  .table("qoe_silver")

# COMMAND ----------

# DBTITLE 1,View Silver Table
# MAGIC %sql select * from qoe_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Train Anomaly Detection Model
# MAGIC 
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/qoe-full-3.png" width="80%">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Prepare Data for Model Training
df = spark.table('qoe_silver').toPandas()

# COMMAND ----------

metrics_df = df[['scenario','video_start_failure','playback_failure','exit_before_video_start',
                 'time_to_first_frame','downshift_cnt','rebuffer_ratio','rebuffer_count']].fillna(0)
base_case_df = metrics_df[metrics_df['scenario'] == 'base_case'].drop(['scenario'],axis=1)

# COMMAND ----------

# DBTITLE 1,Train Isolation Forest
from mlflow.models.signature import infer_signature

with mlflow.start_run():
  mlflow.sklearn.autolog(silent=True)
  max_samples = 100
  random_state = np.random.RandomState(42)
  contamination = 0.01
  
  clf = IsolationForest(max_samples=max_samples, random_state=random_state, contamination=contamination)
  clf.fit(base_case_df)
  
  mlflow.set_tag("field_demos", "qoe_monitoring")

# COMMAND ----------

# DBTITLE 1,Select and Register Best Model
best_model = mlflow.search_runs(filter_string='attributes.status = "FINISHED" and tags.field_demos = "qoe_monitoring"', max_results=1).iloc[0]
model_registered = mlflow.register_model(f"runs:/{best_model.run_id}/model", "field_demos_qoe_monitoring")

# COMMAND ----------

# DBTITLE 1,Push Model to Production
client = mlflow.tracking.MlflowClient()

client.transition_model_version_stage(name = "field_demos_qoe_monitoring", 
                                      version = model_registered.version, 
                                      stage = "Production", 
                                      archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Detect Anomalies in Near Real-Time
# MAGIC 
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/qoe-full-4.png" width="80%">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Create detect_anomalies UDF
#                                                                                    Stage/version
#                                                                 Model name               |
#                                                                     |                    |
detect_anomalies = mlflow.pyfunc.spark_udf(spark, 'models:/field_demos_qoe_monitoring/Production')
#We can also register the model as SQL function
spark.udf.register("detect_anomalie", detect_anomalies)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *, detect_anomalie(video_start_failure, playback_failure, exit_before_video_start, time_to_first_frame, downshift_cnt, rebuffer_ratio, rebuffer_count) as predictions  FROM qoe_silver;

# COMMAND ----------

# DBTITLE 1,Apply UDF in a stream for Real-time Anomaly Detection
input_column_names = mlflow.pyfunc.load_model('models:/field_demos_qoe_monitoring/Production').metadata.get_input_schema().input_names()

# Apply pyfunc_udf for real-time anomaly detection
stream = spark.readStream.table('qoe_silver').withColumn('predictions', detect_anomalies(*input_column_names))
stream.writeStream.option("checkpointLocation", cloud_storage_path+"/checkpoints/gold").table('qoe_anomalies_prediction')

# COMMAND ----------

# DBTITLE 1,Display Anomaly Detection Results
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW play_attempts AS (
# MAGIC   SELECT isp_code, state, city, count(*) AS total_play_attempts
# MAGIC   FROM qoe_anomalies_prediction
# MAGIC   GROUP BY isp_code, state, city);
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW anomalies AS (
# MAGIC   SELECT isp_code, state, city, count(*) AS anomaly_cnt
# MAGIC   FROM qoe_anomalies_prediction
# MAGIC   WHERE predictions = -1
# MAGIC   GROUP BY isp_code, state, city);
# MAGIC 
# MAGIC SELECT a.isp_code,a.state,a.city, total_play_attempts, anomaly_cnt, round(anomaly_cnt/total_play_attempts,2) as pct_anomaly
# MAGIC FROM play_attempts p INNER JOIN anomalies a
# MAGIC WHERE p.isp_code = a.isp_code
# MAGIC AND p.state = a.state
# MAGIC AND p.city = a.city
# MAGIC ORDER BY pct_anomaly desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Perform Root Cause Analysis
# MAGIC 
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/qoe-full-5.png" width="80%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: left">
# MAGIC   <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/video_qoe_isp_outage.png"; width=100%>
# MAGIC </div>
# MAGIC <a href='https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/bd205017-3a2a-401b-81c4-f0e04cf3e8e6-video-qoe-health?o=1444828305810485'>Link to Dashboard</a>
# MAGIC </br>
# MAGIC <a href='https://e2-demo-field-eng.cloud.databricks.com/sql/queries/fce3c9ed-d1e6-4e2f-a055-542dbe26c7d9/source?o=1444828305810485'>Link to SQL IDE</a>
