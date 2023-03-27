# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Deploying our model in production
# MAGIC 
# MAGIC This is the last step of our pipeline, and one of the most complicated:
# MAGIC 
# MAGIC * The model should be used in batch or real time, based on the use-case
# MAGIC * Model should be packaged and deployed with all the dependencies
# MAGIC * To simplify integration, model flavor/framework should be abstracted
# MAGIC * Model serving monitoring is required to track model behavior over time
# MAGIC * Deploying a new model in production should be 1-click, without downtime
# MAGIC * Model governance is required to understand which model is deployed and how it has been trained, ensure schema etc.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-7.png" width="1000"/>
# MAGIC 
# MAGIC Once our model in the registry, Databricks simplify these final steps with a 1-click deployment for 
# MAGIC 
# MAGIC * Batch inference (high throughput but >sec latencies)
# MAGIC * Real-time (ms latencies)
# MAGIC 
# MAGIC Let's see how this can be done
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fboat_satellite_imaging%2Fnotebook_serving&dt=MANUFACTURING_BOAT_SERVING">
# MAGIC <!-- [metadata={"description":"Model serving for images - Boat Satellite Images.",
# MAGIC  "authors":["tarik.boukherissa@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Recreation
# MAGIC 
# MAGIC Handling dependencies is often tricky and time consuming. MLFlow solves this challenge by keeping track of the dependencies used saving the model.
# MAGIC 
# MAGIC We can easyly load them from our registry and use pip to install them in our cluster:

# COMMAND ----------

import os
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

local_path = ModelsArtifactRepository("models:/field_demos_satellite_segmentation/Production").download_artifacts("") # download model from remote registry

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# DBTITLE 1,Install model requirement for batch inferences
# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md ## Running inference at scale - batch mode
# MAGIC 
# MAGIC Databricks let you run inferences at scale. This is ideal and very cost efficient to detect our boat with a streaming or batch job (ex: every hours).
# MAGIC 
# MAGIC The first step is to retrieve our model from MLFlow registry and save the function as a pandas_udf. 
# MAGIC 
# MAGIC Once it's done, this function can be used by an analyst or a data engineer in a pipeline to run our inferences at scale.

# COMMAND ----------

# DBTITLE 1,Define a User Define Function to run the inferences at scale
model_uri = "models:/field_demos_satellite_segmentation/Production"

@pandas_udf("binary")
def detect_boat(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    #load the model from the registry
    model = mlflow.pyfunc.load_model(model_uri)
    for s in iterator:
        yield model.predict(s)
#save the function as SQL udf
spark.udf.register("detect_boat", detect_boat)

# COMMAND ----------

# DBTITLE 1,Run the inferences as simple python or SQL query
spark.table("gold_satellite_image").limit(20) \
      .withColumn('prediction', detect_boat(col('content')).alias('prediction', metadata=image_meta)) \
      .select('image_id', 'content', 'prediction').display()

# COMMAND ----------

# MAGIC %md ## Running inference in real-time - Model Serving
# MAGIC 
# MAGIC The second option is to use Databricks Model Serving capabilities. Within 1 click, Databricks will start a serverless REST API serving the model define in MLFlow.
# MAGIC 
# MAGIC Open your model registry and click on Model Serving.
# MAGIC 
# MAGIC This will grant you real-time capabilities without any infra setup.

# COMMAND ----------

# DBTITLE 1,Real-time Inferences with Python REST api on our model serving endpoint
#our content_base64 has been defined in the helper notebook. Headers contain our credential to access the model.
model_answer = r.get(f"https://{host_name}/model/field_demos_satellite_segmentation_rt/Staging/invocations", headers = headers, data = {"content": content_base64})
display_realtime_inference(content_base64, model_answer)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Conclusion
# MAGIC 
# MAGIC That's it, we have build an end 2 end pipeline to incrementally ingest our dataset, clean it and train a Deep Learning model. The model is now deployed and ready for production-grade usage.
# MAGIC 
# MAGIC Databricks Lakehouse accelerate your team and simplify the go-to production:
# MAGIC 
# MAGIC 
# MAGIC * Unique ingestion and data preparation capabilities with Delta Live Table, making Data Engineering accessible to all
# MAGIC * Ability to support all use-cases ingest and process structured and non structured dataset
# MAGIC * Advanced ML capabilities for ML training
# MAGIC * MLOps coverage to let your Data Scientist team focus on what matters (improving your business) and not operational task
# MAGIC * Support for all type of production deployment to cover all your use case, without external tools
# MAGIC * Security and compliance covered all along, from data security (table ACL) to model governance
# MAGIC 
# MAGIC 
# MAGIC As result, Teams using Databricks are able to deploy in production advanced ML projects in a matter of weeks, from ingestion to model deployment, drastically accelerating business.
