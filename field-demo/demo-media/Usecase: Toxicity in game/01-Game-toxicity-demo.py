# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakehouse for Gaming - Analysing game toxicity
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-lakehouse.png" width="800"/>
# MAGIC 
# MAGIC ## Analyzing in game chat with Spark-NLP
# MAGIC 
# MAGIC TODO: add business intro for in-game toxicity analysis here. What's the challenge, why is it important
# MAGIC 
# MAGIC In this demo, we'll be ingesting in game chat messaging and score these message as being toxic or not.
# MAGIC 
# MAGIC This will allow us to then build dashboard and take actions to increase our player retention but also reduce brand image risks.
# MAGIC 
# MAGIC We'll be implementing this workflow in 2 steps:
# MAGIC * 1/ ingest our training data (from kaggle, see companion notebook to download the data), and train a NLP model
# MAGIC * 2/ Load this model and use it to score our in-game chat dataset in near-realtime and build analysis on top of it
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-toxicity-flow-0.png" width="1000"/>
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_main&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"End to end gaming toxicity demo: analyse message with NLP to detect and reduce toxic player behavior.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technical requirement for this demo
# MAGIC 
# MAGIC **Note: Feel free to remove this cell during the demo** 
# MAGIC 
# MAGIC As this demo is doing advanced NLP computation, we need to install an extra library. Databricks ML Runtime makes it very simple using the notebook libraries.
# MAGIC 
# MAGIC   * To use this notebook, the cluster must be configured to support Spark NLP. Make sure you install `com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.2` in your cluster (Open your cluster setup, Library, install new and select maven)<br/>
# MAGIC     Maven Coordinates:
# MAGIC       * CPU: `com.johnsnowlabs.nlp:spark-nlp-spark32_2.12:3.4.2`
# MAGIC       * GPU: `com.johnsnowlabs.nlp:spark-nlp-gpu-spark32_2.12:3.4.2`   
# MAGIC   * **Install libraries:**
# MAGIC     * PyPi: `spark-nlp` (it's executed for you in the setup notebook)  
# MAGIC   * **Add critical Spark config:**
# MAGIC     *  `spark.serializer org.apache.spark.serializer.KryoSerializer`
# MAGIC     *  `spark.kryoserializer.buffer.max 2000M`
# MAGIC     
# MAGIC *A note on cluster instances: A CPU or GPU cluster can be used to run this notebook. We found that leveraging GPU-based clusters with the GPU spark-nlp jar from maven trained in 1/3rd of the time compared to the CPU-based training*
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Setup data. Make sure you add your Kaggle credential under ./resource/_kaggle_credential
# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1: Stream Data Into Delta Lake using AutoLoader
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-toxicity-flow-1.png" width="600"/>
# MAGIC 
# MAGIC Our first step is to ingest raw data from blob storage into our first Delta Lake Table. To do so, we'll use Databricks Autoloader (`cloudFiles`).
# MAGIC 
# MAGIC Autoloader simplify ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files.
# MAGIC 
# MAGIC As we have multiple tables to ingest, let's create a small python loop to go over all the folders and ingest all of them in 1 command.

# COMMAND ----------

# DBTITLE 1,Ingesting incremental data using the Autoloader
def load_content(file):
  input_path = f"{raw_data_location}/{file}/"
  bronze_table_name = f"toxicity_{file}_bronze"
  print(f"loading data under {input_path} as table {bronze_table_name}...")
  spark.readStream.format("cloudFiles") \
          .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schemas/{file}") \
          .option("cloudFiles.format", "csv") \
          .option("escape", "\"").option("multiline", True).option("header", "true") \
          .load(input_path) \
       .writeStream \
          .trigger(once=True) \
          .option("checkpointLocation", f"{cloud_storage_path}/checkpoints/{file}/") \
          .table(bronze_table_name).awaitTermination()
  
#Starts 3 thread at the same time to quickly ingest our data
with ThreadPoolExecutor(max_workers=3) as executor:
  folders = ['test', 'train', 'match', 'match_outcomes', 'player_ratings', 'players', 'chat', 'cluster_regions']
  deque(executor.map(load_content, folders))

# COMMAND ----------

# DBTITLE 1,View Bronze Tables
# MAGIC %sql
# MAGIC -- view result as visualization: Bar Chart => X = Region, Y = #of message / players
# MAGIC SELECT region,  count(distinct account_id) `# of players`,  count(key) `# of messages`
# MAGIC FROM toxicity_chat_bronze chat
# MAGIC   JOIN toxicity_players_bronze players USING (match_id)
# MAGIC   JOIN toxicity_match_bronze match USING (match_id)
# MAGIC   JOIN toxicity_cluster_regions_bronze regions USING (cluster)
# MAGIC GROUP BY region
# MAGIC ORDER BY count(account_id) desc, count(account_id) desc

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 2: Apply In-Stream Transformations
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-toxicity-flow-2.png" width="600"/>
# MAGIC 
# MAGIC Once the data is loaded, we can start preparing our dataset for Model Training. 
# MAGIC 
# MAGIC The first thing to do is to cast all our labels as Double.
# MAGIC 
# MAGIC We'll also need to add a `label` column containing the class names, and a `label_true` columns with the class represented as a vector.

# COMMAND ----------

labels = ['toxic', 'severe_toxic', 'obscene', 'threat', 'insult', 'identity_hate']

df = spark.readStream.table("toxicity_train_bronze")
#Let's convert all our classes as float instead of string
for label in labels:
  df = df.withColumn(label, col(label).cast(DoubleType()))

#merge the classes in a single column
assembler = VectorAssembler(inputCols=labels, outputCol='label_true')
df = assembler.transform(df)
#build the dataframe with the classes and also the equivalent labels
df = df.withColumn("label_true", vector_to_array(col("label_true"))) \
       .withColumn("labels", get_class_label(col("label_true"), F.array([lit(l) for l in labels])))

# COMMAND ----------

df.writeStream \
  .option("checkpointLocation", cloud_storage_path+"/checkpoints/toxicity_silver_training") \
  .trigger(once=True) \
  .table("silver_training").awaitTermination()

# COMMAND ----------

# DBTITLE 1,View Silver Table
# MAGIC %sql select * from silver_training where toxic = 0

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ###  Step 3: Train Toxicity Classification Model
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-toxicity-flow-3.png" width="600"/>
# MAGIC 
# MAGIC We now have our dataset ready to train our model!
# MAGIC 
# MAGIC Let's split the dataset in training / testing, and create our NLP pipeline using the Spark-NLP library

# COMMAND ----------

# DBTITLE 1,Prepare Data for Model Training
#note: to make this demo train faster, we can limit the number of training document to 1000: .limit(1000)
train, val = spark.table("silver_training").randomSplit([0.8,0.2],42)

# COMMAND ----------

# DBTITLE 1,Model Pipeline
# MAGIC %md
# MAGIC * The models built in this notebook uses Spark NLP to classify toxic comments and is an adaptation of a [demo notebook](https://nlp.johnsnowlabs.com/2021/01/21/multiclassifierdl_use_toxic_sm_en.html) published by John Snow Labs.
# MAGIC * Spark NLP is an open source library that is built on top of Apache Spark&trade; and Spark ML. A few benefits of using Spark NLP include:
# MAGIC   * `Start-of-the-Art:` pre-trained algorithms available out-of-the-box
# MAGIC   * `Efficient:` single processing framework mitigates serializing/deserializing overhead
# MAGIC   * `Enterprise Ready:` successfully deployed by many large enterprises.
# MAGIC   
# MAGIC * Further information on Spark-NLP and more can be found [here](https://towardsdatascience.com/introduction-to-spark-nlp-foundations-and-basic-components-part-i-c83b7629ed59).
# MAGIC   * [Transformers documentation](https://nlp.johnsnowlabs.com/docs/en/transformers) used in pipeline
# MAGIC   * [Annotators documentation](https://nlp.johnsnowlabs.com/docs/en/annotators) used in pipeline
# MAGIC 
# MAGIC Lets jump in and build our pipeline. 
# MAGIC   * [Document Assembler](https://nlp.johnsnowlabs.com/docs/en/transformers#documentassembler-getting-data-in) creates the first annotation of type Document from the contents of our dataframe. This is used by the annotators in subsequent steps.
# MAGIC   * Embeddings map words to vectors. A great explanation on this topic can be found [here](https://towardsdatascience.com/word-embeddings-exploration-explanation-and-exploitation-with-code-in-python-5dac99d5d795). The embeddings serve as an input for our classifer.
# MAGIC   
# MAGIC <div><img src="https://cme-solution-accelerators-images.s3-us-west-2.amazonaws.com/toxicity/nlp_pipeline.png"; width="70%"></div>

# COMMAND ----------

# DBTITLE 1,Define pipeline & classifier
#We'll fix hyperparameters for this demo, but a production-grade implementation should do some hyperparam tuning (ex: hyperopt)
max_epochs = 10
lr = 1e-3
batch_size = 32
threshold = 0.7

document_assembler = DocumentAssembler() \
  .setInputCol("comment_text") \
  .setOutputCol("document")

universal_embeddings = UniversalSentenceEncoder.pretrained() \
  .setInputCols(["document"]) \
  .setOutputCol("universal_embeddings")

classifierDL = MultiClassifierDLApproach() \
  .setInputCols(["universal_embeddings"]) \
  .setOutputCol("class") \
  .setLabelColumn("labels") \
  .setMaxEpochs(max_epochs) \
  .setLr(lr) \
  .setBatchSize(batch_size) \
  .setThreshold(threshold) \
  .setOutputLogsPath('./') \
  .setEnableOutputLogs(False)

#Custom transformer to display nicer output as class, see companion notebook for more details
toClassTransformer = ToClassTransformer().setLabels(labels)

endToEndPipeline = Pipeline(stages=[
  document_assembler,
  universal_embeddings,
  classifierDL,
  toClassTransformer
])

# COMMAND ----------

# DBTITLE 1,Train classification model & register model
from mlflow.models.signature import infer_signature
with mlflow.start_run() as run:
  
  client = mlflow.tracking.MlflowClient()
  model_name = 'Toxicity MultiLabel Classification'
  
  mlflow.spark.autolog()
  mlflow.tensorflow.autolog(log_models = False)
  mlflow.log_param('threshold', threshold)
  mlflow.log_param('batchSize', batch_size)
  mlflow.log_param('maxEpochs', max_epochs)
  mlflow.log_param('learningRate', lr)
  
  model = endToEndPipeline.fit(train)
  
  #supports "f1" (default), "weightedPrecision", "weightedRecall", "accuracy"
  evaluator = MultilabelClassificationEvaluator(labelCol="label_true", predictionCol="label_pred")
  predictions = model.transform(val)
  
  score = evaluator.evaluate(predictions)
  mlflow.log_metric('f1', score)
  
  extra_pip_requirements = [f"sparknlp=={sparknlp.version()}"]
  input_example = train.select("comment_text").limit(1).toPandas() #pandas
  signature = infer_signature(input_example, predictions.limit(10).toPandas()) #pandas
  mlflow.spark.log_model(model, "spark_nlp_model", dfs_tmpdir = "/tmp/nlp_model", extra_pip_requirements=extra_pip_requirements, input_example=input_example, signature=signature)
  
  mlflow.set_tag("field_demos", model_name)
  run_id = run.info.run_id
  
display(predictions)

# COMMAND ----------

# DBTITLE 1,MLOps cycle: deploy Model to registry and move it as Production
#Once our model ready and validated, we'll deploy it in the registry and flag it as Production ready
model_registered = mlflow.register_model("runs:/"+run_id+"/spark_nlp_model", "field_demos_game_toxicity")
#Move it as Production
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_game_toxicity", version = model_registered.version, stage = "Production")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ###  Step 4: Load model from MLFlow registry and detect Toxicity in Real Time
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-toxicity-flow-4.png" width="600"/>
# MAGIC 
# MAGIC Now that our model is trained, it can be loaded as part of a Data Engineer pipeline and used to make inferences in a streaming pipeline.
# MAGIC 
# MAGIC This is one of the key value of the Lakehouse: Data Engineers can load and deploy models being designed by other teams without friction, acceleration project delivery.

# COMMAND ----------

# DBTITLE 1,Load classification model from MLflow Model Registry
model = mlflow.spark.load_model('models:/field_demos_game_toxicity/Production')

# COMMAND ----------

# DBTITLE 1,Perform Classification in Real-Time
# Reading the stream of messages. For the demo we do a repartition to spread the load on multiple small tasks
raw_comments = spark.readStream.table("toxicity_chat_bronze").withColumnRenamed('key', 'comment_text').repartition(500)

# Using the classification model in stream
comments_pred = model.transform(raw_comments) \
                     .withColumnRenamed('key', 'comment_text') \
                     .drop('document', 'token', 'universal_embeddings')

# Writing the output of the classification model to a delta for analysis
comments_pred.writeStream \
  .trigger(once=True) \
  .option("checkpointLocation", cloud_storage_path+"/checkpoints/toxicity_chat_pred") \
  .table("silver_chat_pred")

# COMMAND ----------

# DBTITLE 1,Let's visualize our Toxicity predictions
# MAGIC %sql select * from silver_chat_pred 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 5: Analyze Toxicity impact
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/gaming-toxicity-flow-5.png" width="600"/>
# MAGIC 
# MAGIC We now have a full pipeline ingesting chat messaging in near-realtime and saving output to our Delta Table.
# MAGIC 
# MAGIC This table can now be shared with external teams (Data Analysts) to start performing adhoc reporting and improving games to ultimately increase gamer retention and reduce brand image risks
# MAGIC 
# MAGIC <a href='https://adb-984752964297111.11.azuredatabricks.net/sql/dashboards/ace22295-717d-42ca-b1c3-5d4560eeef37-toxicity?o=984752964297111'>Link to Dashboard</a>
# MAGIC </br>
# MAGIC <a href='https://adb-984752964297111.11.azuredatabricks.net/sql/queries?o=984752964297111&page=1&page_size=20&q=toxicity'>Link to SQL IDE</a>

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2021]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Spark-nlp|Apache-2.0 License| https://github.com/JohnSnowLabs/spark-nlp/blob/master/LICENSE | https://github.com/JohnSnowLabs/spark-nlp/
# MAGIC |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle/kaggle-api|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
