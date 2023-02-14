# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Setup notebook
# MAGIC This notebook will ensure the cluster has the proper settings, and install required lib.
# MAGIC 
# MAGIC It'll also automatically download the data from kaggle if it's not available locally (please set your kaggle credential in the `_kaggle_credential` companion notebook)
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_setup&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"Companion notebook to setup libs and dependencies.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

# DBTITLE 1,This demo requires specific settings, let's make sure it's define at the cluster level
wrong_serializer = True
try:
  serializer = spark.conf.get("spark.serializer") 
  wrong_serializer = serializer != "org.apache.spark.serializer.KryoSerializer"
except:
  pass
if wrong_serializer:
  print("ERROR: you need to set spark.serializer to org.apache.spark.serializer.KryoSerializer in your cluster configuration")

wrong_serializer_setting = True
try:
  serializer = spark.conf.get("spark.kryoserializer.buffer.max")
  wrong_serializer_setting = serializer != "2000M"
except:
  pass
if wrong_serializer_setting:
  print("ERROR: you need to set spark.kryoserializer.buffer.max to 2000M in your cluster configuration")
    
if wrong_serializer_setting or wrong_serializer:
  raise RuntimeError("Wrong cluster setup. Make sure you set spark.serializer to org.apache.spark.serializer.KryoSerializer and spark.kryoserializer.buffer.max to 2000M in your cluster configuration and restart your cluster")

# COMMAND ----------

# DBTITLE 1,Making sure spark NLP is installed at the cluster level
# MAGIC %scala
# MAGIC try {
# MAGIC     Class.forName("com.johnsnowlabs.nlp.embeddings.BertEmbeddings");
# MAGIC } catch {
# MAGIC     case _: Throwable => {
# MAGIC       val error = "Can't load Johnsnowlabs. Make sure it's loaded in the cluster (com.johnsnowlabs.nlp:spark-nlp-spark32_xxxxx)"
# MAGIC       println(error)
# MAGIC       throw new RuntimeException(error)
# MAGIC     }
# MAGIC } 

# COMMAND ----------

# DBTITLE 1,Install sparknlp python binding library
# MAGIC %pip install spark-nlp==3.4.2

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=media

# COMMAND ----------

try:
  path = "/mnt/field-demos/media/toxicity"
  dbutils.fs.ls(path)
  print(f"Using default path {path} as raw data location")
  #raw data is set to the current user location where we just uploaded all the data
  raw_data_location = "/mnt/field-demos/media/toxicity"
except:
  print(f"Couldn't find data saved in the default mounted bucket. Will download it from Kaggle instead under {cloud_storage_path}.")
  print("Note: you need to specify your Kaggle Key under ./_resources/_kaggle_credential ...")
  result = dbutils.notebook.run("./_resources/01_download", 300, {"cloud_storage_path": cloud_storage_path + "/toxicity"})
  if result is not None and "ERROR" in result:
    print("-------------------------------------------------------------")
    print("---------------- !! ERROR DOWNLOADING DATASET !!-------------")
    print("-------------------------------------------------------------")
    print(result)
    print("-------------------------------------------------------------")
    raise RuntimeError(result)
  else:
    print(f"Success. Dataset downloaded from kaggle and saved under {cloud_storage_path}.")
  raw_data_location = cloud_storage_path + "/toxicity"


# COMMAND ----------

import re
import sparknlp
from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *
from concurrent.futures import ThreadPoolExecutor
from collections import deque

from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import MultilabelMetrics
from pyspark.sql.functions import lit,when,col,array,array_contains,array_remove,regexp_replace,size,when
from pyspark.sql.types import ArrayType,DoubleType,StringType

from pyspark.ml.evaluation import MultilabelClassificationEvaluator

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
import pyspark.sql.functions as F

@pandas_udf('array<string>')
def get_class_label(s: pd.Series, labels: pd.Series) -> pd.Series:
    fs = labels.iloc[0]
    return s.apply(lambda arr: [fs[i] for i, value in enumerate(arr) if value == 1])
  


# COMMAND ----------

# DBTITLE 1,Custom transformer to clean sparkNLP output
import uuid
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

#Helper to convert cleanup the output from spark NLP to classes
class ToClassTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
  # lazy workaround - a transformer needs to have these attributes
  #_defaultParamMap = dict()
  #_paramMap = dict()
  #_params = dict()
  labels = Param(Params._dummy(), "labels", "labels", typeConverter=TypeConverters.toListString)
    
  @keyword_only
  def __init__(self, labels=None):
      super(ToClassTransformer, self).__init__()
      self.stopwords = Param(self, "labels", "")
      self._setDefault(labels=[])
      kwargs = self._input_kwargs
      self.setParams(**kwargs)
      #self.uid = str(uuid.uuid1())

  @keyword_only
  def setParams(self, labels=None):
      kwargs = self._input_kwargs
      return self._set(**kwargs)

  def setLabels(self, value):
      return self._set(labels=list(value))

  def getLabels(self):
      return self.getOrDefault(self.labels)

  def _transform(self, dataset):
    when_case = [when(array_contains(col('class.result'), f),1).otherwise(0).cast(DoubleType()) for f in self.getLabels()]
    return dataset.withColumn('label_pred', array(*when_case))
