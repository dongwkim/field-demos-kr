# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Implementing and deploying our pytorch model
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-6.png" width="700" style="float:right; margin-left: 10px"/>
# MAGIC 
# MAGIC Our next step as Data Scientist is to implement a ML model to run image segmentation.
# MAGIC 
# MAGIC We'll re-use the gold table built in our previous data pipeline as training dataset.
# MAGIC 
# MAGIC Building such a model is greatly simplified by the use of [segmentation_models.pytorch](https://github.com/qubvel/segmentation_models.pytorch).
# MAGIC 
# MAGIC ## MLOps steps
# MAGIC 
# MAGIC While building an image segmentation model can be easily done, deploying such model in production is much harder.
# MAGIC 
# MAGIC Databricks simplify this process and accelerate DS journey with the help of MLFlow by providing
# MAGIC 
# MAGIC * Auto experimentation tracking to keep track of progress
# MAGIC * Simple, distributed hyperparameter tuning with hyperopt to get the best model
# MAGIC * Model packaging in MLFlow, abstracting our ML framework
# MAGIC * Model registry for governance
# MAGIC * Batch or real time serving (1 click deployment)
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fboat_satellite_imaging%2Fnotebook_ml&dt=MANUFACTURING_BOAT_ML">
# MAGIC <!-- [metadata={"description":"Deep Learning notebook training ML model for boat detection with pytorch segmentation and deploying it to MLFlow.",
# MAGIC  "authors":["tarik.boukherissa@databricks.com"]}] -->

# COMMAND ----------

# DBTITLE 1,Installing pytorch segmentation_model lib
# MAGIC %pip install git+https://github.com/qubvel/segmentation_models.pytorch pytorch-lightning==1.5.10

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Importing our DL functions / image transformation helpers
# MAGIC %run ./_resources/03-DL-helpers

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Split data as train/test dataset
# MAGIC 
# MAGIC Like for any ML model, we start by splitting the images in a training/test dataset

# COMMAND ----------

import mlflow
import torch

from petastorm.spark import SparkDatasetConverter, make_spark_converter
from petastorm import TransformSpec

gold_satellite_image =spark.table('gold_satellite_image')
(images_train, images_test) = gold_satellite_image.randomSplit([0.8, 0.2 ], 42)

#Check GPU availability
if not torch.cuda.is_available(): # is gpu
  raise Exception("Please use a GPU-cluster for model training, CPU instances will be too slow")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Delta table for DL with petastorm
# MAGIC Our data is currently stored as Delta table and available as spark dataframe. However, pytorch is expecting a specific type of data.
# MAGIC 
# MAGIC To solve that, we'll be using petastorm and spark converter to automatically send data to our model from the table. The converter will incrementally load the data using local cache for faster process. Please read the [Documentation](https://docs.databricks.com/applications/machine-learning/load-data/petastorm.html) for more details.

# COMMAND ----------

from petastorm.spark import SparkDatasetConverter, make_spark_converter
#Set the converter cache folder to petastorm_path
spark.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF, petastorm_path)
#convert the image for pytorch
converter_train = make_spark_converter(images_train.coalesce(4))
converter_test = make_spark_converter(images_test.coalesce(4))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Model implementation
# MAGIC 
# MAGIC The following cells implement an ML model leveraging pytorch. 
# MAGIC 
# MAGIC A couple of transformations are required to prepare the image for pytorch, the rest is standard DL code using the Pytorch Segmentation Models library.
# MAGIC 
# MAGIC For more details, the BoatModel is defined in the [03-DL-helpers]($./_resources/03-DL-helpers) notebook.
# MAGIC 
# MAGIC Note that we'll be using MLFlow to autolatically log our experiment metrics and register our final model.

# COMMAND ----------

# DBTITLE 1,Train and log the model in MLFlow
mlflow.autolog(log_models=False)
def train_model(arch, encoder_name, lr, nested=False):
  with mlflow.start_run(nested=nested) as run:
    #See 03-DL-helpers for more details on the model
    model = BoatModel(arch=arch, encoder_name=encoder_name, in_channels=3, out_classes=1, lr=lr)  
    trainer = pl.Trainer(
        benchmark=True,
        num_sanity_val_steps=0,
        precision=16,
        accelerator= "gpu", 
        gpus = "1",
        log_every_n_steps=100,
        default_root_dir="/tmp",
        max_epochs=1)
    
    mlflow.log_param("encoder", encoder_name)
    mlflow.log_param("arch", arch)
    mlflow.set_tag("field_demos", "satellite_segmentation")
    with converter_train.make_torch_dataloader(num_epochs=1, transform_spec=get_transform_spec(is_train=True), batch_size=BATCH_SIZE) as train_dataloader, \
       converter_test.make_torch_dataloader(num_epochs=1, transform_spec=get_transform_spec(is_train=False), batch_size=BATCH_SIZE) as valid_dataloader:  
      trainer.fit(model, train_dataloaders=train_dataloader, val_dataloaders=valid_dataloader)
      delattr(model, "trainer")  
      #add model requirement
      reqs = mlflow.pytorch.get_default_pip_requirements() + ["git+https://github.com/qubvel/segmentation_models.pytorch", "pytorch-lightning=="+pl.__version__]
      #save model to MLFlow. See 03-DL-helpers for details on the model wrapper to make inference easier
      mlflow.pyfunc.log_model(artifact_path="model", python_model=CVModelWrapper(model), pip_requirements=reqs)
      #log and returns model accuracy
      valid_metrics = trainer.validate(model, dataloaders=valid_dataloader, verbose=False)
      valid_per_image_iou = valid_metrics[0]['valid_per_image_iou']
      mlflow.log_metric("valid_per_image_iou", valid_per_image_iou)
      return valid_per_image_iou

#valid_image = train_model("FPN", "resnet34", 0.0001)
#print(valid_image)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameters tuning with Hyperopt
# MAGIC 
# MAGIC Our model is now ready. Tuning such a model can be tricky. We have the choice between lot of architecture, encoder and extra hyperparameter like the learning rate.
# MAGIC 
# MAGIC Let's use Hyperopt to find the best set of hyperparameters for us. Note that Hyperopt can also work in a distributed fashion, training multiple models in parallel on multiple instances to speedup our training.

# COMMAND ----------

from hyperopt import fmin, rand, hp, SparkTrials, STATUS_OK, space_eval
import gc 

# define hyperparameter search space
search_space = {
    'lr': hp.loguniform('lr', -10, -4),
    'segarch': hp.choice('segarch',['Unet', 'FPN', 'deeplabv3plus', 'unetplusplus']),
    'encoder_name': hp.choice('encoder_name',['resnet50', 'resnet101', 'resnet152', 'resnet34'])}
 
# define training function to return results as expected by hyperopt
def train_fn(params):
  arch=params['segarch']
  encoder_name=params['encoder_name']
  lr=params['lr']
  gc.collect()
  torch.cuda.empty_cache()

  valid_per_image_iou = train_model(arch, encoder_name, lr, nested=True)
  return {'loss': 1 - valid_per_image_iou, 'status': STATUS_OK}
 
parallelism = int(sc.getConf().get('spark.databricks.clusterUsageTags.clusterWorkers'))
trials = SparkTrials(parallelism=parallelism) if parallelism > 1 else Trials()

# perform distributed hyperparameter tuning. Real training would go with max_eval > 20 
mlflow.autolog(log_models=False)
with mlflow.start_run() as run:
  argmin = fmin(fn=train_fn, space=search_space, algo=tpe.suggest, max_evals=3, trials=trials)
  params = space_eval(search_space, argmin)
  for p in params:
    mlflow.log_param(p, params[p])
  mlflow.set_tag("field_demos", "satellite_segmentation")
  run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Deploying our model in production
# MAGIC 
# MAGIC Our model is now trained. All we have to do is get the best model (based on the `valid_per_image_iou` metric) and deploy it in MLFlow registry.
# MAGIC 
# MAGIC We can do that using the UI or with a couple of API calls:

# COMMAND ----------

# DBTITLE 1,Save the best model to the registry (as a new version)
#get the best model from the registry
best_model = mlflow.search_runs(filter_string='attributes.status = "FINISHED" and tags.field_demos = "satellite_segmentation"', order_by=["metrics.valid_per_image_iou DESC"], max_results=1).iloc[0]
model_registered = mlflow.register_model("runs:/"+best_model.run_id+"/model", "field_demos_satellite_segmentation")
print(model_registered)

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_satellite_segmentation", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our model is now deployed and flag as production-ready!
# MAGIC 
# MAGIC We have now deploy our model to our Registry. This will give model governance, simplifying and accelerating all downstream pipeline developements.
# MAGIC 
# MAGIC The model is now ready to be used in any data pipeline (DLT, batch or real time with Databricks Model Serving). 
# MAGIC 
# MAGIC Let's see how we can use it to [run inferences]($./03-Prediction-Boat-detection) at scale.
