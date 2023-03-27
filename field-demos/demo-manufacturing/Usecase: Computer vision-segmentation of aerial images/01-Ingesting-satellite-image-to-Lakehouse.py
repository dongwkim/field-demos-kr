# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# DBTITLE 1,Setup data. Make sure you add your Kaggle credential under ./resource/_kaggle_credential
# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Computer vision - segmentation of aerial images
# MAGIC 
# MAGIC <div style="float:right">
# MAGIC <img width="500px" src="https://storage.googleapis.com/kaggle-media/competitions/Airbus/ships.jpg">
# MAGIC </div>
# MAGIC 
# MAGIC In this demo, we'll show you how Databricks can help you deploying an end to end image segmentation. 
# MAGIC 
# MAGIC We'll be using the Kaggle [Airbus ship detection](https://www.kaggle.com/c/airbus-ship-detection), and build an end to end pipeline to detect ships in our satellite images. This will help for environmental spill detection, maritime control and even commodities trading by being able to detect if shipments will arrive late. 
# MAGIC 
# MAGIC ## Why image segmentation
# MAGIC 
# MAGIC Image segmentation is a common challenge in the context of manufacturing. As example, these models can be very useful for:
# MAGIC - quality teams to detect area where damage occurs on electronics boards, mechanical part wear, wind turbine rotor damage, manufacturing defect...
# MAGIC - for application team to develop new services like environmental spill detection, forest fire evaluation, marine traffic monitoring...
# MAGIC - Any other application. As example, Amazon used image segementation to help with visual recommendation in their fulfillment centers
# MAGIC 
# MAGIC ## Implementing a production-grade pipeline
# MAGIC 
# MAGIC At a pure ML level, the image segmentation problem has been facilitated in the recent years with pre-trained models (transfer learning) and higher level ML frameworks. 
# MAGIC 
# MAGIC While a talented DS team can quickly deploy such model, a real challenge remains in the implementation of a production grade, end to end pipeline, consuming images and covering all the MLOps/governances, and ultimately exposing result to business lines (BI/Dashboarding). 
# MAGIC 
# MAGIC This is the critical part of all ML project and one of the most difficult.
# MAGIC 
# MAGIC Databricks Lakehouse is designed to make this overall process simple, letting Data Scientist focus on the core use-case.
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fboat_satellite_imaging%2Fnotebook_ingestion&dt=MANUFACTURING_BOAT_INGESTION">
# MAGIC <!-- [metadata={"description":"Ingestion notebook to ingest and prepare satellite images for boat detection. Unstructured data, autoloader.",
# MAGIC  "authors":["tarik.boukherissa@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Image segmentation pipeline
# MAGIC 
# MAGIC This is the pipeline we'll be building. We're ingesting 2 dataset:
# MAGIC 
# MAGIC * The raw satellite images (jpg) containing boat
# MAGIC * The masks, saved as CSV files, containing the pixels where a boat has been labeled
# MAGIC 
# MAGIC We'll first focus on building a data pipeline to incrementally load this data and create a final Gold table.
# MAGIC 
# MAGIC This table will then be used to train a ML Segmentation model to learn to detect boat in our images in real time!
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-0.png" width="1000"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Ingesting raw images with Databricks Autoloader
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-1.png" width="700" style="float:right; margin-left: 10px"/>
# MAGIC 
# MAGIC The first step is to load the individual JPG images. This can be quite challenging at scale, especially for incremental load (consume only the new one).
# MAGIC 
# MAGIC Databricks can easily handle binary files and solve these challenges using the Autoloader.
# MAGIC 
# MAGIC Autoloader will garantee that only new files are being processed while scaling with millions of individual images. 
# MAGIC 
# MAGIC It'll also automatically detect and handle images, taking care of the schema and removing unecessary compression on jpg being saved as Delta Tables.

# COMMAND ----------

print(f"Our raw images are saved as jpg files under {raw_data_location}/test_v2/")
display(dbutils.fs.ls(f"{raw_data_location}/test_v2/"))

# COMMAND ----------

bronze_images = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "binaryfile")
                      .option("cloudFiles.schemaLocation", cloud_storage_path+"/schema_bronze_images")
                      .option("cloudFiles.maxFilesPerTrigger", 10000)
                      .option("pathGlobFilter", "*.jpg")
                      .load(f"{raw_data_location}/train_v2/"))

(bronze_images.writeStream
              .trigger(availableNow=True)
              .option("checkpointLocation", cloud_storage_path+"/checkpoint_bronze_images")
              .table("bronze_satellite_images").awaitTermination())

display(spark.table("bronze_satellite_images"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cleaning images as Silver table
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-2.png" width="700" style="float:right; margin-left: 10px"/>
# MAGIC 
# MAGIC Once the images are ingested, we'll make some transformation and save the result them as a Silver table:
# MAGIC 
# MAGIC * get the ID from file name
# MAGIC * only keep valid images (using expectations)
# MAGIC * resizing data to a suitable size.

# COMMAND ----------

silver_satellite_images = (spark.readStream.table("bronze_satellite_images")
                                .withColumn("image_id",substring_index(col('path'), '/', -1))       
                                .withColumn("content", resize_image_udf(col("content")).alias("content", metadata=image_meta))
                                .filter("content is not null")
                                .select("image_id", "content"))

(silver_satellite_images.writeStream
                        .trigger(availableNow=True)
                        .option("checkpointLocation", cloud_storage_path+"/checkpoint_silver_images")
                        .table("silver_satellite_images").awaitTermination())

# COMMAND ----------

# MAGIC %sql select * from silver_satellite_images

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Loading the raw image mask
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-4.png" width="700" style="float:right; margin-left: 10px"/>
# MAGIC 
# MAGIC For each image, we have a companion dataset containing all the pixels where a boat has been detected. This data is being saved as CSV file.
# MAGIC 
# MAGIC We'll first start by ingesting this csv data and saving them as a raw Delta table.

# COMMAND ----------

annotationsDF = (spark.read.option("header","true")
                           .option("inferSchema","true")
                           .csv(f"{raw_data_location}/train_ship_segmentations_v2.csv"))
      
(annotationsDF.withColumnRenamed("ImageId", "image_id")
              .withColumnRenamed("EncodedPixels", "encoded_pixels")
              .write.mode('overwrite').saveAsTable("bronze_satellite_mask"))
display(spark.table("bronze_satellite_mask"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Transforming masks as images
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-5.png" width="700" style="float:right; margin-left: 10px"/>
# MAGIC 
# MAGIC We now have all the information on the image labels saved as an array containing the pixels coordinate (where the boat have been detected).
# MAGIC 
# MAGIC However, ML models work with images. Therefore, we need to create a Mask (image) based on this information.
# MAGIC 
# MAGIC The mask will be saved as a jpg, black (no boat) and white (boat detected). We'll do that in the 

# COMMAND ----------

silver_satellite_mask = (spark.table("bronze_satellite_mask")
                               .filter("encoded_pixels is not null")
                               .groupBy("image_id").agg(collect_list('encoded_pixels').alias('encoded_pixels'))
                               .withColumn("boat_number", size(col("encoded_pixels")))
                               .withColumn("mask", computeMaskUDF(col("encoded_pixels")).alias("mask", metadata=image_meta)))
silver_satellite_mask.write.mode('overwrite').saveAsTable("silver_satellite_mask")
display(spark.table("silver_satellite_mask"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Joining image and mask both as gold layer
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/satellite-imaging/manufacturing-boat-3.png" width="700" style="float:right; margin-left: 10px"/>
# MAGIC 
# MAGIC Ultimately, we can build our gold dataset by merging the mask and the initial image together in 1 single table.
# MAGIC 
# MAGIC This is a simple SQL join operation, based on the image id

# COMMAND ----------

#spark.sql("DROP TABLE IF EXISTS gold_satellite_image_mask")
imagesWithMask = spark.table("silver_satellite_mask").join(spark.table("silver_satellite_images"), "image_id")
imagesWithMask.select("image_id","boat_number","mask","content").write.saveAsTable("gold_satellite_image")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_satellite_image order by boat_number desc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from gold_satellite_image

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our dataset is ready for our Data Scientist team
# MAGIC 
# MAGIC That's it! We have now deployed a production-ready pipeline.
# MAGIC 
# MAGIC Our images are incrementally ingested, joined with our label dataset and properly resized.
# MAGIC 
# MAGIC Let's see how this data can be used by a Data Scientist to [build the model]($./02-Segmentation-model-Pytorch) required for boat detection.
