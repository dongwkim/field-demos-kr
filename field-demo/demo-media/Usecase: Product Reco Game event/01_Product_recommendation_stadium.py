# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Product Recommendation for Live Events - Stadium Analytics
# MAGIC <img style='float: right' width='600px' src='https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-notif.png'>
# MAGIC 
# MAGIC **Use Case Overview**
# MAGIC * Live sporting events generate large volumes of customer data that can be used to create a better fan experience. Fan engagement is directly correlated to increased sales and customer retention. This demo shows how to create a personalized discount for a fan based on their purchasing history and location of where they sit in a stadium to drive additional sales during the game and create a more individualized experience.
# MAGIC * Final Goal: Send Push Notification to Fans with Promotional Offer
# MAGIC 
# MAGIC **Business Impact of Solution**
# MAGIC * **Fan Engagment and Customer Retention:** Fans that have a better experience while attending a game are more likely to return for another one in the future.
# MAGIC * **Increased Revenue:** Sending personalized discounts to fans will incentivize them to purchase more from vendors during a game which will increase revenue.
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fproduct_recommender_stadium%2Fnotebook&dt=MEDIA_USE_CASE">
# MAGIC <!-- [metadata={"description":"Product recommendation for live events. This demo shows how to create a personalized discount for a fan based on their purchasing history and location of where they sit in a stadium to drive additional sales during the game and create a more individualized experience.",
# MAGIC  "authors":["dan.morris@databricks.com"],
# MAGIC   "db_resources":{},
# MAGIC   "search_tags":{"vertical": "media", "step": "Data Engineering", "components": ["sparkml", "mlflow", "als", "recommender"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 1: Ingest Data into Delta Lake and Apply Transformations
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-1.png" width="1000px">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### List of Vendors and Items at Ford Field
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-vendors.png" width="400px" style="float: right" />
# MAGIC 
# MAGIC 
# MAGIC Our list of vendors and item At [https://www.fordfield.com/restaurant-partners](Ford Field) are uploaded as CSV files.
# MAGIC 
# MAGIC We can easily ingest these data, infer the schema and handle Schema Evolution using Databricks Autoloader. This is done using the `cloudFiles` format.
# MAGIC 
# MAGIC Because we'll re-use the autoloader to ingest multiple stream, let's define a function to wrap it:

# COMMAND ----------

def ingest_bronze(raw_files_path, raw_files_format, bronze_table_name):
  spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", raw_files_format) \
            .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schemas_reco/{bronze_table_name}") \
            .option("cloudFiles.inferColumnTypes", "true") \
            .load(raw_files_path)\
        .writeStream \
            .option("checkpointLocation", f"{cloud_storage_path}/chekpoints_reco/{bronze_table_name}") \
            .trigger(once=True).table(bronze_table_name).awaitTermination()

ingest_bronze("/demo/stadium_recommender/vendors/", "csv", "stadium_vendors")

display(spark.read.table("stadium_vendors"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC 
# MAGIC ### Tickets sales
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-pricing.png" width="400px" style="float: right" />
# MAGIC 
# MAGIC Tickets sales are also saved as bronze table

# COMMAND ----------

ingest_bronze("/demo/stadium_recommender/ticket_sales/", "json", "ticket_sales")
display(spark.read.table("ticket_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Game schedule
# MAGIC 
# MAGIC Game & events can be ingested using the same technique:

# COMMAND ----------

ingest_bronze("/demo/stadium_recommender/games/", "csv", "games")
display(spark.read.table("games"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Points of Sale
# MAGIC 
# MAGIC And finally point of sales:

# COMMAND ----------

ingest_bronze("/demo/stadium_recommender/point_of_sale/", "json", "point_of_sale")
display(spark.read.table("point_of_sale"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Materialize the silver table
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-2.png" width="1000px">

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_sales AS (
# MAGIC   SELECT * EXCEPT (t._rescued_data, p._rescued_data, s._rescued_data)
# MAGIC     FROM ticket_sales t 
# MAGIC       JOIN point_of_sale p ON t.customer_id = p.customer 
# MAGIC       JOIN stadium_vendors s ON p.item_purchased = s.item_id AND t.game_id = p.game);
# MAGIC 
# MAGIC SELECT * FROM silver_sales ;

# COMMAND ----------

# DBTITLE 1,Top 10 most purchased items
# MAGIC %sql SELECT item_id, count(item_id) AS item_purchases FROM silver_sales GROUP BY item_id order by item_purchases desc limit 10

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 3: Build Recommendation Model
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-3.png" width="1000px">
# MAGIC 
# MAGIC <img src=https://databricks.com/wp-content/uploads/2020/04/databricks-adds-access-control-to-mlflow-model-registry_01.jpg width='500px' style='float: right'>
# MAGIC 
# MAGIC ### Building an ALS recommender with MLFlow
# MAGIC 
# MAGIC 
# MAGIC Mlflow is used to track all the experiments metrics, including the model itself.
# MAGIC 
# MAGIC We'll use MLFlow to deploy the model in the registry and flag it as production ready. Once it's done, we'll be able to reuse this model to get the final recommendations and send our push notifications with personal offers.

# COMMAND ----------

with mlflow.start_run() as run:
  #MLFlow will automatically log all our parameters
  mlflow.pyspark.ml.autolog()
  df = spark.sql("select customer_id, item_id, count(item_id) as item_purchases from silver_sales group by customer_id, item_id")
  # Build the recommendation model using ALS on the training data
  # Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
  # rating matrix is derived from another source of information (i.e. it is inferred from other signals), setting implicitPrefs to true to get better results:
  als = ALS(rank=3, userCol="customer_id", itemCol="item_id", ratingCol="item_purchases", implicitPrefs=True, seed=0, coldStartStrategy="nan")
  
  num_cores = sc.defaultParallelism
  als.setNumBlocks(num_cores)
  
  model = als.fit(df)
  
  mlflow.spark.log_model(model, "spark-model", registered_model_name='Stadium_Recommendation')
   #Let's get back the run ID as we'll need to add other figures in our run from another cell
  run_id = run.info.run_id

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
model_registered = mlflow.register_model("runs:/"+run_id+"/spark-model", "field_demos_stadium_recommendation")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_stadium_recommendation", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md ### Deploy & get Recommendations from the Model in production
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-4.png" width="1000px">
# MAGIC 
# MAGIC Now that our model is in our MLFlow registry, we can start to use it in a production pipeline.
# MAGIC 
# MAGIC All we have to do is load it from MLFlow and apply the inference. 

# COMMAND ----------

#                                                                             Stage/version
#                                                     Model name                   |
#                                                         |                        |
model = mlflow.spark.load_model(f'models:/field_demos_stadium_recommendation/Production')

# COMMAND ----------

# get top 10 recommendations for each customer
recommendations = model.stages[0].recommendForAllUsers(10)
recommendations.createOrReplaceTempView("customer_recommendations")

display(recommendations)

# COMMAND ----------

# MAGIC %md ## Step 4: Filter and Re-Rank Recommendations
# MAGIC 
# MAGIC 
# MAGIC Let's explode the recommendations for each users and get back the items and customers details
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-5.png" width="1000px">

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gold_recommendations as (
# MAGIC SELECT ci.*, v.* EXCEPT (v.item_id)
# MAGIC     FROM stadium_vendors v
# MAGIC        JOIN (SELECT customer_id, items.*
# MAGIC              FROM   (SELECT customer_id, explode(recommendations) AS items
# MAGIC                      FROM   customer_recommendations) a) ci
# MAGIC          ON v.item_id = ci.item_id );
# MAGIC select * from gold_recommendations;

# COMMAND ----------

# MAGIC %md ### Finding the best item for each customer
# MAGIC 
# MAGIC We want to find a personalized item, but we also want to make sure that the item can be bought from a close distance. To do that, we'll compute the distance between the customer seat and the shop location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sections_recommendations as (
# MAGIC   SELECT r.customer_id, item_id, rating, section, section_number, abs(section-section_number) as distance
# MAGIC       FROM gold_recommendations r
# MAGIC          JOIN ticket_sales s on s.customer_id = r.customer_id);
# MAGIC        
# MAGIC select * from sections_recommendations;

# COMMAND ----------

# DBTITLE 1,Let's take the closest item for each customer
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS final_recommendations AS (
# MAGIC SELECT * FROM (
# MAGIC   SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY distance ASC) AS rnk FROM sections_recommendations
# MAGIC ) WHERE rnk = 1);
# MAGIC 
# MAGIC SELECT * FROM final_recommendations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Send Promotional Offer to Fans
# MAGIC 
# MAGIC We now have all the items we need to send to all our users! Let's test the notifications.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-6.png" width="1000px">

# COMMAND ----------

recommendation = spark.sql("select * from final_recommendations where customer_id = 1").collect()[0]

title = "Special deal for you!"
subject= f"Hi {recommendation['customer_id']}! We have a special discount for item {recommendation['item_id']}! \nCheck it out, it's available at store {recommendation['section']}!" 

user_cell_phone = "<Put your real cell phone here to test the notification!>"
 
send_push_notification(title, subject, user_cell_phone)

# COMMAND ----------

# MAGIC %md ## Step 5: Tracking game campaign and metrics
# MAGIC 
# MAGIC During the game, we can capture all the metrics live and build dashboard to monitor the event KPIs.
# MAGIC 
# MAGIC After the game finishes, we can also review the success rate of our promotional offer and make required adjustement for the next game!
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/product_reco_stadium-7.png" width="1000px">

# COMMAND ----------

# MAGIC %sql
# MAGIC -- visualisation: Bar, Series groupings: item_purchased, Values: count, Aggregation: SUM
# MAGIC 
# MAGIC select count(*) as count, item_purchased from (
# MAGIC   SELECT *, CASE WHEN recommended_item_purchased = 1 THEN 'Yes' ELSE 'No' END as item_purchased
# MAGIC     FROM final_recommendations r
# MAGIC     LEFT JOIN ticket_sales s USING(customer_id)
# MAGIC     LEFT JOIN purchase_history p USING(game_id, customer_id)) group by item_purchased
