# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=media

# COMMAND ----------

path = '/demo/stadium_recommender'
reset_all = dbutils.widgets.get("reset_all_data") == "true"
def generate_data():
  dbutils.notebook.run("./01-data-generator", 600, {"reset_all_data": reset_all, "dbName": dbName, "path": path})

if reset_all:
  generate_data()
else:
  try:
    dbutils.fs.ls(path)
  except: 
    generate_data()

# COMMAND ----------

#TODO a retirer
# Import statements
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.sql.functions import *
import random

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

def send_push_notification(title, subject, phone_number):
  #Tune the message with the user running the notebook. In real workd example we'd have a table with the customer details. 
  current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
  first_name = current_user[:current_user.find('.')].capitalize()
  import re
  subject = re.sub("\n", "<br/>", subject) 
  subject = re.sub("Hi ([0-9]*)", "Hi "+first_name, subject) 
  subject = re.sub("item ([0-9]*)", "your favorite pizza", subject) 
  subject = re.sub("store ([0-9]*)", "Goal Post Grill & Pizzeria", subject) 

  displayHTML(f"""<div style="border-radius: 10px; background-color: #adeaff; padding: 10px; width: 400px; box-shadow: 2px 2px 2px #F7f7f7; margin-bottom: 3px">
        <div style="padding-bottom: 5px"><img style="width:20px; margin-bottom: -3px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/media/resources/images/bell.png"/> <strong>{title}</strong></div>
        {subject}
        </div>""")

# COMMAND ----------

#TODO: use SNS

# # Import the boto3 client
# import boto3

# # Set the AWS region name, retrieve the access key & secret key from dbutils secrets.
# # For information about how to store the credentials in a secret, see
# # https://docs.databricks.com/user-guide/secrets/secrets.html
# AWS_REGION = "us-east-2"
# ACCESS_KEY = dbutils.secrets.get('<scope-name>','<access-key>')
# SECRET_KEY = dbutils.secrets.get('<scope-name>','<secret-key>')
# sender='<ford_field@nfl.com>'

# #Create the boto3 client with the region name, access key and secret keys.
# client = boto3.client('sns',region_name=AWS_REGION,
# aws_access_key_id=ACCESS_KEY,
# aws_secret_access_key=SECRET_KEY)

# # Add phone subscribers
# for number in list_of_phone_numbers:
#     client.subscribe(
#         articleArn=article_arn,
#         Protocol='sms',
#         Endpoint=number  # <-- phone numbers who'll receive SMS.
#     )

# # Send message to the SNS article using publish method.
# response = client.publish(
# articleArn='Discounted Item!',
# Message="Your Discounted Item..",
# Subject='Get your discounted Item from Ford Field..',
# MessageStructure='string'
# )
