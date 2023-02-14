# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=hls_interop

# COMMAND ----------

from pyspark.sql.functions import lit, array_contains, year, first, collect_list
import uuid
def create_cohort(cohort_definition_name, cohort_definition_description, cohort):
  uid=abs(uuid.uuid4().fields[0])
  print(f"saving cohort {cohort_definition_name} under the cohort table (id {uid}). Adding entry to cohort_definition.")
  #Add the cohort to the cohort table
  spark.sql(cohort).withColumn("cohort_definition_id", lit(uid).cast('int')).write.mode("append").saveAsTable("cohort")
  #Add the cohort definition to the metadata table
  sql(f"""
      INSERT INTO cohort_definition
        select int({uid}) as cohort_definition_id,
        '{cohort_definition_name}' as cohort_definition_name,
        '{cohort_definition_description}' as cohort_definition_description,
        '{cohort}' as cohort_definition_syntax,
        current_date() as cohort_initiation_date""")

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics.cluster import normalized_mutual_info_score

def get_normalized_mutual_info_score(df):
  cols=patient_covid_hist_pdf.columns
  
  ll=[]
  for col1 in cols:
    for col2 in cols:
      ll+=[normalized_mutual_info_score(patient_covid_hist_pdf[col1], patient_covid_hist_pdf[col2])]
  cols = [m.replace('history_of_','') for m in cols]
  mutI_pdf = pd.DataFrame(np.array(ll).reshape(len(cols),len(cols)),index=cols,columns=cols)
  return mutI_pdf**(1/3)

# COMMAND ----------

from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split

#Cleanup and Split data before training the model: 
# - remove 'history_of' for more visibility in the column name
# - change gender as boolean column for simplicity
# - split in training/test dataset
def prep_data_for_classifier(training):
  X = training.drop(labels=['person_id', 'is_admitted'], axis=1)
  to_rename = {'gender': 'is_male'}
  for c in X.columns:
    #Drop the "history_of_"
    if "history_of" in c:
      to_rename[c] = c[len("history_of_"):]
  X['gender'] = X['gender'] == 'male'
  X = X.rename(columns=to_rename)
  Y = training['is_admitted']
  return train_test_split(X, Y)

# COMMAND ----------

import xgboost
xgboost.set_config(verbosity=0)
