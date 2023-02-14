# Databricks notebook source
# MAGIC %md 
# MAGIC # Advanced Analytics leveraging the Feature Store
# MAGIC 
# MAGIC ## Centralizing our Cohort features
# MAGIC 
# MAGIC In this example, we'll showcase how the features created from our covid admission Analysis can be centralized for cross-team access and more traceability.
# MAGIC 
# MAGIC This notebook is the continuity of the first demo where we ingested the FHIR bundle and prep'd the . 
# MAGIC 
# MAGIC **Important:** Make sure you run the [01-lakehouse-hls-interoperability-dbignite]($./01-lakehouse-hls-interoperability-dbignite) notebook to ingest the FHIRE bundle data required for the COVID admission analysis.
# MAGIC 
# MAGIC ## Databricks Feature Store
# MAGIC 
# MAGIC We'll leverage Databricks Feature Store to do that.
# MAGIC 
# MAGIC The idea is to save all our patient characteristic (SDOH, deases history) and save them as feature in a table, identified with our Patient ID.
# MAGIC 
# MAGIC Once the features are created and saved, any other analyst will be able to access them for any cohort containing a list of Patient id and build analysis on top of them.
# MAGIC 
# MAGIC In addition, using the feature store will let you track who is using which features, and which ML models are using these feature. We could also go further and deploy real-time inference capabilities leveraving realtime feature store (ex: deploying a model to predict and explain covid admission probability in real-time)

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=false

# COMMAND ----------

# DBTITLE 1,Getting the patient covid history from our previous analysis
#Let's get back the dataset we built in the previous notebook (used to compute the Mutual information between different features)
patient_covid_hist_pdf = spark.table("patient_covid_hist")
display(patient_covid_hist_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Saving the patient history as Features to Feature Store
# MAGIC Our features have been computed from the FHIR bundles (see the previous notebook for more details). 
# MAGIC 
# MAGIC We can easily add the extracted features, such as disease history, socio-economic status and demographic information to databricks feature store, which we'll be using later for building our ML model.
# MAGIC 
# MAGIC Let's define 
# MAGIC *Note: For a better deepdive into databricks feature store see [feature store docs](https://docs.databricks.com/applications/machine-learning/feature-store/index.html#databricks-feature-store).*

# COMMAND ----------

# DBTITLE 1,Define the features we want to add to our feature store
sdoh_cols=['person_id',
 'history_of_full-time-employment',
 'history_of_part-time-employment',
 'history_of_not-in-labor-force',
 'history_of_received-higher-education',
 'history_of_has-a-criminal-record',
 'history_of_unemployed',
 'history_of_refugee',
 'history_of_misuses-drugs']

disease_history_cols=['person_id',
 'history_of_obesity',
 'history_of_prediabetes',
 'history_of_hypertension',
 'history_of_diabetes',
 'history_of_coronary-heart-disease'
]
#recreate the features from the first notebook computation
patient_covid_data_df = spark.table("patient_covid_hist").select("person_id", "is_admitted")
demographics_df = spark.sql('select person_id, gender_source_value as gender, year(current_date) - year_of_birth as age from person')
sdoh_df = spark.table("patient_covid_hist").select(sdoh_cols)
disease_history_df = spark.table("patient_covid_hist").select(disease_history_cols)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating the feature store tables
# MAGIC 
# MAGIC First, we create an instance of the Feature Store client.

# COMMAND ----------

from databricks import feature_store
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC We'll then create the feature store table.
# MAGIC 
# MAGIC Use either the `create_table` API (Databricks Runtime 10.2 ML or above) to define schema and unique ID keys.
# MAGIC 
# MAGIC If the optional argument `df` (Databricks Runtime 10.2 ML or above) is passed, the API also writes the data to Feature Store.
# MAGIC 
# MAGIC If you want to drop the feature tables from the feature store, use the following:
# MAGIC ```
# MAGIC fs.drop_table(f"{dbName}.features_demographics")
# MAGIC fs.drop_table(f"{dbName}.features_sdoh")
# MAGIC fs.drop_table(f"{dbName}.features_disease_history")
# MAGIC ```
# MAGIC Note: this API only works with ML Runtime `10.5` alternatively you can use the UI. See the [docs](https://docs.databricks.com/applications/machine-learning/feature-store/ui.html#delete-a-feature-table) for more information.

# COMMAND ----------

# This cell uses an API introduced with Databricks Runtime 10.2 ML.
try:
  fs.drop_table(f"{dbName}.features_demographics")
  fs.drop_table(f"{dbName}.features_sdoh")
  fs.drop_table(f"{dbName}.features_disease_history")
except:
  print("feature store not yet created.")
  
fs.create_table(
    name=f"{dbName}.features_demographics",
    primary_keys=["person_id"],
    df=demographics_df,
    description="Patient's demographics features",
)

fs.create_table(
    name=f"{dbName}.features_sdoh",
    primary_keys=["person_id"],
    df=sdoh_df,
    description="Social Determinants of Health (SDOH) features",
)

fs.create_table(
    name=f"{dbName}.features_disease_history",
    primary_keys=["person_id"],
    df=disease_history_df,
    description="Disease history features",
)

# COMMAND ----------

# MAGIC %md
# MAGIC That's all we have to do. Our features are now saved and ready to be used by our analyst team.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Predicting the risk of being admitted with COVID from Feature Store tables
# MAGIC 
# MAGIC As a next step, we'll train binary classifier to predict the outcome (`is_admitted`) based on the features provided in the training data.
# MAGIC 
# MAGIC This model will predict the likelyhood of behing admitted and will help us understanding which feature has the most impact for the model globally but also each patient.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating the feature lookups
# MAGIC 
# MAGIC Our first step is to define the features we want to use. We'll use what we call `FeatureLookup` to retrieve the features we want based on the key we defined: `person_id`. 
# MAGIC 
# MAGIC We can also specify the features we need. If nothing is specified it'll select the entire dataset.

# COMMAND ----------

from databricks.feature_store import FeatureLookup
 
demographics_feature_lookups = FeatureLookup(table_name = f"{dbName}.features_demographics",
                                             feature_names = ["gender","age"],
                                             lookup_key = ["person_id"])
 
sdoh_feature_lookups = FeatureLookup(table_name = f"{dbName}.features_sdoh",
                                     lookup_key = ["person_id"])

disease_history_feature_lookups = FeatureLookup(table_name = f"{dbName}.features_disease_history",
                                                lookup_key = ["person_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Training Dataset from Feature lookup
# MAGIC 
# MAGIC The next step is to create a training dataset for our model input from our feature store
# MAGIC 
# MAGIC The Feature store will automatically track all dependencies and we'll know that these features are used in this model. This brings:
# MAGIC 
# MAGIC * Governance: know which feature is used by who, allowing you to track sensitive data usage among other
# MAGIC * Lineage: if we are to change the features, we'll be able track potential impact downstream.
# MAGIC * Centralization / Composition: define your features one and re-use them among all your use-cases, accelerating your project delivery.
# MAGIC 
# MAGIC 
# MAGIC When `fs.create_training_set(..)` is invoked below, the following steps will happen:
# MAGIC 
# MAGIC 1. WE select specific features from Feature Store to use in training your model. Each feature is specified by the FeatureLookup's created above.
# MAGIC 2. Features are joined with the raw input data according to each FeatureLookup's lookup_key (here the person id).
# MAGIC 3. The TrainingSet is then transformed into a DataFrame to train on. 
# MAGIC 
# MAGIC See [Create a Training Dataset](https://docs.databricks.com/applications/machine-learning/feature-store/feature-tables.html#create-a-training-dataset) for more information.

# COMMAND ----------

# Start an mlflow run, which is needed for the feature store to log the model
with mlflow.start_run() as run: 
  # Create the training set that includes the raw input data merged with corresponding features from both feature tables
  training_set = fs.create_training_set(
    patient_covid_data_df,
    feature_lookups = [demographics_feature_lookups, sdoh_feature_lookups, disease_history_feature_lookups],
    label = "is_admitted"
  )
  # Load the TrainingSet into a dataframe which can be passed into sklearn for training a model
  training_df = training_set.load_df()
  display(training_df)
  # Build a small model for this example
  X_train, X_test, y_train, y_test = prep_data_for_classifier(training_df.toPandas())
  display("Training model...")
  model = XGBClassifier()
  model.fit(X_train, y_train)
  #Log the model using the FS client to our registry to link it to our features and create the full lineage.
  fs.log_model(model, "model", training_set=training_set, registered_model_name="field_demos_covid_prediction", flavor=mlflow.sklearn,)
  print(f"Model trained. Accuracy score: {model.score(X_test, y_test)}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Tracking feature store usage
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbignite-featurestore.png" style="float: right" width="700px" />
# MAGIC 
# MAGIC We have now saved our model using the Feature store api.
# MAGIC 
# MAGIC Databricks is keeping track of all the model and let you analyze which team is using which feature.
# MAGIC 
# MAGIC You can open the feature store menu and search for your feature tables.
# MAGIC 
# MAGIC On the bottom, you'll find the usage including the new model we just created and its version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Analysis
# MAGIC 
# MAGIC The final steps of our analysis doesn't change. We can load the model from the registry annd predict the probability of a patient being admitted and analyse which features are important to determine that.
# MAGIC 
# MAGIC We'll explain the feature importance in our model and visualize the feature impact on the predictions.

# COMMAND ----------

import shap
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_train)
shap.summary_plot(shap_values = shap_values, features = X_train)

# COMMAND ----------

# MAGIC %md
# MAGIC As expected, the **Hypertension** and **Age** of our patient have the strongest impact in our model.
# MAGIC 
# MAGIC Let's predict the probability of admission of a patient and understand which features drive this outcome:

# COMMAND ----------

print(f"For our second person, our model is predicting an admission at {model.predict_proba(X_train)[1][1]:.2f}%")

print(f"Let's explain how the model is coming to this conclusion and which0 feature are the most important:")
shap.force_plot(explainer.expected_value, shap_values[1], matplotlib=True, feature_names=X_train.columns)

print(f"Person #2 characteristics:")
display(X_train.iloc[1])

# COMMAND ----------

# MAGIC %md 
# MAGIC # Conclusion
# MAGIC 
# MAGIC In this demo, We've seen how to leverage the Lakehouse to accelerate final uses cases & data analysis while ensuring data security and governance:
# MAGIC 
# MAGIC * Make data ingestion easy for all data analyst 
# MAGIC * Leverage standard tools to collaboratively build & share reports
# MAGIC * Unlock the most advanced analytics use-cases for your team
# MAGIC 
# MAGIC Databricks Lakehouse removes all operational burden and let your team focus on adding value on top your data!

# COMMAND ----------

# MAGIC %md
# MAGIC ## License ⚖️
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2022].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Disclaimers
# MAGIC *Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This demo (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account.  Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.*
