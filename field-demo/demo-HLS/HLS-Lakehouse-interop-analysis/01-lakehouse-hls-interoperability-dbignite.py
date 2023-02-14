# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Accelerating Cohort Analysis & RWE with Databricks Lakehouse 
# MAGIC 
# MAGIC ## Unify teams in a single data platform and accelerate your projects
# MAGIC 
# MAGIC Databricks Lakehouse simplify collaboration & accelerate your team, delivering faster analysis.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-lakehouse.png" width="1000px" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## From FHIR Bundles ingestion to Cohort analysis leveraging SQL, Python or R
# MAGIC 
# MAGIC In this demo, we'll demonstrate how the Lakehouse can cover the entire data pipeline for RWE analysis.
# MAGIC 
# MAGIC We received FHIR bundle files and are required to load them, build cohort and share analysis on top of this data. 
# MAGIC 
# MAGIC We can decompose this process in 3 main steps:
# MAGIC 
# MAGIC * **Data ingestion** (on the left)
# MAGIC   * Simplify ingestion, from all kind of sources. As example, we'll use Databricks `dbignite` library to ingest FHIR bundle as tables ready to be requested in SQL in one line.
# MAGIC   * Query and explore the data ingested
# MAGIC   * Secure data access
# MAGIC * **Analysis** (flow on the top)
# MAGIC   * Create cohorts
# MAGIC   * Create a patient level dashboard from the bundles
# MAGIC   * Investigate rate of hospital admissions among covid patients and explore the effect of SDOH and disease history in hospital admission
# MAGIC * **Data Science / Advance Analystics** (bottom)
# MAGIC   * Create a of patient features 
# MAGIC   * Create a training dataset to build a model predicting and analysing our cohort 
# MAGIC   
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-0.png" width="1000px" />
# MAGIC 
# MAGIC 
# MAGIC ### Data
# MAGIC The data used in this demo is generated using [synthea](https://synthetichealth.github.io/synthea/). We used [covid infections module](https://github.com/synthetichealth/synthea/blob/master/src/main/resources/modules/covid19/infection.json), which incorporates patient risk factors such as diabetes, hypertension and SDOH in determining outcomes. The data is available at `s3://hls-eng-data-public/data/synthea/fhir/fhir/`. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Data ingestion & interoperability: ingesting and processing FHIR bundles with dbignite
# MAGIC 
# MAGIC FHIR is a standard for health care data exchange, allowing entities to share data with interoperability.
# MAGIC 
# MAGIC Analysing and loading FHIR bundle can be hard, especially at scale.
# MAGIC 
# MAGIC Databricks developed `dbignite` to simplify FHIR ingestion. Using the library, you can accelerate your time to insight by:
# MAGIC 
# MAGIC * Parsing and reading the FHIR bundle out of the box
# MAGIC * Creating table for SQL usage (BI/reporting) on top of the incoming data
# MAGIC 
# MAGIC `dbignite` is available as a python wheel and can be installed as following:

# COMMAND ----------

# DBTITLE 1,Download dbignite to our local cloud storage
# MAGIC %fs cp s3://hls-eng-data-public/packages/dbinterop-1.0-py2.py3-none-any.whl /demos/hls/dbinterop-1.0-py2.py3-none-any.whl

# COMMAND ----------

# DBTITLE 1,Install dbignite using pip
# MAGIC %pip install /dbfs/demos/hls/dbinterop-1.0-py2.py3-none-any.whl

# COMMAND ----------

# DBTITLE 1,Demo resources setup
# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyzing a raw FHIR bundle
# MAGIC Before we start, let's take a look at the files we want to ingest. They're generated data from synthea and available in our blob storage:

# COMMAND ----------

# DBTITLE 1,List FHIR bundles
BUNDLE_PATH="s3://hls-eng-data-public/data/synthea/fhir/fhir/"
files=dbutils.fs.ls(BUNDLE_PATH)
print(f'there are {len(files)} bundles to process')
display(files)

# COMMAND ----------

# DBTITLE 1,Take a look at one of the files
print(dbutils.fs.head(files[0].path))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1.1/ Load FHIR bundles into ready to query Delta Tables
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-1.png" width="700px" style="float: right" />
# MAGIC 
# MAGIC We can leverage dbignite library to load this data using the `PersonDashboard` API. This will:
# MAGIC 
# MAGIC 1. Extract resources from FHIR bundles and create a dataframe where rows correspond to each patient bundle and columns contain extracted resources
# MAGIC 2. In addition, add corresponding tables - which have similar schemas to OMOP tabels - to our local database - (`print(dbName)`)

# COMMAND ----------

# DBTITLE 1,Create the Person Dashboard from dbignite
from dbinterop.data_model import FhirBundles, PersonDashboard
print(f"Loading FHIR bundle data under our database {dbName}")
person_dashboard = PersonDashboard.builder(from_=FhirBundles(BUNDLE_PATH, cdm_database=dbName, cdm_mapping_database=None))
person_dash_df = person_dashboard.summary()
person_dash_df.display()

# COMMAND ----------

# DBTITLE 1,dbignite creates our tables out of the box:
# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1.2/ Exploring the FHIR data
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-2.png" width="700px" style="float: right" />
# MAGIC 
# MAGIC Now that our data has been loaded, we can start running some exploration
# MAGIC 
# MAGIC #### Analyzing our patient informations
# MAGIC 
# MAGIC The FHIR bundle have been extracted and the patient information can now be analyzed using SQL or any python library. 
# MAGIC 
# MAGIC Let's see what's under the `person` table and start running some analysis on top of our dataset..

# COMMAND ----------

# MAGIC %sql select * from person

# COMMAND ----------

# DBTITLE 1,Analyzing Patient Age Distribution 
# MAGIC %sql
# MAGIC select gender_source_value, year(current_date())-year_of_birth as age, count(*) as count from person group by gender_source_value, age order by age
# MAGIC -- Plot: Key: age, group: gender_source_value, Values: count

# COMMAND ----------

# MAGIC %md
# MAGIC #### Patient Condition
# MAGIC 
# MAGIC The condition information are stored under the `condition` table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from condition

# COMMAND ----------

# DBTITLE 1,Top condition per gender, using python and plotly
from pyspark.sql.functions import desc
import plotly.express as px

#We can also leverage pure SQL to access data
df = spark.table("person").join(spark.table("condition"), "person_id") \
          .groupBy(['gender_source_value', 'condition.condition_status']).count() \
          .orderBy(desc('count')).filter('count > 200').toPandas()
#And use our usual plot libraries
px.bar(df, x="condition_status", y="count", color="gender_source_value", barmode="group")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Procedure Occurrence Table
# MAGIC 
# MAGIC Let's explore the `procedure_occurence` extracted from the bundles

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from procedure_occurrence

# COMMAND ----------

# DBTITLE 1,Distribution of procedures per-patient
df = spark.sql("select person_id, count(procedure_occurrence_id) as procedure_count from procedure_occurrence group by person_id having procedure_count < 500").toPandas()
px.histogram(df, x="procedure_count")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Encounter

# COMMAND ----------

# DBTITLE 1,Encounter repartition
df = spark.sql("select encounter_status, count(*) as count from encounter group by encounter_status order by count desc limit 20").toPandas()
px.pie(df, values='count', names='encounter_status', hole=.4)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 2/ Security and governance with Unity Catalog
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-3.png" width="700px" style="float: right; margin-left: 10px" />
# MAGIC 
# MAGIC Databricks Lakehouse add a layer of governance and security on all your resources.
# MAGIC 
# MAGIC We can choose to grant access to these resources to our Data Analysts so that they can have READ access only on these tables. This is done using standard SQL queries.
# MAGIC 
# MAGIC More advanced capabilities are available to support your sensitive use-cases:
# MAGIC 
# MAGIC * **Governance** and **traceability** with audit log (track who access what)
# MAGIC * **Fine grain access** / row level security (mask column containing PII information to a group of users)
# MAGIC * **Lineage** (track the downstream usage: who is using a given table and what are the impacts if you are to change it)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE person TO `my_analyst_user_group`;
# MAGIC GRANT SELECT ON TABLE procedure_occurrence TO `my_analyst_user_group`;
# MAGIC GRANT SELECT ON TABLE encounter_status TO `my_analyst_user_group`;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 3/ Building and sharing visualization (BI/DW)
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-4.png" width="700px"  style="float: right; margin-left: 10px" />
# MAGIC 
# MAGIC 
# MAGIC The lakehouse provides traditional Data warehousing and BI within one single platform and one security layer.
# MAGIC 
# MAGIC Now that our data is ready, we can leverage Databricks SQL capabilities to build interactive dashboard and share analysis. 
# MAGIC 
# MAGIC Open the [DBSQL Patient Summary](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/352c784e-c0e3-4f42-8a66-a4955b7ee5f8-patient-summary?o=1444828305810485) as example.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-patient-dashboard.png" width="500"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Covid Outcomes Analysis
# MAGIC 
# MAGIC Now, let's take a deeper look at the data and explore factors that might affect covid outcomes. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3.1/ Cohort definition
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-5.png" width="700px"  style="float: right; margin-left: 10px" />
# MAGIC 
# MAGIC 
# MAGIC To ensure better reproducibility and organizing the data, we first create patient cohorts based on the criteria of interest (being admitted to hopital, infection status, disease hirtory etc). We then proceed to create features based on cohorts and add the results to databricks feature store.
# MAGIC 
# MAGIC To make data access easier we add cohort tables to our database (similar to OMOP's results schema). See [The Book Of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/CommonDataModel.html#cdm-standardized-tables) for more detail.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS cohort (
# MAGIC   cohort_definition_id INT,
# MAGIC   person_id STRING,
# MAGIC   cohort_start_date DATE,
# MAGIC   cohort_end_date DATE
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS cohort_definition (
# MAGIC   cohort_definition_id INT,
# MAGIC   cohort_definition_name STRING,
# MAGIC   cohort_definition_description STRING,
# MAGIC   cohort_definition_syntax STRING,
# MAGIC   cohort_initiation_date DATE
# MAGIC );
# MAGIC --reset existing cohort if any
# MAGIC DELETE from cohort;  
# MAGIC DELETE from cohort_definition; 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating the cohorts
# MAGIC 
# MAGIC We can now run simple SQL queries create the cohort and fill the `cohort` and `cohort_definition` 

# COMMAND ----------

patient_cohort = """select person_id, to_date(condition_start_datetime) as cohort_start_date, to_date(condition_end_datetime) as cohort_end_date 
                           from condition where condition_code in (840539006)"""

admission_cohort = """select person_id, to_date(first(encounter_period_start)) as cohort_start_date, to_date(first(encounter_period_end)) as cohort_end_date 
                                from encounter where encounter_code in (1505002, 32485007, 305351004, 76464004) group by person_id"""

#You can open the create_cohort function in the companion notebook in ./_resources
create_cohort('covid', 'patient with covid', patient_cohort)
create_cohort('admission', 'patients admitted', admission_cohort)

# COMMAND ----------

# DBTITLE 1,Our 2 cohorts are created: 'covid' and 'admission'
# MAGIC %sql select * from cohort_definition

# COMMAND ----------

# DBTITLE 1,Reviewing the admission cohort
# MAGIC %sql
# MAGIC select c.cohort_definition_id, person_id, cohort_start_date, cohort_end_date from cohort c join cohort_definition cd
# MAGIC   where cd.cohort_definition_id = c.cohort_definition_id and cd.cohort_definition_name = 'admission';

# COMMAND ----------

# DBTITLE 1,Percentage of covid patients admitted to the hospital
covid_admissions_df = spark.sql("""
  with 
    covid as (select c.person_id, cohort_start_date as covid_start from cohort c join cohort_definition cd using (cohort_definition_id) where cd.cohort_definition_name='covid'),
    admission as (select person_id, first(cohort_start_date) as admission_start from cohort c join cohort_definition cd using (cohort_definition_id) where cd.cohort_definition_name='admission' group by person_id)
  select
    case when admission_start between covid_start AND covid_start + interval 30 days then 1 else 0 end as is_admitted, * from covid left join admission using(person_id)""")

covid_admissions_df.selectExpr('100*avg(is_admitted) as percent_admitted').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2/ Analyze correlation between different factors in our cohorts
# MAGIC 
# MAGIC Now let's take a deeper look into the correlations between different factors. 
# MAGIC 
# MAGIC To do that, we'll add disease history and SDOH information to our patient cohort
# MAGIC 
# MAGIC To simplify downstream analysis, we'll flatten the patient conditions and add 1 column per condition (`True` or `False`)
# MAGIC 
# MAGIC *Note: here, we directly create a dataset of disease and SDOH histories, represented as binary values. Alternatively, for each condition and a given timeframe, you can add a cohort of patients, having had that condition and add to the cohort table.*

# COMMAND ----------

# DBTITLE 1,patient comorbidity history
#Condition code we're interested in
conditions_list = {
  "full-time-employment": 160903007,
  "part-time-employment": 160904001,
  "not-in-labor-force": 741062008,
  "received-higher-education": 224299000,
  "has-a-criminal-record": 266948004,
  "unemployed": 73438004,
  "refugee": 446654005,
  "misuses-drugs": 361055000,
  "obesity": 162864005,
  "prediabetes": 15777000,
  "hypertension": 59621000,
  "diabetes": 44054006,
  "coronary-heart-disease": 53741008
}


#Add 1 column per condition (true or false)
def create_patient_history_table(conditions_list):
  patient_history_df = spark.sql('select person_id, first(gender_source_value) as gender, year(current_date) - first(year_of_birth) as age, collect_list(condition.condition_code) as conditions from person left join condition using (person_id) group by person_id')
  for cond_name, cond_code in conditions_list.items():
    patient_history_df = patient_history_df.withColumn(f'history_of_{cond_name}', array_contains('conditions', str(cond_code)))
    
  return patient_history_df.drop('conditions')

patient_history_df = create_patient_history_table(conditions_list)
display(patient_history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To conduct a complete analysis we look at the [mutual information](https://en.wikipedia.org/wiki/Mutual_information) between different features in our dataset. 
# MAGIC 
# MAGIC To calculate mutual information we use [`normalized_mutual_info_score`](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.normalized_mutual_info_score.html) from `sklearn`.

# COMMAND ----------

# DBTITLE 1,Mutual information between different features
#join patient history with covid admission history 
patient_covid_hist = patient_history_df.join(covid_admissions_df.select('person_id', 'is_admitted'), on='person_id')
#save it as table for future analysis
patient_covid_hist.write.mode('overwrite').saveAsTable("patient_covid_hist")
patient_covid_hist_pdf = spark.table("patient_covid_hist").drop('person_id', 'gender', 'age').toPandas()
#For details see implementation in the companion notebook using sklearn normalized_mutual_info_score
plot_pdf = get_normalized_mutual_info_score(patient_covid_hist_pdf)
plot_pdf.style.background_gradient(cmap='Blues').format(precision=2)

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the table above, we see that the highest correlation is between **hospital admissions** and **hypertension**, followed by **coronary-heart-disease**, which seems consistent with the fatcors taken into account in the synthea modlude for covid infections. 
# MAGIC 
# MAGIC On the SDOH side, we see high correlations with part-time employment status. However, we also see high correlation with criminal records which can be example of a spouroious correlation due to small sample size (100).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 4/ Advanced analytics: Predicting the risk of being admitted with COVID
# MAGIC 
# MAGIC As a next step, we'll train binary classifier to predict the outcome (`is_admitted`) based on the features provided in the training data.
# MAGIC 
# MAGIC This model will predict the likelyhood of behing admitted and will help us understanding which feature has the most impact for the model globally but also each patient.
# MAGIC 
# MAGIC We'll use our previous cohort to run this analysis:

# COMMAND ----------

cohort_training = patient_covid_hist.toPandas()
display(cohort_training)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 4.1/ Training our model, leveraging MLFlow and AutoML
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-6.png" width="700px"  style="float: right; margin-left: 10px" />
# MAGIC 
# MAGIC 
# MAGIC We'll train a simple `XGBClassifier` on our model, trying to predict the `is_admitted`.
# MAGIC 
# MAGIC We'll leverage Databricks ML capabilities, including MLFlow to track all our experimentation out of the box, providing among other traceability and reproducibility.
# MAGIC 
# MAGIC Note that to accelerate the model creation, we could also have use Databricks Auto-ML, producing a state of the art notebook with a model ready to be used. <br/>
# MAGIC This typically saves days while providing best practices to the team.

# COMMAND ----------

# Start an mlflow run, which is needed for the feature store to log the model
with mlflow.start_run() as run: 
  # Build a small model for this example
  X_train, X_test, y_train, y_test = prep_data_for_classifier(cohort_training)
  model = Pipeline([('classifier', XGBClassifier())])
  model.fit(X_train, y_train)
  #Log the model & deploy it to our registry for further usage. This will also link it to our notebook.
  mlflow.sklearn.log_model(model, "model",  registered_model_name="field_demos_covid_prediction")
  print(f"Model trained. Accuracy score: {model.score(X_test, y_test)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2/ Model Analysis
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/hls/resources/dbinterop/hls-dbiginte-flow-7.png" width="700px"  style="float: right; margin-left: 10px" />
# MAGIC 
# MAGIC Our model is now trained. Open the right Experiment menu to access MLFlow UI and the model saved in the registry.
# MAGIC 
# MAGIC Using this model, we can predict the probability of a patient being admitted and analyse which features are important to determine that.
# MAGIC 
# MAGIC We'll explain the feature importance in our model and visualize the feature impact on the predictions.

# COMMAND ----------

import shap
explainer = shap.TreeExplainer(model['classifier'])
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
