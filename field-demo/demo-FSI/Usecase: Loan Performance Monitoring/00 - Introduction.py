# Databricks notebook source
# MAGIC %md
# MAGIC # Mortgage Loan Performance Monitoring Introduction
# MAGIC 
# MAGIC <img src='https://www.corelogic.com/wp-content/uploads/sites/4/2021/05/Loan-Performance-Insights-e1639430812246.jpg' width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ## The notebooks
# MAGIC The following notebooks should be imported with a Delta Live Tables pipeline. They contain the ETL transformations to transform the original data files into business level aggregates:
# MAGIC * <a href="https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/1311671585469936/command/1311671585470375">01 - [Bronze] Loan Performance Monitoring</a>
# MAGIC * <a href="https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/1311671585475055">02 - [Silver] Loan Performance Monitoring</a>
# MAGIC * <a href="https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/1311671585475058/command/1311671585476331">03 - [Gold] Loan Performance Monitoring</a>
# MAGIC 
# MAGIC In the next notebook, there are instructions to access a prebuilt PowerBI dashboard that can be used to monitor the morgage loans over time:
# MAGIC * <a href="https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/1311671585476332/command/1311671585476333">04 - [Platinum] Reporting Summaries</a>
# MAGIC 
# MAGIC The required data is already available on DBFS on the path `dbfs:/loan_data_sergio`. However, if you want to run this demo on another workspace, there is a notebook to download all the data files under <a href="https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/1311671585469935">resources/Data Ingestion</a>. That notebook requires a username and password. You can request them to Freddie Mac <a href="https://freddiemac.embs.com/FLoan/Bin/loginrequest.php">here</a>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Schemas
# MAGIC 
# MAGIC <img src='https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/styles/info_block/public/thumbnails/image/dm-file-formats.jpg?itok=2PE7A_QR' width="400">
# MAGIC 
# MAGIC We will be working with two types of files:
# MAGIC * Origination data files
# MAGIC * Monthly performance data files
# MAGIC 
# MAGIC The full schema of the origination and monthly performance data files are available on <a href="https://www.freddiemac.com/fmac-resources/research/pdf/user_guide.pdf">the user guide</a>. In this notebook, we can see some of the most relevant fields for mortgage loan performance monitoring.
# MAGIC 
# MAGIC It is important to notice that the origination and monthly performance data files contain the LOAN SEQUENCE NUMBER as common fields. This field can be used to join both data files.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Origination Data File
# MAGIC 
# MAGIC Contains information of the loan at the time of the request.
# MAGIC 
# MAGIC | Name | Description | Valid values | Type |
# MAGIC |---|---|---|---|
# MAGIC | **LOAN SEQUENCE NUMBER** | Unique identifier assigned to each loan | PYYQnXXXXXXX; - Product F = FRM and A = ARM; - YYQn = origination year and quarter; and,  - XXXXXXX = randomly assigned digits | Alphanumeric |
# MAGIC | **CREDIT SCORE** | A number, prepared by third parties, summarizing the borrower’s creditworthiness, which may be indicative of the likelihood that the borrower will timely repay future obligations. | 300 - 850  9999 = Not Available, if Credit Score is < 300 or > 850. | Numeric |
# MAGIC | **METROPOLITAN STATISTICAL AREA** | Metropolitan Statistical Areas (MSAs) are defined by the United States Office of Management and Budget (OMB) and have at least one urbanized area with a population of 50,000 or more inhabitants. | Metropolitan Division or MSA Code.  Space (5) = Indicates that the area in which the mortgaged property is located is a) neither an MSA nor a Metropolitan Division, or b) unknown. | Numeric |
# MAGIC | **POSTAL CODE** | The postal code for the location of the mortgaged property | ###00, where “###” represents the first three digits of the 5- digit postal code  Space (5) = Unknown | Numeric |
# MAGIC |...|...|...|...|

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Performance Data File
# MAGIC 
# MAGIC Contains information of the loans over time.
# MAGIC 
# MAGIC | Name | Description | Valid values | Type |
# MAGIC |---|---|---|---|
# MAGIC | **LOAN SEQUENCE NUMBER** | Unique identifier assigned to each loan | PYYQnXXXXXXX; - Product F = FRM and A = ARM; - YYQn = origination year and quarter; and,  - XXXXXXX = randomly assigned digits | Alphanumeric |
# MAGIC | **MONTHLY REPORTING PERIOD** | The as-of month for loan information contained in the loan record. | YYYYMM | Numeric |
# MAGIC | **CURRENT ACTUAL UPB** | The Current Actual UPB reflects the mortgage ending balance as reported by the servicer for the corresponding monthly reporting period. For fixed rate mortgages, this UPB is derived from the mortgage balance as reported by the servicer and includes any scheduled and unscheduled principal reductions applied to the mortgage. | Calculation: (interest bearing UPB) + (noninterest bearing UPB) | Numeric Literal decimal |
# MAGIC | **CURRENT LOAN DELINQUENCY STATUS** | A value corresponding to the number of days the borrower is delinquent, based on the due date of last paid installment (“DDLPI”) reported by servicers to Freddie Mac, and is calculated under the Mortgage Bankers Association (MBA) method | XX = Unknown - 0 = Current, or less than 30 days past due - 1 = 30-59 days delinquent - 2 = 60 – 89 days delinquent - 3 = 90 – 119 days delinquent - And so on… - RA = REO Acquisition - Space (3) = Unavailable | Numeric |
# MAGIC | **CURRENT INTEREST RATE** | Reflects the current interest rate on the mortgage note, taking into account any loan modifications. | Percentage | Numeric |
# MAGIC | **INTEREST BEARING UPB** | The current interest bearing UPB of the modified mortgage. | $ Amount | Numeric |
# MAGIC |...|...|...|...|
