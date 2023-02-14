# Databricks notebook source
# MAGIC %pip install mechanicalsoup==1.1.0 pandas==1.3.5 requests==2.28.1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

data_folder = "/dbfs/loan_data_sergio"
clean_folder = True # When true, it will delete the data_folder at start

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions to download the data

# COMMAND ----------

import mechanicalsoup as ms
import requests
from zipfile import ZipFile
import urllib
import os
from io import BytesIO
from urllib import request
from requests import get  # to make GET request
import glob
import pandas as pd
import sys
from pathlib import Path
import pyspark.sql.functions as F
import requests
import time

def login(login, passw, data_folder, initial_range=95, end_range=96, timeout=2):
    print("Pass:" + str(passw))
    url = "https://freddiemac.embs.com/FLoan/secure/auth.php"
    url2 = "https://freddiemac.embs.com/FLoan/Data/download.php"
    s = requests.Session()
    browser = ms.Browser(session=s)
    print("Logging in....")
    while True:
      try:
        login_page = browser.get(url, timeout=timeout)
        break
      except:
        print("Try again...")
    login_form = login_page.soup.find("form", {"class": "form"})
    login_form.find("input", {"name": "username"})["value"] = login
    login_form.find("input", {"name": "password"})["value"] = passw
    while True:
      try:
        response = browser.submit(login_form, login_page.url, timeout=timeout)
        break
      except:
        print("Try again...")
    print(response)
    
    while True:
      try:
        login_page2 = browser.get(url2, timeout=timeout)
        break
      except:
        print("Try again...")
    print("To the continue page...")
    next_form = login_page2.soup.find("form", {"class": "fmform"})
    a = next_form.find("input", {"name": "accept"}).attrs
    a["checked"] = True
    while True:
      try:
        response2 = browser.submit(next_form, login_page2.url, timeout=timeout)
        break
      except:
        print("Try again...")
    
    print("Start Downloading from..." + response2.url)
    table = response2.soup.find("table", {"class": "table1"})
    t = table.find_all("a")
    for x in range(initial_range, end_range):
        print("Download step", x)
        c = "https://freddiemac.embs.com/FLoan/Data/" + t[x]["href"]
        print(c)
        while True:
          try:
            r = s.get(c, timeout=timeout)
            break
          except:
            print("Try again...")
        
        z = ZipFile(BytesIO(r.content))
        z.extractall(data_folder)
        print("done with file: " + c)
    print("Downloaded all sample successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download data

# COMMAND ----------

username = dbutils.secrets.getBytes(scope="portfolio-optimization-demo", key="email")
passw = dbutils.secrets.getBytes(scope="portfolio-optimization-demo", key="password")
data_folder_spark = data_folder.replace("/dbfs", "")
assert clean_folder in [True, False]
if clean_folder:
  dbutils.fs.rm(data_folder_spark, True)
dbutils.fs.mkdirs(data_folder_spark)
login(username, passw, data_folder, initial_range=95, end_range=117)

# COMMAND ----------

display(dbutils.fs.ls(data_folder_spark))

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the dataset
# MAGIC We have downloaded two classes of files:
# MAGIC * sample_orig: this file contains information about the individual mortgage requests. For example, it contains the original value of the property used as colateral in the morgage, the credit score of the requester and more
# MAGIC * sample_svcg: this file contains the monthly information about the status of the mortgages. For example it includes, the remaining debt per month and potential risks of not paying back.

# COMMAND ----------

cols_names_orig = [
     'CREDIT_SCORE',
     'FIRST_PAYMENT_DATE',
     'FIRST_TIME_HOMEBUYER_FLAG',
     'MATURITY_DATE',
     'METROPOLITAN_STATISTICAL_AREA',
     'MORTGAGE_INSURANCE_PERCENTAGE',
     'NUMBER_OF_UNITS',
     'OCCUPANCY_STATUS',
     'ORIGINAL_COMBINED_LOAN-TO-VALUE',
     'ORIGINAL_DEBT_TO_INCOME_RATIO',
     'ORIGINAL_UPB',
     'ORIGINAL_LOAN_TO_VALUE',
     'ORIGINAL_INTEREST_RATE',
     'CHANNEL',
     'PREPAYMENT_PENALTY_MORTGAGE_FLAG',
     'AMORTIZATION_TYPE',
     'PROPERTY_STATE',
     'PROPERTY_TYPE',
     'POSTAL_CODE',
     'LOAN_SEQUENCE_NUMBER',
     'LOAN_PURPOSE',
     'ORIGINAL_LOAN_TERM',
     'NUMBER_OF_BORROWERS',
     'SELLER_NAME',
     'SERVICER_NAME',
     'SUPER_CONFORMING_FLAG',
     'PRE_RELIEF_REFINANCE_LOAN_SEQUENCE_NUMBER',
     'PROGRAM_INDICATOR',
     'RELIEF_REFINANCE_INDICATOR',
     'PROPERTY_VALUATION_METHOD',
     'INTEREST_ONLY_INDICATOR'
]

cols_names_svcg = [
     'LOAN_SEQUENCE_NUMBER',
     'MONTHLY_REPORTING_PERIOD',
     'CURRENT_ACTUAL_UPB',
     'CURRENT_LOAN_DELINQUENCY_STATUS',
     'LOAN_AGE',
     'REMAINING_MONTHS_TO_LEGAL_MATURITY',
     'DEFECT_SETTLEMENT_DATE',
     'MODIFICATION_FLAG',
     'ZERO_BALANCE_CODE',
     'ZERO_BALANCE_EFFECTIVE_DATE',
     'CURRENT_INTEREST_RATE',
     'CURRENT_DEFERRED_UPB',
     'DUE_DATE_OF_LAST_PAID_INSTALLMENT',
     'MI_RECOVERIES',
     'NET_SALE_PROCEEDS',
     'NON_MI_RECOVERIES',
     'EXPENSES',
     'LEGAL_COSTS',
     'MAINTENANCE_AND_PRESERVATION_COSTS',
     'TAXES_AND_INSURANCE',
     'MISCELLANEOUS_EXPENSES',
     'ACTUAL_LOSS_CALCULATION',
     'MODIFICATION_COST',
     'STEP_MODIFICATION_FLAG',
     'DEFERRED_PAYMENT_PLAN',
     'ESTIMATED_LOAN_TO_VALUE',
     'ZERO_BALANCE_REMOVAL_UPB',
     'DELINQUENT_ACCRUED_INTEREST',
     'DELINQUENCY_DUE_TO_DISASTER',
     'BORROWER_ASSISTANCE_STATUS_CODE',
     'CURRENT_MONTH_MODIFICATION_COST',
     'INTEREST_BEARING_UPB'
]

# COMMAND ----------

df_orig = spark.read.option("delimiter", "|").csv(data_folder_spark + "/sample_orig_*.txt").toDF(*cols_names_orig)
df_svcg = spark.read.option("delimiter", "|").csv(data_folder_spark + "/sample_svcg_*.txt").toDF(*cols_names_svcg)

# COMMAND ----------

display(df_orig)

# COMMAND ----------

display(df_svcg)
