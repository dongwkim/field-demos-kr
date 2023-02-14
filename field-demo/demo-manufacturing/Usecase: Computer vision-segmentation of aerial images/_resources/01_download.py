# Databricks notebook source
dbutils.widgets.text("cloud_storage_path", "/demos/manufacturing/satellite-imaging")

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC # Downloading the data from Kaggle
# MAGIC 
# MAGIC ## Obtain KAGGLE_USERNAME and KAGGLE_KEY for authentication
# MAGIC 
# MAGIC * Instructions on how to obtain this information can be found [here](https://www.kaggle.com/docs/api).
# MAGIC 
# MAGIC * This information will need to be entered below. Please use [secret](https://docs.databricks.com/security/secrets/index.html) to avoid having your key as plain text
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmedia%2Fgaming_toxicity%2Fnotebook_setup_data&dt=MEDIA_USE_CASE_GAMING_TOXICITY">
# MAGIC <!-- [metadata={"description":"Companion notebook to setup data.</i>",
# MAGIC  "authors":["duncan.davis@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %run ./_kaggle_credential

# COMMAND ----------

if "KAGGLE_USERNAME" not in os.environ or os.environ['KAGGLE_USERNAME'] == "" or "KAGGLE_KEY" not in os.environ or os.environ['KAGGLE_KEY'] == "":
  print("You need to specify your KAGGLE USERNAME and KAGGLE KEY to download the data")
  print("Please open notebook under ./_resources/01_download and sepcify your Kaggle credential")
  dbutils.notebook.exit("ERROR: Kaggle credential is required to download the data. Please open notebook under ./_resources/kaggle_credential and specify your Kaggle credential")

# COMMAND ----------

try:
    cloud_storage_path
except NameError:
    cloud_storage_path = dbutils.widgets.get("cloud_storage_path")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Airbus ship detection dataset
# MAGIC 
# MAGIC Get the dataset from kaggle

# COMMAND ----------

# MAGIC %sh
# MAGIC kaggle competitions download -q -c airbus-ship-detection -p /tmp/

# COMMAND ----------

# MAGIC %md
# MAGIC Unzip it 

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -qo /tmp/airbus-ship-detection.zip -d /tmp/airbus-ship-detection/

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# External Imports
from tqdm import tqdm


# TODO: Check the number of optimal threads
def threaded_dbutils_copy(source_directory, target_directory, n_threads=10):
  """
  Copy source directory to target directory with threads.
  
  This function uses threads to execute multiple copy commands to speed up
  the copy process. Especially useful when dealing with multiple small files
  like images.
  
  :param source_directory: directory where the files are going to be copied from
  :param target_directory: directory where the files are going to be copied to
  :param n_threads: number of threads to use, bigger the number, faster the process
  
  Notes
    - Do not include backslashes at the end of the paths.
    - Increasing n_threads will put more load on the driver, keep an eye on the metrics
    to make sure the driver doesn't get overloaded
    - 100 threads pushes a decent driver properly
  """
  
  print("Listing all the paths")
  
  # Creating an empty list for all fiels
  all_files = []
  
  # Recursive search function for discovering all the files
  # TODO: Turn this into a generator
  def recursive_search(_path):
    file_paths = dbutils.fs.ls(_path)
    for file_path in file_paths:
      if file_path.isFile():
        all_files.append(file_path.path)
      else:
        recursive_search(file_path.path)
  
  # Applying recursive search to source directory
  recursive_search(source_directory)
  
  # Formatting path strings
  all_files = [path.split(source_directory)[-1][1:] for path in all_files]
  
  n_files = len(all_files)
  print(f"{n_files} files found")
  print(f"Beginning copy with {n_threads} threads")
  
  # Initiating TQDM with a thread lock for building a progress bar 
  p_bar = tqdm(total=n_files, unit=" copies")
  bar_lock = Lock()
  
  # Defining the work to be executed by a single thread
  def single_thread_copy(file_sub_path):
    dbutils.fs.cp(f"{source_directory}/{file_sub_path}", f"{target_directory}/{file_sub_path}")
    with bar_lock:
      p_bar.update(1)
  
  # Mapping the thread work accross all paths 
  with ThreadPoolExecutor(max_workers=n_threads, thread_name_prefix="copy_thread") as ex:
    ex.map(single_thread_copy, all_files)
  
  # Closing the progress bar
  p_bar.close()
  
  print("Copy complete")
  return

source_dir = "file:/tmp/airbus-ship-detection"
target_dir = f"{cloud_storage_path}"

threaded_dbutils_copy(
  source_directory=source_dir, 
  target_directory=target_dir, 
  n_threads=100
)
