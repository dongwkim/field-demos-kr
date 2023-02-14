# Databricks notebook source
dbutils.widgets.text('path', '/demo/stadium_recommender', 'Storage Path')
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databrickslabs/dbldatagen faker

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=media

# COMMAND ----------

path = dbutils.widgets.get('path')

# COMMAND ----------

#as the path changes from the source notebook, we make sure it'll work calling this notebook or from the parent one.
import pathlib
data_path = str(pathlib.Path().resolve())
if not data_path.endswith("_resources"):
  data_path += "/_resources"

# COMMAND ----------

print("Vendors Item type data generation...")
import pandas as pd
from pyspark.sql.functions import col
pandasDF = pd.read_csv(f"{data_path}/data/vendors.csv")
df_items = spark.createDataFrame(pandasDF, ['vendor_id', 'vendor_location_number', 'vendor_name', 'vendor_scope', 'section', 'item_id', 'item_type', 'item', 'price', 'error'])
for c in "vendor_id", "vendor_location_number", "section", "item_id", "price":
  df_items = df_items.withColumn(c, col(c).cast('int'))
df_items.repartition(1).write.mode("overwrite").option("header", "true").format("csv").save(f"{path}/vendors")

# COMMAND ----------

print("Game data generation...")
from pyspark.sql.functions import col, to_date
pandasDF = pd.read_csv(f"{data_path}/data/games.csv")
df_games = spark.createDataFrame(pandasDF, ['game_id','date_time','date','location','home_team','away_team','error','game_date'])
df_games = df_games.withColumn("game_id", col("game_id").cast('int'))
df_games = df_games.withColumn("game_date", to_date(col("game_date"),'yyyy-MM-dd'))
df_games.repartition(1).write.mode("overwrite").option("header", "true").format("csv").save(f"{path}/games")

# COMMAND ----------

print("Ticket sales data generation...")
import dbldatagen as dg
from pyspark.sql.types import *
from pyspark.sql import functions as F
from faker import Faker
fake = Faker()

fake_name = F.udf(fake.name)
fake_phone = F.udf(fake.phone_number)

data_rows = 70000
df_spec = (dg.DataGenerator(spark, name="ticket_sales", rows=data_rows, partitions=4)
                            .withIdOutput()
                            .withColumn("game_id", IntegerType(), values=['3'])
                            .withColumn("ticket_price", IntegerType(), expr="floor(rand() * 350)")
                            .withColumn("gate_entrance", StringType(), values=['a', 'b', 'c', 'd', 'e', ], random=True, weights=[9, 1, 1, 2, 2])
                            .withColumn("section_number", IntegerType(), minValue=100, maxValue=347, random=True)
                            .withColumn("row_number", IntegerType(), minValue=1, maxValue=35 , random=True)
                            .withColumn("seat_number", IntegerType(), minValue=1, maxValue=30, random=True)
                            .withColumn("ticket_type", StringType(), values=['single_game', 'packaged_game', 'season_holder'], random=True, weights=[7, 2, 1]))
df_tickets = df_spec.build().withColumnRenamed("id", "customer_id")
df_tickets = df_tickets.withColumn("customer_name", fake_name())
df_tickets = df_tickets.withColumn("phone_number", fake_phone())
df_tickets.repartition(4).write.mode("overwrite").format("json").save(f"{path}/ticket_sales")

# COMMAND ----------

print("Point of sales generation...")
data_rows = 1000000
df_spec = (dg.DataGenerator(spark, name="point_of_sale", rows=data_rows, partitions=4)
                            .withIdOutput()
                            .withColumn("game", IntegerType(), minValue=1, maxValue=97 , random=True)
                            .withColumn("item_purchased", IntegerType(), minValue=1, maxValue=364, random=True)
                            .withColumn("customer", IntegerType(), minValue=1, maxValue=1000, random=True))
                            
df_pos = df_spec.build().withColumnRenamed("id", "order_id")

# Write the data to a delta table.
df_pos.write.mode("overwrite").format("json").save(f"{path}/point_of_sale")

# COMMAND ----------

print("Purchase History to evaluate model efficiency after real game...")
import pandas as pd
pandasDF = pd.read_csv(f"{data_path}/data/purchase_history.csv")
spark.createDataFrame(pandasDF, ['item_id', 'game_id', 'customer_id', 'recommended_item_purchased']).repartition(1).write.mode('overwrite').saveAsTable(f"{dbName}.purchase_history")

# COMMAND ----------

#Let's cleanup _started and _commited _committed files

for f in dbutils.fs.ls(path):
  for file in dbutils.fs.ls(f.path):
    if ("_SUCCESS" in file.path or "_committed_" in file.path or "_started" in file.path) and "stadium_recommender" in file.path:
      print(f"deleting {file.path}")
      dbutils.fs.rm(file.path)
