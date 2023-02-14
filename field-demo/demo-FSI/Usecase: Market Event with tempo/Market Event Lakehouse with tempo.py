# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC # Building Market Event Data Models with [tempo](https://github.com/databrickslabs/tempo)
# MAGIC 
# MAGIC ## What is a Quintessential Capital Markets Use Case? *Price Improvement* 
# MAGIC 
# MAGIC Price improvement is the measure of how much better a market order is execute as compared to the bid (in the case of a sell order) or ask (in the case of a buy order). In the diagram below, we see that the price improvement dependents on the bid/ask at the time of order placement vs bid/ask at the time of execution. 
# MAGIC 
# MAGIC Price improvement is a measure that investors pay attention to and is critical as a client reporting metric. 
# MAGIC 
# MAGIC For example, say you place an order to buy 1,000 shares of XYZ stock currently quoted at $5 per share. If your order is executed at $4.99, then you realize $0.01 per share of price improvement, resulting in a total savings of $10.00 (1,000 shares × $0.01).
# MAGIC 
# MAGIC <img src = 'https://github.com/QuentinAmbard/databricks-demo/raw/main/fsi/resources/tempo-fsi-price-improvement.png' width="800px">
# MAGIC 
# MAGIC ## Why Databricks + tempo for Financial Services? 
# MAGIC 
# MAGIC To run analysis on market events such as price improvement, we requires large-scale processing and point-in-time capabilities. Databricks provides both. 
# MAGIC 
# MAGIC `tempo` is a library built on of Spark to process timeseries, abstracting much of the Spark-native syntax away from users. It is meant primarily to simplify time series operations at scale but gives users the full power of distributed computing. 
# MAGIC 
# MAGIC The key reasons to use tempo on Databricks is because tempo utilizes operations such as ZORDER'ing, catalyst, and pandas UDFs, all of which run much faster on Delta Lake tables in Databricks. Most use case in financial services rely on structured time series - everything from daily or hourly sentiment to fine-grained tick data from market events. 
# MAGIC 
# MAGIC Quants, analysts, and engineers can save precious time on boilerplate code, and focus on business problems such as signal creation and risk calculation (Greeks) instead of low-level Spark commands.
# MAGIC 
# MAGIC ### Use Cases for Tempo 
# MAGIC 
# MAGIC The use cases below are just a few samples of the kinds of use cases which tempo easily solves:
# MAGIC 
# MAGIC * Bar calculations
# MAGIC * Best execution (slippage / price improvement) 
# MAGIC * Market Manipulation
# MAGIC * Order Execution Timeliness
# MAGIC * Order Marking
# MAGIC 
# MAGIC ### Typical Data Architecture using Databricks
# MAGIC 
# MAGIC <img src='https://github.com/QuentinAmbard/databricks-demo/raw/main/fsi/resources/tempo-fsi-flow.png' width=1000>
# MAGIC 
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffsi%2Ftempo%2Fnotebook_tempo&dt=FSI_TEMPO">
# MAGIC <!-- [metadata={"description":"Financial timeseries analysis with tempo", "authors":["layla.yang@databricks.com"]}] -->

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### 1. Installing tempo from PyPI
# MAGIC 
# MAGIC We'll be using Tempo to manipulate our timeseries.
# MAGIC 
# MAGIC Databricks provides support for third party libraries. We'll install tempo from PyPI directly. Because we're installing it from the notebook, we'll have user isolation to prevent from any library conflict!

# COMMAND ----------

# DBTITLE 1,Installing temp from PyPI
# MAGIC %pip install dbl-tempo

# COMMAND ----------

# MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### 2. Ingesting data
# MAGIC 
# MAGIC <img style="float:right" src='https://github.com/QuentinAmbard/databricks-demo/raw/main/fsi/resources/tempo-fsi-flow-1.png' width=600>
# MAGIC 
# MAGIC Our first step is to ingest the timeseries data we receive. Databricks can ingest almost all format type, from a variaty of data sources.
# MAGIC 
# MAGIC We have to source of data, being saved as JSON in a blob storage:
# MAGIC 
# MAGIC * Trades, containing the trading events
# MAGIC * Quotes, containing the bid price
# MAGIC 
# MAGIC 
# MAGIC To ingest this information, we cab use Databricks Auto-Loader or SQL `COPY INTO` to incrementally load new received files in a cloud storage.

# COMMAND ----------

# DBTITLE 1,Incrementally ingesting TRADES
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS trades (date string, event_ts timestamp, order_rcvd_ts timestamp, symbol string, trade_dt string, trade_pr double, trade_qt bigint);
# MAGIC COPY INTO trades FROM ( 
# MAGIC   SELECT date, to_timestamp(event_ts) event_ts, to_timestamp(order_rcvd_ts) order_rcvd_ts, symbol, trade_dt, trade_pr, trade_qt
# MAGIC     FROM '/mnt/field-demos/fsi/tempo/incoming_raw_data/trades') FILEFORMAT = JSON;
# MAGIC select * from trades;

# COMMAND ----------

# DBTITLE 1,Incrementally ingesting QUOTES
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS quotes (ask_pr double, bid_pr double, date string, event_ts timestamp, symbol string, trade_dt string);
# MAGIC COPY INTO quotes FROM ( 
# MAGIC   SELECT ask_pr, bid_pr, date, to_timestamp(event_ts) event_ts, symbol, trade_dt
# MAGIC     FROM '/mnt/field-demos/fsi/tempo/incoming_raw_data/quotes') FILEFORMAT = JSON ;
# MAGIC select * from quotes;

# COMMAND ----------

# DBTITLE 1,Define TSDF Time Series Data Structure
from tempo import *
trades_df = spark.table("trades")
quotes_df = spark.table("quotes")

#Load the dataframe as tempo dataframe
trades_tsdf = TSDF(trades_df, partition_cols = ['date', 'symbol'], ts_col = 'event_ts')
quotes_tsdf = TSDF(quotes_df, partition_cols = ['date', 'symbol'], ts_col = 'event_ts')

# COMMAND ----------

# DBTITLE 1,Use Data Preview Features with TSDF Directly
display(trades_tsdf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 2. Interactive Resampling on Tick Data - Our First Step Toward Understanding Source Data for Price Improvement
# MAGIC 
# MAGIC Note that we resample below because it is impossible to visualize big datasets. Resampling is always required and comes out-of-the-box in Databricks Lab tempo as well as native Spark SQL functions

# COMMAND ----------

# DBTITLE 1,Resample TSDF for Visualization Purposes - Intentionally Showing Irregular Time Series
portfolio = ['ROKU']

resampled_sdf = trades_tsdf.resample(freq='min', func='floor')
resampled_pdf = resampled_sdf.df.filter(col('event_ts').cast("date") == "2017-08-31").filter(col("symbol").isNotNull()).filter(col("symbol").isin(portfolio)).toPandas()

# Plotly figure 1
fig = px.line(resampled_pdf, x='event_ts', y='trade_pr',
              color="symbol",
              line_group="symbol", hover_name = "symbol")
fig.update_layout(title='Daily Trade Information' , showlegend=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### 3. Delta Lake for Interactive Time Series Analytics

# COMMAND ----------

# DBTITLE 1,For Delta Optimization, Tempo Offers a Writer
trades_tsdf.write(spark, "silver_delta_trades")

# COMMAND ----------

# DBTITLE 1,Time Travel for Security Master Audit or Tick Data Audit
# MAGIC %sql 
# MAGIC describe history silver_delta_trades

# COMMAND ----------

# MAGIC %sql select * from silver_delta_trades 

# COMMAND ----------

# DBTITLE 1,Take Advantage of Analytics Format for Range Queries (ZORDER on time/ticker)
# MAGIC %sql 
# MAGIC 
# MAGIC select * from silver_delta_trades
# MAGIC where symbol = 'AMH' 
# MAGIC -- newly computed column for filtering!
# MAGIC and event_time between 103400 and 103500

# COMMAND ----------

# DBTITLE 1,Save tables for Use in Databricks SQL - Note Schema Evolution is Supported on Order Lifecycle Events
minute_bars = trades_tsdf.calc_bars(freq = '1 minute')

minute_bars.df.write.mode('overwrite').option("mergeSchema", "true").saveAsTable("gold_bars_minute")

moving_avg = trades_tsdf.withRangeStats("trade_pr", rangeBackWindowSecs=600).df
output = moving_avg.select('symbol', 'event_ts', 'trade_pr', 'mean_trade_pr', 'stddev_trade_pr', 'sum_trade_pr', 'min_trade_pr')

moving_avg.write.mode('overwrite').option("mergeSchema", "true").saveAsTable('gold_sma_10min')

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### 4. Capital Markets Application - Slippage Calculation
# MAGIC 
# MAGIC Slippage (a.k.a price improvement) is the idea that the trade price may deviate significantly from the original bid/ask at the time the order was placed. Brokerage firms, exchanges, and hedge funds care about the price improvement and slippage because it is an indicator for quality of execution. This can be summarized by first apply an AS OF join to get the bid/ask at the time of order placement and then comparing to the received trade price. 
# MAGIC 
# MAGIC [Reference Picture](https://docs.google.com/presentation/d/1Qsxt19UyrH5MwDq_4CTIxsmalX0lMRzkSwcxbjvbpLw/edit#slide=id.g1073a5d0b7e_0_246)
# MAGIC 
# MAGIC ### Why Are AS OF Joins Critical? 
# MAGIC 
# MAGIC Simple joins don't scale. The AS OF join doesn't actually over-shuffle data at all plus normal joins duplicate keys. AS OF joins are used to get point-in-time views of market data and are so popular that all time series databases implement them.
# MAGIC 
# MAGIC <p></p>
# MAGIC 
# MAGIC * **ASSUMPTION** - all trades are buy-trades

# COMMAND ----------

trades_tsdf = TSDF(spark.table("trades"), ts_col = 'event_ts', partition_cols = ["date", "symbol"])
quotes_tsdf = TSDF(spark.table("quotes"), ts_col='event_ts', partition_cols = ["date", "symbol"])

ex_asof = trades_tsdf.asofJoin(quotes_tsdf, right_prefix = "asof_ex_time")

orders_tsdf = TSDF(ex_asof.df, ts_col = 'order_rcvd_ts', partition_cols = ["date", "symbol"])
order_asof = ex_asof.asofJoin(quotes_tsdf, right_prefix = "asof_ord_time")

order_asof.write(spark, "silver_trade_slippage")

# COMMAND ----------

# DBTITLE 1,Savings Per Share - Price Improvement
pi_df = spark.sql("select symbol, trade_qt, order_rcvd_ts, event_ts, (asof_ord_time_ask_pr - trade_pr)  / asof_ord_time_ask_pr price_difference  from silver_trade_slippage where event_ts between '2017-08-31 10:00:00' and '2017-08-31 16:00:00'")

pi_tsdf = TSDF(pi_df, ts_col = 'event_ts', partition_cols =['symbol']).resample(freq='1 minute', func='max', fill=True)

unfilled = TSDF(pi_df, ts_col = 'event_ts', partition_cols =['symbol']).resample(freq='1 minute', func='max').withRangeStats("price_difference",  rangeBackWindowSecs=600)

res = pi_tsdf.asofJoin(unfilled, right_prefix='right')

res.df.write.option("mergeSchema", "true").mode('overwrite').saveAsTable("price_improvement")

display(res.df.filter(col("symbol") == "COST"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### 6. DBSQL Dashboard with Market Event Derived Tables 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/fsi/resources/tempo-fsi-dashboard.png" style="float:right" width="600" />
# MAGIC 
# MAGIC We now have our data ready and accessible as table!
# MAGIC 
# MAGIC We can easily leverage the Lakehouse capabilities and DBSQL to build [dashboards](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/29f43a19-7b27-48dd-b76f-0f19813f3e54-time-series-analytics---tempo?o=1444828305810485) tracking our Market events and bid efficiency
