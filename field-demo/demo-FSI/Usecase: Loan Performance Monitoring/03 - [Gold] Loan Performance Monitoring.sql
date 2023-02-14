-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Compute Business-Level Aggregates with Delinquency Summaries and Seller Details
-- MAGIC 
-- MAGIC <img src = 'https://raw.githubusercontent.com/rportilla-databricks/dbt-asset-mgmt/main/images/GOLD_loan_perf.png'>

-- COMMAND ----------

CREATE
OR REFRESH LIVE TABLE gold_delin_stats as with tot_ct as (
  select
    count(1) ct
  from
    live.silver_svcg
  where
    cast(
      substr(monthly_reporting_period, 0, 4) as integer
    ) = cast(year(current_timestamp()) as integer) - 1
),
delin_ct as (
  select
    count(1) ct
  from
    live.silver_svcg
  where
    (actual_loss_calculation is not null)
    and (
      cast(
        substr(monthly_reporting_period, 0, 4) as integer
      ) = cast(year(current_timestamp()) as integer) - 1
    )
),
delin_amount as (
  select
    sum(actual_loss_calculation) loss
  from
    live.silver_svcg
  where
    cast(
      substr(monthly_reporting_period, 0, 4) as integer
    ) = year(current_timestamp()) -1
)
select
  100 * b.ct / a.ct delinquency_pct,
  -1 * c.loss losses
FROM
  tot_ct a
  join delin_ct b
  join delin_amount c

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a table with the SVCG data filtered by the loan requests that contain information about the interest rate

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE silver_interest_rate_present (
CONSTRAINT drop_interest_rate_rows_null EXPECT (CURRENT_INTEREST_RATE IS NOT NULL) ON VIOLATION DROP ROW)
COMMENT "Cleaned table of the sample svcg data"
TBLPROPERTIES ("quality" = "silver")
AS 
  SELECT *
  FROM LIVE.silver_svcg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load precomputed table of loan seq numbers that have enough data to compute the risk

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW gold_svcg_min_period (
CONSTRAINT drop_rows_min_months EXPECT (count >= 10) ON VIOLATION DROP ROW)
COMMENT "List of loans with at least five months of data"
AS
  SELECT LOAN_SEQUENCE_NUMBER, COUNT(*) as count 
  FROM LIVE.silver_svcg
  GROUP BY LOAN_SEQUENCE_NUMBER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculate the risk on the interest rate per loan sequence number. 

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW gold_risk_per_loan_seq
AS 
  SELECT t1.LOAN_SEQUENCE_NUMBER, t1.RISK
  FROM (
      SELECT LOAN_SEQUENCE_NUMBER, std(CURRENT_INTEREST_RATE) as RISK
      FROM LIVE.silver_interest_rate_present
      GROUP BY LOAN_SEQUENCE_NUMBER
   ) as t1
  INNER JOIN LIVE.gold_svcg_min_period t2 ON t1.LOAN_SEQUENCE_NUMBER = t2.LOAN_SEQUENCE_NUMBER

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_risk_per_loan_seq_with_sector
COMMENT "Risk per loan with the added column if the loan purpose"
TBLPROPERTIES ("quality" = "gold")
AS 
  SELECT t2.LOAN_SEQUENCE_NUMBER, t2.LOAN_PURPOSE, t1.RISK
  FROM LIVE.gold_risk_per_loan_seq as t1
  INNER JOIN (
  SELECT DISTINCT LOAN_SEQUENCE_NUMBER, LOAN_PURPOSE       
  FROM LIVE.silver_origination) t2 ON t1.LOAN_SEQUENCE_NUMBER = t2.LOAN_SEQUENCE_NUMBER

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculate the average risk per loan purpose (sector)

-- COMMAND ----------

CREATE LIVE TABLE average_risk_per_sector
COMMENT "Average risk of the interest rate per loan purpose. The risk was calculated based on the standard deviation on the interest rate per loan over at least ten months"
TBLPROPERTIES ("quality" = "gold")
AS 
  SELECT LOAN_PURPOSE, avg(RISK) as RISK
  FROM LIVE.gold_risk_per_loan_seq_with_sector
GROUP BY LOAN_PURPOSE

-- COMMAND ----------

create
or refresh live table gold_sfl_portfolio_balance as
select
  sum(interest_bearing_upb) portfolio_balance
from
  (
    select
      distinct loan_sequence_number,
      interest_bearing_upb
    from
      (
        select
          x.*,
          row_number() over (
            partition by loan_sequence_number
            order by
              monthly_reporting_period desc
          ) rn
        from
          live.silver_svcg x
      ) Foo
    where
      rn = 1
  ) foo2

-- COMMAND ----------

create or refresh live table  gold_sfl_loan_deliquency_by_type
as 
select loan_purpose, monthly_reporting_period, max(cast(current_loan_delinquency_status as double)) loan_deliquency
from live.silver_origination a join live.silver_svcg b 
using (loan_sequence_number )
where monthly_reporting_Period >= '201912'
and monthly_reporting_period < '202203'
group by loan_purpose, monthly_reporting_period
order by monthly_reporting_period

-- COMMAND ----------

create
or refresh live table gold_monthly_reporting_zero_balance as
select
  substr(zero_balance_effective_date, 0, 4) year,
  case
    when zero_balance_code = '01' then 'Prepaid or Matured'
    when zero_balance_code = '02' then 'Third Party Sale'
    when zero_balance_code = '03' then 'Short Sale or Charge Off'
    when zero_balance_code = '96' then 'Repurchase prior to Property Disposition'
    when zero_balance_code = '09' then 'REO Disposition'
    when zero_balance_code = '15' then 'NPL/RPL Loan Sale'
  end zero_balance_code,
  count(1) frequency
From
  live.silver_svcg
where
  zero_balance_code is not null
  and zero_balance_code != '01'
  and cast(
    substr(zero_balance_effective_date, 0, 4) as integer
  ) between (cast(year(current_timestamp()) as integer) - 2)
  and cast(year(current_timestamp()) as integer)
group by
  case
    when zero_balance_code = '01' then 'Prepaid or Matured'
    when zero_balance_code = '02' then 'Third Party Sale'
    when zero_balance_code = '03' then 'Short Sale or Charge Off'
    when zero_balance_code = '96' then 'Repurchase prior to Property Disposition'
    when zero_balance_code = '09' then 'REO Disposition'
    when zero_balance_code = '15' then 'NPL/RPL Loan Sale'
  end,
  substr(zero_balance_effective_date, 0, 4)

-- COMMAND ----------

-- DBTITLE 1,Create Region-Specific Summary of Concentration Areas for UPB (Unpaid Balance)
create or refresh live table gold_state_origination_upb as
select sum(original_upb) starting_balance, property_state from live.silver_origination group by property_state

-- COMMAND ----------


