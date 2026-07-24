-- Athena / Glue Data Catalog tables over the Gold layer.
-- Replace <GOLD_BUCKET> with your actual bucket name before running.
-- Run once to set up; re-run MSCK REPAIR TABLE after each pipeline run
-- so Athena picks up newly written partitions.

CREATE DATABASE IF NOT EXISTS gold;

-- ---------------------------------------------------------------------
-- Core model
-- ---------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_order_item (
    order_id            string,
    lineitem_id         string,
    creation_time_utc   timestamp,
    restaurant_id       string,
    user_id             string,
    app_name            string,
    is_loyalty          boolean,
    currency            string,
    item_category       string,
    item_name           string,
    item_price          double,
    item_quantity       int,
    item_gross_amount   double,
    discount_amount     double,
    addon_amount        double,
    gross_revenue       double,
    net_revenue         double,
    has_discount        boolean
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/fact_order_item/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_order (
    order_id        string,
    restaurant_id   string,
    user_id         string,
    app_name        string,
    is_loyalty      boolean,
    currency        string,
    gross_revenue   double,
    discount_amount double,
    net_revenue     double,
    item_quantity   bigint,
    lineitem_count  bigint,
    has_discount    boolean
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/fact_order/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_customer (
    user_id                string,
    first_order_date       date,
    last_order_date        date,
    lifetime_order_count   bigint,
    lifetime_net_revenue   double,
    is_loyalty_member      boolean
)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/dim_customer/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_restaurant (
    restaurant_id           string,
    first_order_date        date,
    last_order_date         date,
    lifetime_order_count    bigint
)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/dim_restaurant/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.dim_date (
    date_key        date,
    day_of_week     string,
    week            int,
    month           string,
    year            int,
    is_weekend      boolean,
    is_holiday      boolean,
    holiday_name    string
)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/dim_date/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ---------------------------------------------------------------------
-- Business marts (Step 5 metrics)
-- ---------------------------------------------------------------------

-- Primary goal: daily CLV, plus RFM segmentation and churn signal
CREATE EXTERNAL TABLE IF NOT EXISTS gold.fact_customer_daily (
    user_id                 string,
    orders_to_date          bigint,
    clv_to_date             double,
    clv_tier                string,
    days_since_last_order   int,
    frequency_trailing      bigint,
    monetary_trailing       double,
    spend_change_pct        double,
    churn_risk              boolean,
    recency_score           string,
    frequency_score         string,
    rfm_segment             string
)
PARTITIONED BY (snapshot_date date)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/fact_customer_daily/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.sales_trends_agg (
    year            int,
    month           string,
    week            int,
    is_weekend      boolean,
    is_holiday      boolean,
    restaurant_id   string,
    item_category   string,
    net_revenue     double,
    items_sold      bigint,
    order_count     bigint
)
PARTITIONED BY (order_date date)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/sales_trends_agg/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.loyalty_impact_agg (
    is_loyalty                 boolean,
    order_count                bigint,
    total_net_revenue          double,
    avg_order_value            double,
    customer_count              bigint,
    repeat_customer_count       bigint,
    avg_orders_per_customer     double,
    repeat_customer_rate        double
)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/loyalty_impact_agg/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.location_performance_agg (
    restaurant_id       string,
    total_net_revenue   double,
    order_count         bigint,
    avg_order_value     double,
    active_days         bigint,
    customer_count      bigint,
    avg_orders_per_day  double,
    revenue_rank        int
)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/location_performance_agg/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold.discount_effectiveness_agg (
    has_discount        boolean,
    order_count         bigint,
    gross_revenue       double,
    discount_amount     double,
    net_revenue         double,
    avg_order_value     double
)
STORED AS PARQUET
LOCATION 's3://<GOLD_BUCKET>/gold/discount_effectiveness_agg/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ---------------------------------------------------------------------
-- Register partitions for the partitioned tables. Re-run these two lines
-- after every pipeline run so Athena sees newly written partitions.
-- ---------------------------------------------------------------------
MSCK REPAIR TABLE gold.fact_order_item;
MSCK REPAIR TABLE gold.fact_order;
MSCK REPAIR TABLE gold.fact_customer_daily;
MSCK REPAIR TABLE gold.sales_trends_agg;
