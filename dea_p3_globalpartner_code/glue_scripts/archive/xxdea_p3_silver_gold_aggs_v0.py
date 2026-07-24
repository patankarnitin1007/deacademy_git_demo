import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Silver -> Gold: core Fact/Dim tables plus the Step 5 business marts.
#
# TRAILING_WINDOW_DAYS: lookback for RFM frequency/monetary ("last N
# months" in the brief). 180 days (~6 months) is a placeholder pending
# SME confirmation - see the "open items" list in the solution design doc.
TRAILING_WINDOW_DAYS = 180
CHURN_THRESHOLD_DAYS = 45

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_S3_PATH', 'GOLD_S3_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER = args['SILVER_S3_PATH'].rstrip('/')
GOLD = args['GOLD_S3_PATH'].rstrip('/')


def read_silver(table_name):
    path = f"{SILVER}/{table_name}/"
    print(f"Reading silver table '{table_name}' from {path}")
    return spark.read.parquet(path)


def write_gold(df, table_name, partition_col=None):
    path = f"{GOLD}/{table_name}/"
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    print(f"Wrote {df.count()} row(s) to {path}")


# --------------------------------------------------------------------------
# Core model
# --------------------------------------------------------------------------
def build_fact_order_item(order_items, options):
    opt_agg = options.groupBy("order_id", "lineitem_id").agg(
        F.sum(
            F.when(F.col("option_price") < 0, -F.col("option_price") * F.col("option_quantity"))
            .otherwise(F.lit(0))
        ).alias("discount_amount"),
        F.sum(
            F.when(F.col("option_price") > 0, F.col("option_price") * F.col("option_quantity"))
            .otherwise(F.lit(0))
        ).alias("addon_amount"),
        F.sum(F.col("option_price") * F.col("option_quantity")).alias("options_net_amount"),
    )

    fact = order_items.join(opt_agg, on=["order_id", "lineitem_id"], how="left").fillna(
        {"discount_amount": 0, "addon_amount": 0, "options_net_amount": 0}
    )

    fact = (
        fact.withColumn("item_gross_amount", F.col("item_price") * F.col("item_quantity"))
        .withColumn("gross_revenue", F.col("item_gross_amount") + F.col("addon_amount"))
        .withColumn("net_revenue", F.col("item_gross_amount") + F.col("options_net_amount"))
        .withColumn("has_discount", F.col("discount_amount") > 0)
    )

    return fact.select(
        "order_id", "lineitem_id", "order_date", "creation_time_utc", "restaurant_id", "user_id",
        "app_name", "is_loyalty", "currency", "item_category", "item_name", "item_price", "item_quantity",
        "item_gross_amount", "discount_amount", "addon_amount", "gross_revenue", "net_revenue", "has_discount",
    )


def build_fact_order(fact_order_item):
    return fact_order_item.groupBy("order_id").agg(
        F.first("order_date").alias("order_date"),
        F.first("restaurant_id").alias("restaurant_id"),
        F.first("user_id").alias("user_id"),
        F.first("app_name").alias("app_name"),
        F.max("is_loyalty").alias("is_loyalty"),
        F.first("currency").alias("currency"),
        F.sum("gross_revenue").alias("gross_revenue"),
        F.sum("discount_amount").alias("discount_amount"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum("item_quantity").alias("item_quantity"),
        F.count("lineitem_id").alias("lineitem_count"),
        F.max("has_discount").alias("has_discount"),
    )


def build_dim_customer(fact_order):
    return fact_order.groupBy("user_id").agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("lifetime_order_count"),
        F.sum("net_revenue").alias("lifetime_net_revenue"),
        F.max("is_loyalty").alias("is_loyalty_member"),
    )


def build_dim_restaurant(fact_order):
    return fact_order.groupBy("restaurant_id").agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.count("order_id").alias("lifetime_order_count"),
    )


# --------------------------------------------------------------------------
# Primary goal: daily-evolving CLV, plus RFM segmentation and churn signal
# on the same snapshot grain (user_id x snapshot_date)
# --------------------------------------------------------------------------
def build_fact_customer_daily(fact_order, date_dim):
    as_of_date = fact_order.agg(F.max("order_date")).collect()[0][0]
    print(f"Building daily customer snapshot through {as_of_date}")

    daily_activity = fact_order.groupBy("user_id", "order_date").agg(
        F.sum("net_revenue").alias("daily_net_revenue"),
        F.count("order_id").alias("daily_order_count"),
    )

    customer_bounds = fact_order.groupBy("user_id").agg(F.min("order_date").alias("first_order_date"))
    calendar = date_dim.select("date_key").filter(F.col("date_key") <= F.lit(as_of_date))

    # Broadcast the small date dimension; the range predicate expands each
    # customer to one row per day from their first order through as_of_date.
    cb = customer_bounds.alias("cb")
    cal = calendar.alias("cal")
    customer_calendar = cb.join(
        F.broadcast(cal), F.col("cal.date_key") >= F.col("cb.first_order_date"), "inner"
    ).select(F.col("cb.user_id").alias("user_id"), F.col("cal.date_key").alias("snapshot_date"))

    joined = customer_calendar.join(
        daily_activity,
        (customer_calendar.user_id == daily_activity.user_id)
        & (customer_calendar.snapshot_date == daily_activity.order_date),
        "left",
    ).select(
        customer_calendar.user_id,
        customer_calendar.snapshot_date,
        F.coalesce(daily_activity.daily_net_revenue, F.lit(0)).alias("daily_net_revenue"),
        F.coalesce(daily_activity.daily_order_count, F.lit(0)).alias("daily_order_count"),
    )

    joined = joined.withColumn("date_index", F.datediff("snapshot_date", F.lit("1970-01-01")))

    cum_window = Window.partitionBy("user_id").orderBy("date_index").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    trail_window = Window.partitionBy("user_id").orderBy("date_index").rangeBetween(
        -TRAILING_WINDOW_DAYS, 0
    )
    prior_trail_window = Window.partitionBy("user_id").orderBy("date_index").rangeBetween(
        -2 * TRAILING_WINDOW_DAYS, -TRAILING_WINDOW_DAYS - 1
    )

    snap = (
        joined.withColumn("orders_to_date", F.sum("daily_order_count").over(cum_window))
        .withColumn("clv_to_date", F.sum("daily_net_revenue").over(cum_window))
        .withColumn(
            "last_order_date",
            F.last(F.when(F.col("daily_order_count") > 0, F.col("snapshot_date")), ignorenulls=True).over(
                cum_window
            ),
        )
        .withColumn("frequency_trailing", F.sum("daily_order_count").over(trail_window))
        .withColumn("monetary_trailing", F.sum("daily_net_revenue").over(trail_window))
        .withColumn("monetary_prior_trailing", F.sum("daily_net_revenue").over(prior_trail_window))
    )

    snap = snap.withColumn("days_since_last_order", F.datediff("snapshot_date", "last_order_date")).withColumn(
        "spend_change_pct",
        F.when(
            F.col("monetary_prior_trailing") > 0,
            F.round(
                (F.col("monetary_trailing") - F.col("monetary_prior_trailing"))
                / F.col("monetary_prior_trailing")
                * 100,
                1,
            ),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn("churn_risk", F.col("days_since_last_order") > CHURN_THRESHOLD_DAYS)

    # Percentile scores computed against the active customer population for
    # that specific day, so the same dollar figure can land in a different
    # tier as the population shifts day to day.
    clv_window = Window.partitionBy("snapshot_date").orderBy(F.desc("clv_to_date"))
    recency_window = Window.partitionBy("snapshot_date").orderBy(F.asc("days_since_last_order"))
    freq_window = Window.partitionBy("snapshot_date").orderBy(F.desc("frequency_trailing"))

    def tier(percentile_col):
        return (
            F.when(percentile_col <= 0.20, F.lit("High"))
            .when(percentile_col <= 0.80, F.lit("Medium"))
            .otherwise(F.lit("Low"))
        )

    snap = (
        snap.withColumn("clv_tier", tier(F.percent_rank().over(clv_window)))
        .withColumn("recency_score", tier(F.percent_rank().over(recency_window)))
        .withColumn("frequency_score", tier(F.percent_rank().over(freq_window)))
    )

    snap = snap.withColumn(
        "rfm_segment",
        F.when(
            (F.col("recency_score") == "High")
            & (F.col("frequency_score") == "High")
            & (F.col("clv_tier") == "High"),
            F.lit("VIP"),
        )
        .when((F.col("recency_score") == "High") & (F.col("frequency_score") == "Low"), F.lit("New Customer"))
        .when((F.col("recency_score") == "Low") & (F.col("frequency_score") == "Low"), F.lit("Churn Risk"))
        .otherwise(F.lit("Regular")),
    )

    return snap.select(
        "user_id", "snapshot_date", "orders_to_date", "clv_to_date", "clv_tier",
        "days_since_last_order", "frequency_trailing", "monetary_trailing", "spend_change_pct",
        "churn_risk", "recency_score", "frequency_score", "rfm_segment",
    )


# --------------------------------------------------------------------------
# Secondary marts
# --------------------------------------------------------------------------
def build_sales_trends(fact_order_item, date_dim):
    return (
        fact_order_item.withColumn("order_hour", F.hour("creation_time_utc"))
        .join(date_dim, fact_order_item.order_date == date_dim.date_key, "left")
        .groupBy("order_date", "year", "month", "week", "is_weekend", "is_holiday", "restaurant_id", "item_category")
        .agg(
            F.sum("net_revenue").alias("net_revenue"),
            F.sum("item_quantity").alias("items_sold"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )


def build_loyalty_impact(fact_order):
    per_customer = fact_order.groupBy("is_loyalty", "user_id").agg(F.count("order_id").alias("orders"))
    customer_stats = per_customer.groupBy("is_loyalty").agg(
        F.count("user_id").alias("customer_count"),
        F.sum(F.when(F.col("orders") > 1, 1).otherwise(0)).alias("repeat_customer_count"),
    )
    order_stats = fact_order.groupBy("is_loyalty").agg(
        F.count("order_id").alias("order_count"),
        F.sum("net_revenue").alias("total_net_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_order_value"),
    )
    result = order_stats.join(customer_stats, on="is_loyalty")
    return result.withColumn(
        "avg_orders_per_customer", F.round(F.col("order_count") / F.col("customer_count"), 2)
    ).withColumn(
        "repeat_customer_rate", F.round(F.col("repeat_customer_count") / F.col("customer_count") * 100, 1)
    )


def build_location_performance(fact_order):
    per_loc = fact_order.groupBy("restaurant_id").agg(
        F.sum("net_revenue").alias("total_net_revenue"),
        F.count("order_id").alias("order_count"),
        F.round(F.avg("net_revenue"), 2).alias("avg_order_value"),
        F.countDistinct("order_date").alias("active_days"),
        F.countDistinct("user_id").alias("customer_count"),
    )
    per_loc = per_loc.withColumn("avg_orders_per_day", F.round(F.col("order_count") / F.col("active_days"), 2))
    rank_window = Window.orderBy(F.desc("total_net_revenue"))
    return per_loc.withColumn("revenue_rank", F.rank().over(rank_window))


def build_discount_effectiveness(fact_order):
    return fact_order.groupBy("has_discount").agg(
        F.count("order_id").alias("order_count"),
        F.sum("gross_revenue").alias("gross_revenue"),
        F.sum("discount_amount").alias("discount_amount"),
        F.sum("net_revenue").alias("net_revenue"),
        F.round(F.avg("net_revenue"), 2).alias("avg_order_value"),
    )


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------
order_items = read_silver("order_items")
order_item_options = read_silver("order_item_options")
date_dim = read_silver("date_dim")

fact_order_item = build_fact_order_item(order_items, order_item_options)
fact_order = build_fact_order(fact_order_item).cache()

write_gold(fact_order_item, "fact_order_item", partition_col="order_date")
write_gold(fact_order, "fact_order", partition_col="order_date")
write_gold(build_dim_customer(fact_order), "dim_customer")
write_gold(build_dim_restaurant(fact_order), "dim_restaurant")
write_gold(date_dim, "dim_date")

write_gold(build_fact_customer_daily(fact_order, date_dim), "fact_customer_daily", partition_col="snapshot_date")
write_gold(build_sales_trends(fact_order_item, date_dim), "sales_trends_agg", partition_col="order_date")
write_gold(build_loyalty_impact(fact_order), "loyalty_impact_agg")
write_gold(build_location_performance(fact_order), "location_performance_agg")
write_gold(build_discount_effectiveness(fact_order), "discount_effectiveness_agg")

job.commit()
