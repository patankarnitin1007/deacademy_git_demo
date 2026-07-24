import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F, types as T
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Bronze -> Silver transform for order_items, order_item_options, date_dim.
#
# Two classes of data-quality issue, handled differently:
#   - HARD requirements: missing/invalid join keys or monetary fields. These
#     rows are quarantined rather than silently dropped, so nothing
#     disappears without a trace.
#   - SOFT fields: cosmetic/descriptive columns. Missing or malformed values
#     are cleaned and defaulted instead of discarding an otherwise valid
#     transaction (e.g. a blank loyalty flag shouldn't cost you the order's
#     revenue).

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'RAW_S3_PATH', 'SILVER_S3_PATH', 'QUARANTINE_S3_PATH'],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW = args['RAW_S3_PATH'].rstrip('/')
SILVER = args['SILVER_S3_PATH'].rstrip('/')
QUARANTINE = args['QUARANTINE_S3_PATH'].rstrip('/')

INT_RE = r'^-?\d+$'
NUMERIC_RE = r'^-?\d+(\.\d+)?$'


def read_raw_csv(table_name, schema):
    path = f"{RAW}/{table_name}/"
    print(f"Reading raw CSV for '{table_name}' from {path}")
    return (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .schema(schema)
        .csv(path)
    )


def trimmed(colname):
    # Written without F.nullif for compatibility with Glue 4.0 (Spark 3.3),
    # which predates that function (added in Spark 3.5 / Glue 5.0).
    c = F.trim(F.col(colname))
    return F.when(c == "", F.lit(None)).otherwise(c)


def to_boolean(colname, default=None):
    c = F.upper(trimmed(colname))
    parsed = (
        F.when(c.isin("TRUE", "1", "YES", "Y"), F.lit(True))
        .when(c.isin("FALSE", "0", "NO", "N"), F.lit(False))
        .otherwise(F.lit(None).cast(T.BooleanType()))
    )
    if default is None:
        return parsed
    return F.coalesce(parsed, F.lit(default))


def safe_number(colname, pattern):
    c = trimmed(colname)
    return F.when(c.rlike(pattern), c)


def quarantine_reasons(*checks):
    """checks: list of (condition_is_bad, message) tuples."""
    parts = [F.when(cond, F.lit(msg)) for cond, msg in checks]
    return F.array_remove(F.array(*parts), None)


def split_valid_invalid(df):
    invalid = (
        df.filter(F.size("_reject_reasons") > 0)
        .withColumn("_reject_reason", F.concat_ws("; ", "_reject_reasons"))
        .drop("_reject_reasons")
    )
    valid = df.filter(F.size("_reject_reasons") == 0).drop("_reject_reasons")
    return valid, invalid


def write_silver(df, table_name, partition_col=None):
    path = f"{SILVER}/{table_name}/"
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.parquet(path)
    print(f"Wrote {df.count()} valid row(s) to {path}")


def write_quarantine(df, table_name):
    path = f"{QUARANTINE}/{table_name}/"
    count = df.count()
    if count == 0:
        print(f"No quarantined rows for '{table_name}'.")
        return
    df.write.mode("overwrite").parquet(path)
    print(f"Wrote {count} quarantined row(s) to {path}")


# --------------------------------------------------------------------------
# order_items
# --------------------------------------------------------------------------
def transform_order_items():
    schema = T.StructType([
        T.StructField("app_name", T.StringType()),
        T.StructField("restaurant_id", T.StringType()),
        T.StructField("creation_time_utc", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("printed_card_number", T.StringType()),
        T.StructField("is_loyalty", T.StringType()),
        T.StructField("currency", T.StringType()),
        T.StructField("lineitem_id", T.StringType()),
        T.StructField("item_category", T.StringType()),
        T.StructField("item_name", T.StringType()),
        T.StructField("item_price", T.StringType()),
        T.StructField("item_quantity", T.StringType()),
    ])

    raw = read_raw_csv("order_items", schema)
    raw_count = raw.count()

    df = raw.select(
        trimmed("app_name").alias("app_name"),
        trimmed("restaurant_id").alias("restaurant_id"),
        F.to_timestamp(trimmed("creation_time_utc")).alias("creation_time_utc"),
        trimmed("order_id").alias("order_id"),
        trimmed("user_id").alias("user_id"),
        trimmed("printed_card_number").alias("printed_card_number"),
        to_boolean("is_loyalty", default=False).alias("is_loyalty"),
        F.coalesce(F.upper(trimmed("currency")), F.lit("USD")).alias("currency"),
        trimmed("lineitem_id").alias("lineitem_id"),
        F.coalesce(trimmed("item_category"), F.lit("Uncategorized")).alias("item_category"),
        trimmed("item_name").alias("item_name"),
        safe_number("item_price", NUMERIC_RE).cast(T.DecimalType(10, 2)).alias("item_price"),
        safe_number("item_quantity", INT_RE).cast(T.IntegerType()).alias("item_quantity"),
    )

    df = df.withColumn(
        "_reject_reasons",
        quarantine_reasons(
            (F.col("order_id").isNull(), "missing order_id"),
            (F.col("lineitem_id").isNull(), "missing lineitem_id"),
            (F.col("user_id").isNull(), "missing user_id"),
            (F.col("restaurant_id").isNull(), "missing restaurant_id"),
            (F.col("creation_time_utc").isNull(), "invalid or missing creation_time_utc"),
            (F.col("item_price").isNull(), "missing or non-numeric item_price"),
            (F.col("item_price") < 0, "negative item_price"),
            (F.col("item_quantity").isNull(), "missing or non-integer item_quantity"),
            (F.col("item_quantity") <= 0, "non-positive item_quantity"),
        ),
    )

    valid, invalid = split_valid_invalid(df)
    valid = valid.withColumn("order_date", F.to_date("creation_time_utc"))
    valid = valid.dropDuplicates(["lineitem_id"])

    print(f"order_items: raw={raw_count}, valid={valid.count()}, quarantined={invalid.count()}")
    write_silver(valid, "order_items", partition_col="order_date")
    write_quarantine(invalid, "order_items")


# --------------------------------------------------------------------------
# order_item_options
# --------------------------------------------------------------------------
def transform_order_item_options():
    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("lineitem_id", T.StringType()),
        T.StructField("option_group_name", T.StringType()),
        T.StructField("option_name", T.StringType()),
        T.StructField("option_price", T.StringType()),
        T.StructField("option_quantity", T.StringType()),
    ])

    raw = read_raw_csv("order_item_options", schema)
    raw_count = raw.count()

    df = raw.select(
        trimmed("order_id").alias("order_id"),
        trimmed("lineitem_id").alias("lineitem_id"),
        F.coalesce(trimmed("option_group_name"), F.lit("General")).alias("option_group_name"),
        trimmed("option_name").alias("option_name"),
        safe_number("option_price", NUMERIC_RE).cast(T.DecimalType(10, 2)).alias("option_price"),
        F.coalesce(
            safe_number("option_quantity", INT_RE).cast(T.IntegerType()), F.lit(1)
        ).alias("option_quantity"),
    )

    df = df.withColumn(
        "_reject_reasons",
        quarantine_reasons(
            (F.col("order_id").isNull(), "missing order_id"),
            (F.col("lineitem_id").isNull(), "missing lineitem_id"),
            (F.col("option_name").isNull(), "missing option_name"),
            (F.col("option_price").isNull(), "missing or non-numeric option_price"),
            (F.col("option_quantity") < 0, "negative option_quantity"),
        ),
    )

    valid, invalid = split_valid_invalid(df)
    valid = valid.dropDuplicates(["order_id", "lineitem_id", "option_group_name", "option_name"])

    print(f"order_item_options: raw={raw_count}, valid={valid.count()}, quarantined={invalid.count()}")
    write_silver(valid, "order_item_options")
    write_quarantine(invalid, "order_item_options")


# --------------------------------------------------------------------------
# date_dim
# --------------------------------------------------------------------------
def transform_date_dim():
    schema = T.StructType([
        T.StructField("date_key", T.StringType()),
        T.StructField("day_of_week", T.StringType()),
        T.StructField("week", T.StringType()),
        T.StructField("month", T.StringType()),
        T.StructField("year", T.StringType()),
        T.StructField("is_weekend", T.StringType()),
        T.StructField("is_holiday", T.StringType()),
        T.StructField("holiday_name", T.StringType()),
    ])

    raw = read_raw_csv("date_dim", schema)
    raw_count = raw.count()

    df = raw.select(
        F.to_date(trimmed("date_key")).alias("date_key"),
        trimmed("day_of_week").alias("day_of_week"),
        safe_number("week", INT_RE).cast(T.IntegerType()).alias("week"),
        trimmed("month").alias("month"),
        safe_number("year", INT_RE).cast(T.IntegerType()).alias("year"),
        to_boolean("is_weekend").alias("is_weekend"),
        to_boolean("is_holiday", default=False).alias("is_holiday"),
        trimmed("holiday_name").alias("holiday_name"),
    )

    # Soft fields derived from date_key when the source left them blank.
    df = df.withColumn(
        "day_of_week", F.coalesce(F.col("day_of_week"), F.date_format("date_key", "EEEE"))
    ).withColumn(
        "month", F.coalesce(F.col("month"), F.date_format("date_key", "MMMM"))
    ).withColumn(
        "is_weekend",
        F.coalesce(F.col("is_weekend"), F.dayofweek("date_key").isin(1, 7)),
    )

    df = df.withColumn(
        "_reject_reasons",
        quarantine_reasons(
            (F.col("date_key").isNull(), "invalid or missing date_key"),
            (F.col("year").isNull(), "missing or non-integer year"),
            (F.col("week").isNull(), "missing or non-integer week"),
            ((F.col("week") < 1) | (F.col("week") > 53), "week out of range 1-53"),
        ),
    )

    valid, invalid = split_valid_invalid(df)
    valid = valid.dropDuplicates(["date_key"])

    print(f"date_dim: raw={raw_count}, valid={valid.count()}, quarantined={invalid.count()}")
    write_silver(valid, "date_dim")
    write_quarantine(invalid, "date_dim")


transform_order_items()
transform_order_item_options()
transform_date_dim()

job.commit()
