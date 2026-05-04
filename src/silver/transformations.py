"""
Silver Layer — Clean, join, enrich, and build the master table.

WHAT THIS DOES:
Reads from Bronze Delta tables, applies cleaning rules (type casting,
null handling, dedup), joins all 5 tables into a single denormalized
master table, and enriches with window functions.

DESIGN PRINCIPLE:
Every function is PURE: takes a DataFrame, returns a DataFrame.
No side effects, no spark.sql() inside transformations.
This makes every function independently testable.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, trim, upper,
    datediff, round as spark_round,
    count, sum as spark_sum, avg, max as spark_max,
    row_number, rank, current_timestamp, coalesce,
)
from pyspark.sql.window import Window

from src.config.settings import PipelineConfig
from src.utils.helpers import drop_audit_columns, get_logger

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────
# INDIVIDUAL TABLE CLEANING FUNCTIONS
# Each is pure: DataFrame → DataFrame (testable in isolation)
# ──────────────────────────────────────────────────────────

def clean_orders(df: DataFrame) -> DataFrame:
    """
    Clean the orders table:
    - Drop audit columns from Bronze
    - Cast all timestamp strings to proper TimestampType
    - Compute delivery metrics (actual_delivery_days, is_late_delivery)
    - Standardise status to uppercase
    - Drop nulls on PK and deduplicate
    """
    return (
        drop_audit_columns(df)

        # Cast timestamps
        .withColumn("order_purchase_timestamp",
                    to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_approved_at",
                    to_timestamp(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_carrier_date",
                    to_timestamp(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_customer_date",
                    to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_estimated_delivery_date",
                    to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"))

        # Derived delivery metrics
        .withColumn("actual_delivery_days",
                    datediff(col("order_delivered_customer_date"),
                             col("order_purchase_timestamp")))
        .withColumn("estimated_delivery_days",
                    datediff(col("order_estimated_delivery_date"),
                             col("order_purchase_timestamp")))
        .withColumn("is_late_delivery",
                    when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                         lit(True)).otherwise(lit(False)))

        # Standardise
        .withColumn("order_status", upper(trim(col("order_status"))))

        # Data quality — PK must exist, no duplicates
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    """Clean order items: cast timestamps, compute line_total, filter invalid prices."""
    return (
        drop_audit_columns(df)
        .withColumn("shipping_limit_date",
                    to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("line_total",
                    spark_round(col("price") + col("freight_value"), 2))
        .filter(col("price").isNotNull() & (col("price") > 0))
        .dropDuplicates(["order_id", "order_item_id"])  # composite PK
    )


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean customers: standardise state/city, filter null PKs, dedup."""
    return (
        drop_audit_columns(df)
        .withColumn("customer_state", upper(trim(col("customer_state"))))
        .withColumn("customer_city", trim(col("customer_city")))
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def clean_products(df: DataFrame) -> DataFrame:
    """Clean products: fill missing categories, filter null PKs, dedup."""
    return (
        drop_audit_columns(df)
        .withColumn("product_category_name",
                    coalesce(col("product_category_name"), lit("unknown")))
        .filter(col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def aggregate_payments(df: DataFrame) -> DataFrame:
    """
    Aggregate payments to order level.
    One order can have multiple payment rows (installments, multiple methods).
    We roll them up before joining to avoid grain explosions.
    """
    return (
        drop_audit_columns(df)
        .groupBy("order_id")
        .agg(
            spark_round(spark_sum("payment_value"), 2).alias("total_payment_value"),
            spark_max("payment_installments").alias("max_installments"),
            count("payment_sequential").alias("payment_rows"),
        )
    )


# ──────────────────────────────────────────────────────────
# MASTER TABLE BUILD — Join all cleaned tables
# ──────────────────────────────────────────────────────────

def build_master_table(
    df_orders: DataFrame,
    df_items: DataFrame,
    df_customers: DataFrame,
    df_products: DataFrame,
    df_payments_agg: DataFrame,
) -> DataFrame:
    """
    Build the Silver master table by joining all cleaned tables.

    Join strategy:
    - items INNER JOIN orders  (items must have a valid order)
    - LEFT JOIN customers      (keep order even if customer lookup fails)
    - LEFT JOIN products       (keep item even if product lookup fails)
    - LEFT JOIN payments       (keep order even if payment missing)
    """
    return (
        df_items.alias("i")

        .join(df_orders.alias("o"), on="order_id", how="inner")
        .join(df_customers.alias("c"), on="customer_id", how="left")
        .join(df_products.alias("p"), on="product_id", how="left")
        .join(df_payments_agg.alias("pay"), on="order_id", how="left")

        .select(
            # Order identifiers
            col("order_id"),
            col("order_item_id"),
            col("order_status"),
            col("order_purchase_timestamp"),
            col("order_delivered_customer_date"),

            # Delivery metrics
            col("actual_delivery_days"),
            col("estimated_delivery_days"),
            col("is_late_delivery"),

            # Item financials
            col("i.product_id"),
            col("i.seller_id"),
            col("i.price"),
            col("i.freight_value"),
            col("i.line_total"),

            # Customer
            col("customer_id"),
            col("c.customer_unique_id"),
            col("c.customer_city"),
            col("c.customer_state"),

            # Product
            col("p.product_category_name"),
            col("p.product_weight_g"),

            # Payment
            col("pay.total_payment_value"),
            col("pay.max_installments"),

            # Silver audit
            current_timestamp().alias("_silver_processed_at"),
        )
    )


# ──────────────────────────────────────────────────────────
# WINDOW FUNCTION ENRICHMENT
# ──────────────────────────────────────────────────────────

def enrich_with_windows(df: DataFrame) -> DataFrame:
    """
    Add window-function-based columns:
    - customer_order_rank: which order is this for the customer? (1st, 2nd...)
    - is_repeat_customer: True if rank > 1
    - price_rank_in_category: rank of this item's price within its category
    """
    w_customer = (
        Window.partitionBy("customer_unique_id")
        .orderBy(col("order_purchase_timestamp").asc())
    )
    w_category = (
        Window.partitionBy("product_category_name")
        .orderBy(col("line_total").desc())
    )

    return (
        df
        .withColumn("customer_order_rank", row_number().over(w_customer))
        .withColumn("is_repeat_customer",
                    when(col("customer_order_rank") > 1, True).otherwise(False))
        .withColumn("price_rank_in_category", rank().over(w_category))
    )


# ──────────────────────────────────────────────────────────
# ORCHESTRATOR — Runs the full Silver pipeline
# ──────────────────────────────────────────────────────────

def run_silver_pipeline(spark: SparkSession, config: PipelineConfig) -> int:
    """
    Execute the full Silver pipeline:
    1. Read all Bronze tables
    2. Clean each independently
    3. Build master table via joins
    4. Enrich with window functions
    5. Write to Silver Delta table
    6. OPTIMIZE + ZORDER

    Returns the row count of the Silver master table.
    """
    # Ensure Silver schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.silver_full}")

    # Read Bronze tables
    logger.info("Reading Bronze tables...")
    df_orders    = spark.table(config.bronze_table("orders"))
    df_items     = spark.table(config.bronze_table("order_items"))
    df_customers = spark.table(config.bronze_table("customers"))
    df_products  = spark.table(config.bronze_table("products"))
    df_payments  = spark.table(config.bronze_table("payments"))

    # Clean each table
    logger.info("Cleaning tables...")
    orders_clean    = clean_orders(df_orders)
    items_clean     = clean_order_items(df_items)
    customers_clean = clean_customers(df_customers)
    products_clean  = clean_products(df_products)
    payments_agg    = aggregate_payments(df_payments)

    # Build master table
    logger.info("Building master table...")
    master = build_master_table(
        orders_clean, items_clean, customers_clean,
        products_clean, payments_agg
    )

    # Enrich with window functions
    logger.info("Enriching with window functions...")
    enriched = enrich_with_windows(master)

    # Write Silver table
    silver_table = config.silver_table("orders_master")
    logger.info(f"Writing Silver table: {silver_table}")

    (
        enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("customer_state")
        .saveAsTable(silver_table)
    )

    # OPTIMIZE + ZORDER
    if config.optimize_after_write:
        zorder_cols = ", ".join(config.zorder_columns_silver)
        spark.sql(f"OPTIMIZE {silver_table} ZORDER BY ({zorder_cols})")
        logger.info("OPTIMIZE + ZORDER complete.")

    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").collect()[0]["cnt"]
    logger.info(f"Silver master table ready: {count:,} rows")
    return count
