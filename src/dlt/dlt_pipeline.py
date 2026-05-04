"""
Delta Live Tables (DLT) Pipeline Definition.

This file is the ACTUAL DLT pipeline that runs via Databricks DLT.
It is NOT run interactively — it must be wired to a DLT Pipeline
in the Databricks UI or via DABs deployment.

DLT provides:
- Declarative pipeline definition (@dlt.table)
- Built-in data quality expectations (@dlt.expect*)
- Automatic dependency resolution
- Pipeline-level observability (event log)
"""

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, upper, trim, current_timestamp,
    when, lit, round as spark_round,
    sum as spark_sum, avg, countDistinct,
)

VOLUME_PATH = "/Volumes/brazilian-ecommerce/filestore/olis/"


# ─────────────────────────────────────────────
# BRONZE LAYER — Raw ingestion with DLT
# ─────────────────────────────────────────────

@dlt.table(
    name="bronze_orders_dlt",
    comment="Raw orders ingested from CSV source volume",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(VOLUME_PATH + "olist_orders_dataset.csv")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_layer", lit("bronze"))
    )


@dlt.table(
    name="bronze_order_items_dlt",
    comment="Raw order items from CSV source",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(VOLUME_PATH + "olist_order_items_dataset.csv")
        .withColumn("_ingested_at", current_timestamp())
    )


# ─────────────────────────────────────────────
# SILVER LAYER — Clean + validate with expectations
# ─────────────────────────────────────────────

@dlt.table(
    name="silver_orders_dlt",
    comment="Cleaned, typed, deduplicated orders",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_status", "order_status IS NOT NULL")
@dlt.expect_or_drop(
    "valid_purchase_timestamp",
    "order_purchase_timestamp IS NOT NULL"
)
@dlt.expect_or_fail(
    "no_future_orders",
    "order_purchase_timestamp < current_timestamp()"
)
def silver_orders():
    return (
        dlt.read("bronze_orders_dlt")
        .withColumn("order_purchase_timestamp",
                    to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_estimated_delivery_date",
                    to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_delivered_customer_date",
                    to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("is_late_delivery",
                    when(col("order_delivered_customer_date")
                         > col("order_estimated_delivery_date"), True)
                    .otherwise(False))
        .dropDuplicates(["order_id"])
        .filter(col("order_id").isNotNull())
    )


@dlt.table(
    name="silver_order_items_dlt",
    comment="Cleaned order items with line totals",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("positive_price", "price > 0")
@dlt.expect_or_drop("valid_product", "product_id IS NOT NULL")
def silver_order_items():
    return (
        dlt.read("bronze_order_items_dlt")
        .withColumn("line_total",
                    spark_round(col("price") + col("freight_value"), 2))
        .filter(col("price").isNotNull() & (col("price") > 0))
        .dropDuplicates(["order_id", "order_item_id"])
    )


# ─────────────────────────────────────────────
# GOLD LAYER — Business KPIs
# ─────────────────────────────────────────────

@dlt.table(
    name="gold_revenue_summary_dlt",
    comment="Revenue KPIs by order status — executive dashboard feed",
    table_properties={"quality": "gold"},
)
def gold_revenue_summary():
    orders = dlt.read("silver_orders_dlt")
    items = dlt.read("silver_order_items_dlt")

    return (
        items.join(orders, on="order_id", how="inner")
        .filter(col("order_status") == "DELIVERED")
        .groupBy("order_status", "is_late_delivery")
        .agg(
            spark_round(spark_sum("line_total"), 2).alias("total_revenue"),
            countDistinct("order_id").alias("total_orders"),
            spark_round(avg("line_total"), 2).alias("avg_order_value"),
        )
    )
