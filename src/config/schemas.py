"""
Schema Definitions — Single source of truth for all table schemas.

WHY EXPLICIT SCHEMAS MATTER:
- Without schemas, Spark infers types → strings become strings, numbers become
  strings, dates become strings. You catch errors at query time, not load time.
- With explicit schemas, bad data fails at LOAD time → fast feedback.
- Schemas also serve as documentation of your data contract.

PRODUCTION PATTERN:
In a real pipeline, schemas might live in a schema registry (Confluent, Glue).
For Databricks + Delta Lake, defining them in code and version-controlling
them in Git gives you the same benefit.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ──────────────────────────────────────────────────────────
# BRONZE SCHEMAS — Match the raw CSV column layout exactly.
# All timestamps stay as StringType at Bronze (raw landing).
# Silver is where we cast them to TimestampType.
# ──────────────────────────────────────────────────────────

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", DoubleType(), True),
        StructField("product_length_cm", DoubleType(), True),
        StructField("product_height_cm", DoubleType(), True),
        StructField("product_width_cm", DoubleType(), True),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ]
)


# ──────────────────────────────────────────────────────────
# SCHEMA REGISTRY — Maps table names to schemas.
# Used by the ingestion engine to auto-select the right schema.
# ──────────────────────────────────────────────────────────

BRONZE_SCHEMAS = {
    "orders": {"schema": ORDERS_SCHEMA, "file": "olist_orders_dataset.csv"},
    "order_items": {"schema": ORDER_ITEMS_SCHEMA, "file": "olist_order_items_dataset.csv"},
    "customers": {"schema": CUSTOMERS_SCHEMA, "file": "olist_customers_dataset.csv"},
    "products": {"schema": PRODUCTS_SCHEMA, "file": "olist_products_dataset.csv"},
    "payments": {"schema": PAYMENTS_SCHEMA, "file": "olist_order_payments_dataset.csv"},
}


def get_schema(table_name: str) -> StructType:
    """Get the schema for a Bronze table by name."""
    if table_name not in BRONZE_SCHEMAS:
        raise ValueError(f"Unknown table '{table_name}'. Available: {list(BRONZE_SCHEMAS.keys())}")
    return BRONZE_SCHEMAS[table_name]["schema"]


def get_source_file(table_name: str) -> str:
    """Get the source CSV filename for a Bronze table."""
    if table_name not in BRONZE_SCHEMAS:
        raise ValueError(f"Unknown table '{table_name}'.")
    return BRONZE_SCHEMAS[table_name]["file"]
