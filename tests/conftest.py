"""
Test Fixtures — Shared SparkSession and sample data for all tests.

HOW TESTING WORKS WITHOUT DATABRICKS:
PySpark can run locally (no cluster needed) using SparkSession.builder.
This conftest.py creates a local SparkSession that all tests share.
Tests create small DataFrames in memory → run transformations → assert results.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for testing.

    scope="session" means ONE SparkSession for ALL tests (much faster).
    The Delta Lake extension is loaded so we can test Delta operations.
    """
    session = (
        SparkSession.builder
        .master("local[2]")    # 2 cores — enough for tests
        .appName("ecommerce-pipeline-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")   # small data = few partitions
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")            # no Spark UI in tests
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_orders(spark):
    """Create a small orders DataFrame for testing."""
    from pyspark.sql.types import StringType, StructField, StructType

    data = [
        ("ORD001", "CUST001", "delivered", "2018-06-01 10:00:00", "2018-06-01 11:00:00",
         "2018-06-02 10:00:00", "2018-06-05 14:00:00", "2018-06-10 00:00:00"),
        ("ORD002", "CUST002", "delivered", "2018-06-02 12:00:00", "2018-06-02 13:00:00",
         "2018-06-03 10:00:00", "2018-06-15 14:00:00", "2018-06-08 00:00:00"),
        ("ORD003", "CUST003", "shipped",   "2018-07-01 08:00:00", None,
         None, None, "2018-07-15 00:00:00"),
        (None,     "CUST004", "canceled",  "2018-07-02 09:00:00", None,
         None, None, None),  # Null PK — should be filtered
        ("ORD001", "CUST001", "delivered", "2018-06-01 10:00:00", "2018-06-01 11:00:00",
         "2018-06-02 10:00:00", "2018-06-05 14:00:00", "2018-06-10 00:00:00"),  # Duplicate
    ]

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_order_items(spark):
    """Create a small order items DataFrame for testing."""
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType,
    )

    data = [
        ("ORD001", 1, "PROD001", "SELL001", "2018-06-05 00:00:00", 150.00, 20.00),
        ("ORD001", 2, "PROD002", "SELL002", "2018-06-05 00:00:00", 75.50,  10.50),
        ("ORD002", 1, "PROD001", "SELL001", "2018-06-06 00:00:00", 200.00, 30.00),
        ("ORD003", 1, "PROD003", "SELL003", "2018-07-10 00:00:00", 50.00,  5.00),
        ("ORD004", 1, "PROD001", "SELL001", "2018-06-05 00:00:00", None,   10.00),  # Null price
        ("ORD005", 1, "PROD002", "SELL002", "2018-06-05 00:00:00", -10.00, 5.00),   # Negative price
    ]

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_customers(spark):
    """Create a small customers DataFrame for testing."""
    from pyspark.sql.types import StringType, StructField, StructType

    data = [
        ("CUST001", "UNIQUE001", "01310", "sao paulo", "sp"),
        ("CUST002", "UNIQUE002", "21040", "rio de janeiro", "rj"),
        ("CUST003", "UNIQUE003", "30130", "belo horizonte", "mg"),
        (None,      "UNIQUE004", "40000", "salvador", "ba"),  # Null PK
    ]

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_payments(spark):
    """Create a small payments DataFrame for testing."""
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType,
    )

    data = [
        ("ORD001", 1, "credit_card", 3, 200.00),
        ("ORD001", 2, "boleto",      1, 45.50),    # Same order, different payment method
        ("ORD002", 1, "credit_card", 1, 230.00),
        ("ORD003", 1, "debit_card",  1, 55.00),
    ]

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ])

    return spark.createDataFrame(data, schema)
