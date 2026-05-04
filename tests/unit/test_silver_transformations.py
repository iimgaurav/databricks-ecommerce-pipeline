"""
Unit Tests — Silver Layer Transformations.

These tests validate that each cleaning function works correctly
using small, in-memory DataFrames. No Databricks cluster needed.

HOW TO RUN:
    pytest tests/unit/test_silver_transformations.py -v
"""

from src.silver.transformations import (
    aggregate_payments,
    clean_customers,
    clean_order_items,
    clean_orders,
)
from src.utils.helpers import add_audit_columns


class TestCleanOrders:
    """Tests for the orders cleaning function."""

    def test_removes_null_order_ids(self, spark, sample_orders):
        """Orders with null order_id should be filtered out."""
        # Add audit columns (simulating Bronze output)
        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)

        # Check that null PKs are removed
        null_count = result.filter(result.order_id.isNull()).count()
        assert null_count == 0, "Null order_ids should be removed"

    def test_deduplicates_on_order_id(self, spark, sample_orders):
        """Duplicate order_ids should be collapsed to one row."""
        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)

        # Input has 2 rows with ORD001, output should have 1
        ord001_count = result.filter(result.order_id == "ORD001").count()
        assert ord001_count == 1, "Duplicate order_ids should be deduplicated"

    def test_uppercase_status(self, spark, sample_orders):
        """Order status should be uppercased."""
        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)

        statuses = [row.order_status for row in result.select("order_status").collect()]
        for status in statuses:
            assert status == status.upper(), f"Status '{status}' should be uppercase"

    def test_late_delivery_flag(self, spark, sample_orders):
        """is_late_delivery should be True when delivered after estimated."""
        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)

        # ORD002: delivered 2018-06-15, estimated 2018-06-08 → LATE
        ord002 = result.filter(result.order_id == "ORD002").collect()
        if len(ord002) > 0:
            assert ord002[0].is_late_delivery is True, "ORD002 should be marked as late"

        # ORD001: delivered 2018-06-05, estimated 2018-06-10 → ON TIME
        ord001 = result.filter(result.order_id == "ORD001").collect()
        if len(ord001) > 0:
            assert ord001[0].is_late_delivery is False, "ORD001 should NOT be late"

    def test_audit_columns_removed(self, spark, sample_orders):
        """Bronze audit columns should not appear in Silver output."""
        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)

        assert "_ingested_at" not in result.columns
        assert "_source_file" not in result.columns
        assert "_layer" not in result.columns

    def test_timestamp_casting(self, spark, sample_orders):
        """Timestamp strings should be cast to TimestampType."""
        from pyspark.sql.types import TimestampType

        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)

        ts_field = result.schema["order_purchase_timestamp"]
        assert ts_field.dataType == TimestampType(), (
            "order_purchase_timestamp should be TimestampType"
        )

    def test_output_row_count(self, spark, sample_orders):
        """
        Input: 5 rows (1 null PK, 1 duplicate) → Expected: 3 unique valid rows.
        """
        df = add_audit_columns(sample_orders, "test.csv", "bronze")
        result = clean_orders(df)
        assert result.count() == 3


class TestCleanOrderItems:
    """Tests for order items cleaning."""

    def test_filters_null_prices(self, spark, sample_order_items):
        """Items with null price should be removed."""
        df = add_audit_columns(sample_order_items, "test.csv", "bronze")
        result = clean_order_items(df)

        null_prices = result.filter(result.price.isNull()).count()
        assert null_prices == 0

    def test_filters_negative_prices(self, spark, sample_order_items):
        """Items with price <= 0 should be removed."""
        df = add_audit_columns(sample_order_items, "test.csv", "bronze")
        result = clean_order_items(df)

        from pyspark.sql.functions import col

        bad_prices = result.filter(col("price") <= 0).count()
        assert bad_prices == 0

    def test_line_total_computed(self, spark, sample_order_items):
        """line_total should equal price + freight_value."""
        df = add_audit_columns(sample_order_items, "test.csv", "bronze")
        result = clean_order_items(df)

        first_row = (
            result.filter(result.order_id == "ORD001")
            .filter(result.order_item_id == 1)
            .collect()[0]
        )

        expected = round(150.00 + 20.00, 2)
        assert first_row.line_total == expected, (
            f"line_total should be {expected}, got {first_row.line_total}"
        )


class TestCleanCustomers:
    """Tests for customer cleaning."""

    def test_removes_null_customer_ids(self, spark, sample_customers):
        """Customers with null customer_id should be removed."""
        df = add_audit_columns(sample_customers, "test.csv", "bronze")
        result = clean_customers(df)

        null_count = result.filter(result.customer_id.isNull()).count()
        assert null_count == 0

    def test_uppercase_state(self, spark, sample_customers):
        """customer_state should be uppercased."""
        df = add_audit_columns(sample_customers, "test.csv", "bronze")
        result = clean_customers(df)

        states = [row.customer_state for row in result.select("customer_state").collect()]
        for state in states:
            if state:
                assert state == state.upper()


class TestAggregatePayments:
    """Tests for payment aggregation."""

    def test_aggregates_to_order_level(self, spark, sample_payments):
        """Multiple payment rows per order should be collapsed."""
        df = add_audit_columns(sample_payments, "test.csv", "bronze")
        result = aggregate_payments(df)

        # ORD001 has 2 payment rows → should become 1 row
        ord001 = result.filter(result.order_id == "ORD001").collect()
        assert len(ord001) == 1

    def test_total_payment_value(self, spark, sample_payments):
        """total_payment_value should sum all payments for the order."""
        df = add_audit_columns(sample_payments, "test.csv", "bronze")
        result = aggregate_payments(df)

        ord001 = result.filter(result.order_id == "ORD001").collect()[0]
        expected = round(200.00 + 45.50, 2)
        assert ord001.total_payment_value == expected
