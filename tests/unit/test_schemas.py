"""
Unit Tests — Schema definitions.
"""

from src.config.schemas import (
    BRONZE_SCHEMAS,
    CUSTOMERS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    ORDERS_SCHEMA,
    PAYMENTS_SCHEMA,
    PRODUCTS_SCHEMA,
    get_schema,
    get_source_file,
)


class TestSchemaDefinitions:
    def test_orders_schema_field_count(self):
        """Orders schema should have 8 fields."""
        assert len(ORDERS_SCHEMA.fields) == 8

    def test_order_items_schema_field_count(self):
        """Order items schema should have 7 fields."""
        assert len(ORDER_ITEMS_SCHEMA.fields) == 7

    def test_customers_schema_field_count(self):
        assert len(CUSTOMERS_SCHEMA.fields) == 5

    def test_products_schema_field_count(self):
        assert len(PRODUCTS_SCHEMA.fields) == 9

    def test_payments_schema_field_count(self):
        assert len(PAYMENTS_SCHEMA.fields) == 5

    def test_get_schema_valid(self):
        """get_schema should return the correct schema for known tables."""
        schema = get_schema("orders")
        assert schema == ORDERS_SCHEMA

    def test_get_schema_invalid(self):
        """get_schema should raise for unknown table names."""
        import pytest

        with pytest.raises(ValueError, match="Unknown table"):
            get_schema("nonexistent_table")

    def test_get_source_file(self):
        """get_source_file should return the CSV filename."""
        assert get_source_file("orders") == "olist_orders_dataset.csv"
        assert get_source_file("payments") == "olist_order_payments_dataset.csv"

    def test_all_tables_registered(self):
        """All 5 tables should be in BRONZE_SCHEMAS."""
        expected = {"orders", "order_items", "customers", "products", "payments"}
        assert set(BRONZE_SCHEMAS.keys()) == expected


class TestSchemaTypes:
    def test_orders_has_order_id_string(self):
        """order_id should be StringType."""
        from pyspark.sql.types import StringType

        field = ORDERS_SCHEMA["order_id"]
        assert field.dataType == StringType()

    def test_order_items_price_is_double(self):
        """price field should be DoubleType."""
        from pyspark.sql.types import DoubleType

        field = ORDER_ITEMS_SCHEMA["price"]
        assert field.dataType == DoubleType()
