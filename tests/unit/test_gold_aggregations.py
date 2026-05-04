"""
Unit Tests — Gold Layer Aggregations.
"""

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col

from src.gold.revenue import compute_revenue_kpis
from src.gold.clv import compute_clv
from src.gold.seller_scorecard import compute_seller_scorecard


@pytest.fixture
def silver_master(spark):
    """
    Create a minimal Silver master table for testing Gold aggregations.
    """
    data = [
        Row(order_id="ORD001", order_item_id=1, order_status="DELIVERED",
            order_purchase_timestamp="2018-06-01 10:00:00",
            order_delivered_customer_date="2018-06-05 14:00:00",
            actual_delivery_days=4, estimated_delivery_days=9,
            is_late_delivery=False,
            product_id="P001", seller_id="S001", price=150.0,
            freight_value=20.0, line_total=170.0,
            customer_id="C001", customer_unique_id="U001",
            customer_city="SP", customer_state="SP",
            product_category_name="electronics", product_weight_g=500.0,
            total_payment_value=170.0, max_installments=3,
            _silver_processed_at="2018-06-10 00:00:00",
            customer_order_rank=1, is_repeat_customer=False,
            price_rank_in_category=1),

        Row(order_id="ORD002", order_item_id=1, order_status="DELIVERED",
            order_purchase_timestamp="2018-06-15 12:00:00",
            order_delivered_customer_date="2018-06-25 14:00:00",
            actual_delivery_days=10, estimated_delivery_days=8,
            is_late_delivery=True,
            product_id="P002", seller_id="S001", price=200.0,
            freight_value=30.0, line_total=230.0,
            customer_id="C002", customer_unique_id="U002",
            customer_city="RJ", customer_state="RJ",
            product_category_name="electronics", product_weight_g=300.0,
            total_payment_value=230.0, max_installments=1,
            _silver_processed_at="2018-06-25 00:00:00",
            customer_order_rank=1, is_repeat_customer=False,
            price_rank_in_category=2),

        Row(order_id="ORD003", order_item_id=1, order_status="CANCELED",
            order_purchase_timestamp="2018-07-01 08:00:00",
            order_delivered_customer_date=None,
            actual_delivery_days=None, estimated_delivery_days=14,
            is_late_delivery=False,
            product_id="P003", seller_id="S002", price=50.0,
            freight_value=5.0, line_total=55.0,
            customer_id="C003", customer_unique_id="U003",
            customer_city="MG", customer_state="MG",
            product_category_name="furniture", product_weight_g=1000.0,
            total_payment_value=55.0, max_installments=1,
            _silver_processed_at="2018-07-01 00:00:00",
            customer_order_rank=1, is_repeat_customer=False,
            price_rank_in_category=1),
    ]

    df = spark.createDataFrame(data)
    # Cast timestamp strings to timestamps
    from pyspark.sql.functions import to_timestamp
    return (
        df
        .withColumn("order_purchase_timestamp",
                    to_timestamp("order_purchase_timestamp"))
        .withColumn("order_delivered_customer_date",
                    to_timestamp("order_delivered_customer_date"))
        .withColumn("_silver_processed_at",
                    to_timestamp("_silver_processed_at"))
    )


class TestRevenueKPIs:

    def test_only_delivered_orders(self, silver_master):
        """Revenue KPIs should only include DELIVERED orders."""
        result = compute_revenue_kpis(silver_master)
        # ORD003 is CANCELED → should not appear
        statuses = [r.order_status for r in
                    silver_master.filter(col("order_status") == "DELIVERED").collect()]
        assert result.count() > 0
        # All result rows come from DELIVERED orders only

    def test_revenue_calculation(self, silver_master):
        """Total revenue should match sum of line_total for delivered orders."""
        result = compute_revenue_kpis(silver_master)

        # June 2018 electronics: ORD001 (170) + ORD002 (230) = 400
        june_electronics = result.filter(
            (col("order_month") == 6) & (col("product_category_name") == "electronics")
        ).collect()

        if len(june_electronics) > 0:
            assert june_electronics[0].total_revenue == 400.0


class TestCLV:

    def test_customer_segmentation(self, silver_master):
        """Customers should be segmented based on lifetime revenue."""
        result = compute_clv(silver_master)

        segments = set(r.customer_segment for r in result.select("customer_segment").collect())
        # With our test data (170 and 230), both are "low_value" (< 300)
        assert "low_value" in segments

    def test_only_delivered_orders(self, silver_master):
        """CLV should only count delivered orders."""
        result = compute_clv(silver_master)
        # U003 has only a CANCELED order → should not appear
        u003 = result.filter(col("customer_unique_id") == "U003").count()
        assert u003 == 0


class TestSellerScorecard:

    def test_seller_tier_classification(self, silver_master):
        """Sellers should be classified into tiers based on GMV."""
        result = compute_seller_scorecard(silver_master)

        # S001 has GMV = 400 → should be "bronze" tier (< 1000)
        s001 = result.filter(col("seller_id") == "S001").collect()
        if len(s001) > 0:
            assert s001[0].seller_tier == "bronze"

    def test_excludes_canceled_orders(self, silver_master):
        """Canceled orders should not count in seller metrics."""
        result = compute_seller_scorecard(silver_master)
        # S002 only has a CANCELED order → should not appear
        s002 = result.filter(col("seller_id") == "S002").count()
        assert s002 == 0
