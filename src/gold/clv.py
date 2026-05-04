"""
Gold Layer — Customer Lifetime Value (CLV) computation.

Business question: "How much is each customer worth? Who are our VIPs?"
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    datediff,
    lit,
    when,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)


def compute_clv(df_silver: DataFrame) -> DataFrame:
    """
    Compute Customer Lifetime Value with segmentation.

    Input: Silver master table
    Output: One row per customer with lifetime metrics and segment label
    """
    return (
        df_silver.filter(col("order_status") == "DELIVERED")
        .groupBy("customer_unique_id", "customer_state")
        .agg(
            # Financial
            spark_round(spark_sum("line_total"), 2).alias("lifetime_revenue"),
            spark_round(avg("line_total"), 2).alias("avg_order_value"),
            spark_round(spark_sum("freight_value"), 2).alias("total_freight_paid"),
            # Behavioural
            countDistinct("order_id").alias("total_orders"),
            countDistinct("product_category_name").alias("unique_categories_bought"),
            # Time
            spark_min("order_purchase_timestamp").alias("first_order_date"),
            spark_max("order_purchase_timestamp").alias("last_order_date"),
            # Quality
            spark_sum(when(col("is_late_delivery"), 1).otherwise(0)).alias(
                "late_deliveries_received"
            ),
        )
        # Customer tenure
        .withColumn(
            "customer_tenure_days", datediff(col("last_order_date"), col("first_order_date"))
        )
        # Segmentation
        .withColumn(
            "customer_segment",
            when(col("lifetime_revenue") >= 1000, lit("high_value"))
            .when(col("lifetime_revenue") >= 300, lit("mid_value"))
            .otherwise(lit("low_value")),
        )
    )
