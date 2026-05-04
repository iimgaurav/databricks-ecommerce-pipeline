"""
Gold Layer — Seller Performance Scorecard.

Business question: "Which sellers perform best? Who needs attention?"
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    lit,
    round as spark_round,
    sum as spark_sum,
    when,
)


def compute_seller_scorecard(df_silver: DataFrame) -> DataFrame:
    """
    Compute seller-level performance metrics with tier classification.

    Input: Silver master table
    Output: One row per seller with GMV, delivery, and tier metrics
    """
    return (
        df_silver
        .filter(col("order_status") == "DELIVERED")
        .groupBy("seller_id")
        .agg(
            spark_round(spark_sum("line_total"), 2).alias("total_gmv"),
            spark_round(avg("price"), 2).alias("avg_selling_price"),
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_unique_id").alias("unique_customers_served"),
            countDistinct("product_category_name").alias("category_count"),
            spark_round(avg("actual_delivery_days"), 2).alias("avg_delivery_days"),
            spark_round(
                spark_sum(when(col("is_late_delivery"), 1).otherwise(0))
                / count("*") * 100, 2
            ).alias("late_delivery_pct"),
        )
        # Seller tier classification
        .withColumn("seller_tier",
                    when(col("total_gmv") >= 50000, lit("platinum"))
                    .when(col("total_gmv") >= 10000, lit("gold"))
                    .when(col("total_gmv") >= 1000, lit("silver"))
                    .otherwise(lit("bronze")))
        .orderBy(col("total_gmv").desc())
    )
