"""
Gold Layer — Revenue KPIs by category and month.

Business question: "Which categories are growing? Which are declining?"
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    date_format,
    month,
    quarter,
    when,
    year,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)


def compute_revenue_kpis(df_silver: DataFrame) -> DataFrame:
    """
    Compute monthly revenue KPIs per product category.

    Input: Silver master table
    Output: One row per (year, month, category) with revenue metrics
    """
    # Add time dimensions
    df = (
        df_silver
        .withColumn("order_year", year(col("order_purchase_timestamp")))
        .withColumn("order_month", month(col("order_purchase_timestamp")))
        .withColumn("order_quarter", quarter(col("order_purchase_timestamp")))
        .withColumn("order_month_str",
                    date_format(col("order_purchase_timestamp"), "yyyy-MM"))
    )

    return (
        df
        .filter(col("order_status") == "DELIVERED")
        .groupBy(
            "order_year", "order_month", "order_month_str",
            "order_quarter", "product_category_name"
        )
        .agg(
            spark_round(spark_sum("line_total"), 2).alias("total_revenue"),
            spark_round(spark_sum("freight_value"), 2).alias("total_freight"),
            spark_round(spark_sum("price"), 2).alias("total_product_revenue"),
            spark_round(avg("price"), 2).alias("avg_order_value"),
            countDistinct("order_id").alias("total_orders"),
            countDistinct("customer_unique_id").alias("unique_customers"),
            spark_round(avg("actual_delivery_days"), 2).alias("avg_delivery_days"),
            spark_sum(
                when(col("is_late_delivery"), 1).otherwise(0)
            ).alias("late_order_count"),
        )
        .withColumn("late_delivery_rate",
                    spark_round(col("late_order_count") / col("total_orders") * 100, 2))
        .withColumn("freight_as_pct_revenue",
                    spark_round(col("total_freight") / col("total_revenue") * 100, 2))
        .orderBy("order_year", "order_month", col("total_revenue").desc())
    )
