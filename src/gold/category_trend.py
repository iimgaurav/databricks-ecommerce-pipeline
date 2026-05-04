"""
Gold Layer — Month-over-Month Category Trend Analysis.

Business question: "Which categories are growing fastest this month vs last?"
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lag, round as spark_round,
)
from pyspark.sql.window import Window


def compute_category_trend(df_gold_revenue: DataFrame) -> DataFrame:
    """
    Compute MoM growth rates per category using window functions.

    Input: Gold revenue_by_category_monthly table
    Output: Each row enriched with previous month data and growth %
    """
    w_mom = (
        Window.partitionBy("product_category_name")
        .orderBy("order_year", "order_month")
    )

    return (
        df_gold_revenue
        .select(
            "order_year", "order_month", "order_month_str",
            "product_category_name", "total_revenue",
            "total_orders", "unique_customers"
        )
        .withColumn("prev_month_revenue",
                    lag("total_revenue", 1).over(w_mom))
        .withColumn("mom_revenue_growth_pct",
                    spark_round(
                        (col("total_revenue") - col("prev_month_revenue"))
                        / col("prev_month_revenue") * 100, 2
                    ))
        .withColumn("prev_month_orders",
                    lag("total_orders", 1).over(w_mom))
        .withColumn("mom_order_growth_pct",
                    spark_round(
                        (col("total_orders") - col("prev_month_orders"))
                        / col("prev_month_orders") * 100, 2
                    ))
        .filter(col("prev_month_revenue").isNotNull())
        .orderBy("product_category_name", "order_year", "order_month")
    )
