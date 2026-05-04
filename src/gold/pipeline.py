"""
Gold Pipeline Runner — Orchestrates all Gold KPI computations.
"""

from pyspark.sql import SparkSession

from src.config.settings import PipelineConfig
from src.gold.category_trend import compute_category_trend
from src.gold.clv import compute_clv
from src.gold.revenue import compute_revenue_kpis
from src.gold.seller_scorecard import compute_seller_scorecard
from src.utils.helpers import get_logger

logger = get_logger(__name__)


def run_gold_pipeline(spark: SparkSession, config: PipelineConfig) -> dict:
    """
    Execute the full Gold pipeline.

    1. Read Silver master table
    2. Compute each KPI independently
    3. Write each to its own Gold Delta table
    4. Run OPTIMIZE on each table

    Returns a dict of {table_name: row_count}.
    """
    # Ensure Gold schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.gold_full}")

    # Read Silver master
    silver_table = config.silver_table("orders_master")
    logger.info(f"Reading Silver source: {silver_table}")
    df_silver = spark.table(silver_table)

    # ── Compute KPIs ──
    logger.info("Computing revenue KPIs...")
    df_revenue = compute_revenue_kpis(df_silver)

    logger.info("Computing customer lifetime value...")
    df_clv = compute_clv(df_silver)

    logger.info("Computing seller scorecards...")
    df_sellers = compute_seller_scorecard(df_silver)

    logger.info("Computing category MoM trends...")
    df_trend = compute_category_trend(df_revenue)

    # ── Write Gold tables ──
    gold_tables = {
        "revenue_by_category_monthly": (df_revenue,  "order_year"),
        "customer_lifetime_value":     (df_clv,      "customer_state"),
        "seller_performance":          (df_sellers,  None),
        "category_mom_trend":          (df_trend,    "order_year"),
    }

    results = {}
    for table_name, (df, partition_col) in gold_tables.items():
        full_table = config.gold_table(table_name)
        logger.info(f"Writing Gold table: {full_table}")

        writer = (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_col:
            writer = writer.partitionBy(partition_col)

        writer.saveAsTable(full_table)

        count = spark.sql(f"SELECT COUNT(*) as c FROM {full_table}").collect()[0]["c"]
        results[table_name] = count
        logger.info(f"  ✓ {full_table} → {count:,} rows")

    # ── OPTIMIZE Gold tables ──
    if config.optimize_after_write:
        for table_name in gold_tables:
            full_table = config.gold_table(table_name)
            spark.sql(f"OPTIMIZE {full_table}")
            logger.info(f"  ✓ OPTIMIZE done: {full_table}")

    logger.info("All Gold tables written and optimized.")
    return results
