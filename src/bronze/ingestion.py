"""
Bronze Layer — Raw data ingestion from CSV → Delta tables.

WHAT THIS DOES:
Takes raw CSV files from Unity Catalog Volumes and writes them as
Delta Lake managed tables in the Bronze schema. This is the "land as-is"
layer — no transformations, just add audit columns.

PRODUCTION CONSIDERATIONS:
1. Explicit schemas (never inferSchema in prod — it's slow and fragile)
2. Audit columns for lineage tracking
3. Configurable write mode: overwrite (dev) vs merge (prod)
4. Error handling with structured logging
"""

from pyspark.sql import DataFrame, SparkSession

from src.config.schemas import BRONZE_SCHEMAS, get_schema, get_source_file
from src.config.settings import PipelineConfig
from src.utils.helpers import add_audit_columns, get_logger

logger = get_logger(__name__)


def read_csv(
    spark: SparkSession,
    config: PipelineConfig,
    table_name: str,
) -> DataFrame:
    """
    Read a single CSV file from the Volume path with explicit schema.

    Parameters
    ----------
    spark : SparkSession
    config : PipelineConfig — provides the volume_path
    table_name : str — key in BRONZE_SCHEMAS (e.g., "orders", "customers")

    Returns
    -------
    DataFrame with audit columns added
    """
    schema = get_schema(table_name)
    filename = get_source_file(table_name)
    full_path = config.volume_path + filename

    logger.info(f"Reading CSV: {full_path}")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("nullValue", "NA")
        .option("emptyValue", None)
        .schema(schema)
        .load(full_path)
    )

    # Add audit columns — who, when, where
    df = add_audit_columns(df, source_name=filename, layer="bronze")

    row_count = df.count()
    logger.info(f"  {table_name}: {row_count:,} rows loaded")

    return df


def write_bronze_table(
    spark: SparkSession,
    df: DataFrame,
    config: PipelineConfig,
    table_name: str,
) -> int:
    """
    Write a DataFrame to a Bronze Delta table.

    Returns the row count written (for metrics logging).
    """
    full_table = config.bronze_table(table_name)
    logger.info(f"Writing Bronze table: {full_table}")

    (
        df.write.format("delta")
        .mode(config.bronze_write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )

    # Verify write
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]["cnt"]
    logger.info(f"  ✓ Written → {full_table} ({count:,} rows)")
    return count


def ingest_all_tables(spark: SparkSession, config: PipelineConfig) -> dict:
    """
    Ingest all 5 Bronze tables from CSV source.

    Returns a dict of {table_name: row_count} for monitoring.
    """
    # Ensure Bronze schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.bronze_full}")
    logger.info(f"Schema ready: {config.bronze_full}")

    results = {}
    for table_name in BRONZE_SCHEMAS:
        try:
            df = read_csv(spark, config, table_name)
            count = write_bronze_table(spark, df, config, table_name)
            results[table_name] = count
        except Exception as e:
            logger.error(f"FAILED to ingest {table_name}: {e}")
            results[table_name] = -1
            raise  # Fail fast — don't continue with partial data

    logger.info("All Bronze tables written successfully!")
    return results
