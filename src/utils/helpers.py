"""
Utility functions — Audit columns, logging, Spark helpers.

These are small, reusable functions used across all pipeline layers.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit


# ──────────────────────────────────────────────────────────
# STRUCTURED LOGGING
# ──────────────────────────────────────────────────────────

def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Create a structured logger.

    In Databricks, logs go to the driver log4j output.
    Structured logging with consistent formatting makes
    it possible to search logs in Datadog/Splunk/CloudWatch.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


# ──────────────────────────────────────────────────────────
# AUDIT COLUMNS
# ──────────────────────────────────────────────────────────

def add_audit_columns(df: DataFrame, source_name: str, layer: str = "bronze") -> DataFrame:
    """
    Add standard lineage columns to a DataFrame.

    In production, audit columns answer:
    - WHEN was this row ingested?    → _ingested_at
    - WHERE did it come from?        → _source_file
    - WHICH pipeline layer wrote it? → _layer

    These are mandatory for debugging data quality issues.
    """
    return (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(source_name))
        .withColumn("_layer", lit(layer))
    )


def drop_audit_columns(df: DataFrame) -> DataFrame:
    """Remove audit columns when moving to the next layer."""
    audit_cols = ["_ingested_at", "_source_file", "_layer"]
    existing = [c for c in audit_cols if c in df.columns]
    return df.drop(*existing)
