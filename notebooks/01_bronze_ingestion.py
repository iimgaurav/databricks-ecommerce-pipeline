# Databricks notebook source
# ============================================================
# NOTEBOOK 01 — BRONZE INGESTION (Thin Wrapper)
#
# This notebook does NOT contain business logic.
# All logic lives in src/bronze/ingestion.py.
# This pattern means:
#   - Logic is unit-testable (without Databricks)
#   - Notebooks are just orchestration glue
#   - Code changes go through Git PR review
# ============================================================

# MAGIC %pip install pyyaml

# Load configuration from widget or environment
import sys, os
sys.path.insert(0, os.path.abspath(".."))

from src.config.settings import load_config
from src.bronze.ingestion import ingest_all_tables
from src.orchestration.metrics import MetricsCollector

# Determine environment (set via Workflow parameter or widget)
try:
    env = dbutils.widgets.get("environment")
except Exception:
    env = "dev"

config = load_config(env)
print(f"Environment: {config.environment}")
print(f"Catalog:     {config.catalog}")
print(f"Source:      {config.volume_path}")

# Run Bronze ingestion with metrics collection
with MetricsCollector(spark, config, "bronze_ingestion") as mc:
    results = ingest_all_tables(spark, config)
    mc.rows_written = sum(results.values())
    for name, cnt in results.items():
        print(f"  {name:15} → {cnt:,} rows")
