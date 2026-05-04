# Databricks notebook source
# ============================================================
# NOTEBOOK 05 — FULL PIPELINE RUNNER
#
# This is the main entry point for the pipeline.
# Runs all stages in order with pre-flight validation.
# ============================================================

# MAGIC %pip install pyyaml

import sys, os
sys.path.insert(0, os.path.abspath(".."))

from src.config.settings import load_config
from src.bronze.ingestion import ingest_all_tables
from src.silver.transformations import run_silver_pipeline
from src.gold.pipeline import run_gold_pipeline
from src.orchestration.validators import run_preflight_checks
from src.orchestration.metrics import MetricsCollector
from src.utils.helpers import get_logger

logger = get_logger("pipeline_runner")

# ── Configuration ──
try:
    env = dbutils.widgets.get("environment")
except Exception:
    env = "dev"

config = load_config(env)
logger.info(f"Pipeline starting: environment={config.environment}, catalog={config.catalog}")

# ── Stage 1: Bronze Ingestion ──
with MetricsCollector(spark, config, "bronze_ingestion") as mc:
    results = ingest_all_tables(spark, config)
    mc.rows_written = sum(results.values())

# ── Stage 2: Silver Transformations ──
with MetricsCollector(spark, config, "silver_transformation") as mc:
    silver_count = run_silver_pipeline(spark, config)
    mc.rows_written = silver_count

# ── Stage 3: Gold Aggregations ──
with MetricsCollector(spark, config, "gold_aggregation") as mc:
    gold_results = run_gold_pipeline(spark, config)
    mc.rows_written = sum(gold_results.values())

# ── Summary ──
logger.info("=" * 55)
logger.info("PIPELINE COMPLETE")
logger.info(f"  Bronze tables : {len(results)}")
logger.info(f"  Silver rows   : {silver_count:,}")
logger.info(f"  Gold tables   : {len(gold_results)}")
logger.info("=" * 55)
