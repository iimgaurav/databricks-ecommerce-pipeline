# Databricks notebook source
# ============================================================
# NOTEBOOK 02 — SILVER TRANSFORMATIONS (Thin Wrapper)
# ============================================================

# MAGIC %pip install pyyaml

import sys, os
sys.path.insert(0, os.path.abspath(".."))

from src.config.settings import load_config
from src.silver.transformations import run_silver_pipeline
from src.orchestration.metrics import MetricsCollector

try:
    env = dbutils.widgets.get("environment")
except Exception:
    env = "dev"

config = load_config(env)
print(f"Running Silver pipeline in [{config.environment}] environment")

with MetricsCollector(spark, config, "silver_transformation") as mc:
    row_count = run_silver_pipeline(spark, config)
    mc.rows_written = row_count
    print(f"Silver master table: {row_count:,} rows")
