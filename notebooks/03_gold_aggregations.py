# Databricks notebook source
# ============================================================
# NOTEBOOK 03 — GOLD AGGREGATIONS (Thin Wrapper)
# ============================================================

# MAGIC %pip install pyyaml

import sys, os
sys.path.insert(0, os.path.abspath(".."))

from src.config.settings import load_config
from src.gold.pipeline import run_gold_pipeline
from src.orchestration.metrics import MetricsCollector

try:
    env = dbutils.widgets.get("environment")
except Exception:
    env = "dev"

config = load_config(env)
print(f"Running Gold pipeline in [{config.environment}] environment")

with MetricsCollector(spark, config, "gold_aggregation") as mc:
    results = run_gold_pipeline(spark, config)
    mc.rows_written = sum(results.values())
    for name, cnt in results.items():
        print(f"  {name:35} → {cnt:,} rows")
