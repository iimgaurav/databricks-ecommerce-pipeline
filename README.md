# Databricks E-Commerce Analytics Pipeline

> Production-grade Medallion Architecture | Delta Lake | PySpark | Unity Catalog | DLT | CI/CD

[![CI Pipeline](https://github.com/iimgaurav/databricks-ecommerce-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/iimgaurav/databricks-ecommerce-pipeline/actions/workflows/ci.yml)
[![CD Pipeline](https://github.com/iimgaurav/databricks-ecommerce-pipeline/actions/workflows/cd.yml/badge.svg)](https://github.com/iimgaurav/databricks-ecommerce-pipeline/actions/workflows/cd.yml)

## Architecture

```
CSV Files (Unity Catalog Volume)
        ↓
   🥉 BRONZE  →  5 raw Delta tables     {catalog}.bronze.*
        ↓
   🥈 SILVER  →  1 master table         {catalog}.silver.orders_master
        ↓
   🥇 GOLD    →  4 KPI tables           {catalog}.gold.*
        ↓
   ⚙️ WORKFLOWS + DLT  →  Orchestration + Quality Gates
```

## Tech Stack

| Technology | Usage |
|---|---|
| **Databricks** | Serverless compute, Workflows, DLT |
| **Delta Lake** | ACID writes, MERGE INTO, time travel, CDF |
| **PySpark** | Transformations, joins, window functions |
| **Unity Catalog** | 3-level namespace, managed tables, Volumes |
| **Delta Live Tables** | `@dlt.expect` quality enforcement |
| **GitHub Actions** | CI/CD (lint, test, build, deploy) |
| **DABs** | Infrastructure-as-code deployment |

## Project Structure

```
├── src/                    # Production Python modules
│   ├── config/             # Settings, schemas, environment YAML
│   ├── bronze/             # Raw data ingestion
│   ├── silver/             # Cleaning, joining, enrichment
│   ├── gold/               # KPI computations (revenue, CLV, seller, trend)
│   ├── dlt/                # Delta Live Tables pipeline
│   ├── orchestration/      # Validators, metrics collection
│   └── utils/              # Audit columns, logging helpers
├── notebooks/              # Thin Databricks notebook wrappers
├── tests/                  # Unit + integration tests
├── .github/workflows/      # CI/CD pipelines
├── databricks.yml          # DABs bundle configuration
└── docs/                   # Architecture, runbook, data dictionary
```

## Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run linter
make lint

# Run tests
make test

# Build wheel
make build
```

### Deploy to Databricks
```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Deploy to staging
make deploy-staging

# Deploy to production (after staging validation)
make deploy-prod
```

## Pipeline Flow

| Notebook | Module | Key Concepts |
|---|---|---|
| `01_bronze_ingestion` | `src.bronze.ingestion` | Explicit schema, audit columns, configurable write mode |
| `02_silver_transformations` | `src.silver.transformations` | Dedup, grain alignment, window functions, ZORDER |
| `03_gold_aggregations` | `src.gold.pipeline` | Revenue KPIs, CLV, seller scorecard, MoM growth |
| `05_pipeline_runner` | All modules | Full pipeline with metrics collection |

## CI/CD Pipeline

```
PR → Lint (ruff) → Test (pytest) → Build (wheel)
                                        ↓
Merge → Deploy Staging → Smoke Test → Manual Approval → Deploy Prod
```

## Dataset

[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — ~99,000 orders across 5 tables (real marketplace data)

## Author

**Gaurav Kumar** | Data Engineer @ TCS
Azure | Databricks | PySpark | Delta Lake
