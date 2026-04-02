

# Databricks E-Commerce Analytics Pipeline

> End-to-end Medallion Architecture | Delta Lake | PySpark | Unity Catalog | DLT

## Architecture
```
CSV Files (Unity Catalog Volume)
        ↓
   🥉 BRONZE  →  5 raw Delta tables     brazilian-ecommerce.bronze.*
        ↓
   🥈 SILVER  →  1 master table         brazilian-ecommerce.silver.orders_master
        ↓
   🥇 GOLD    →  4 KPI tables           brazilian-ecommerce.gold.*
        ↓
   ⚙️ WORKFLOWS + DLT  →  Orchestration + Quality Gates
```

## Tech Stack
| Technology        | How I used it                                      |
|-------------------|----------------------------------------------------|
| Databricks        | Serverless compute, Workflows, DLT                 |
| Delta Lake        | ACID writes, MERGE INTO, time travel, CDF          |
| PySpark           | Transformations, joins, window functions           |
| Unity Catalog     | 3-level namespace, managed tables, Volumes         |
| Delta Live Tables | @dlt.expect quality enforcement                    |

## Notebooks
| Notebook | Key Concepts |
|---|---|
| 01_bronze_ingestion | Explicit schema, audit columns, _delta_log |
| 02_silver_transformations | Dedup, grain alignment, window functions, ZORDER |
| 03_gold_aggregations | Revenue KPIs, CLV, seller scorecard, MoM growth |
| 04_delta_lake_deep_dive | MERGE INTO, SCD Type 2, RESTORE, schema evolution, CDF |
| 05_workflows_and_dlt | Circuit breaker, DLT expectations, widgets, CI/CD |

## Dataset
[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
~99,000 orders across 5 tables — real marketplace data

## Author
**Gaurav Kumar** | Data Engineer @ TCS
Azure | Databricks | PySpark | Delta Lake
