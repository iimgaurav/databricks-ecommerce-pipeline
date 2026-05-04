# Operations Runbook

## Common Scenarios

### 1. Pipeline Failed — How to Re-run

```bash
# Option A: Re-run from Databricks UI
# Workflows → ecommerce_pipeline → Runs → click failed run → "Repair Run"

# Option B: Re-run from CLI
databricks jobs run-now --job-id <JOB_ID>
```

### 2. Bad Data Deployed — How to Rollback

```sql
-- Step 1: Find the last good version
DESCRIBE HISTORY `brazilian-ecommerce`.`silver`.`orders_master`;

-- Step 2: Restore to that version
RESTORE TABLE `brazilian-ecommerce`.`silver`.`orders_master`
TO VERSION AS OF <last_good_version>;

-- Step 3: Re-run Gold aggregations (they read from Silver)
-- Trigger the gold_aggregation task manually
```

### 3. Add a New Source Table

1. Add schema to `src/config/schemas.py`
2. Add entry to `BRONZE_SCHEMAS` dict
3. Add cleaning function to `src/silver/transformations.py`
4. Add to the master table join in `build_master_table()`
5. Write unit tests in `tests/unit/`
6. Create a PR → CI validates → merge → CD deploys

### 4. Add a New Gold KPI

1. Create `src/gold/new_kpi.py` with a `compute_xxx()` function
2. Import and call it in `src/gold/pipeline.py`
3. Add to `gold_tables` dict in `run_gold_pipeline()`
4. Write tests in `tests/unit/test_gold_aggregations.py`
5. PR → CI → merge → CD deploys

### 5. Check Pipeline Health

```sql
-- Recent runs
SELECT * FROM `brazilian-ecommerce`.`silver`.`pipeline_run_metrics`
ORDER BY recorded_at DESC
LIMIT 20;

-- Failed runs
SELECT * FROM `brazilian-ecommerce`.`silver`.`pipeline_run_metrics`
WHERE status = 'FAILED'
ORDER BY recorded_at DESC;

-- Row count trend (detect regressions)
SELECT task_name, run_date, rows_written
FROM `brazilian-ecommerce`.`silver`.`pipeline_run_metrics`
WHERE task_name = 'silver_transformation'
ORDER BY run_date DESC
LIMIT 30;
```

### 6. Delta Table Maintenance

```sql
-- Run OPTIMIZE (compact small files)
OPTIMIZE `brazilian-ecommerce`.`silver`.`orders_master`
ZORDER BY (order_purchase_timestamp, product_category_name);

-- Run VACUUM (delete old files, keep 7 days)
VACUUM `brazilian-ecommerce`.`silver`.`orders_master` RETAIN 168 HOURS;

-- Refresh statistics
ANALYZE TABLE `brazilian-ecommerce`.`silver`.`orders_master`
COMPUTE STATISTICS FOR ALL COLUMNS;
```

## Alerting Contacts

| Severity | Channel | Who |
|---|---|---|
| P1 (pipeline down) | Email + PagerDuty | On-call data engineer |
| P2 (data quality) | Email | Data engineering team |
| P3 (performance) | Slack #data-alerts | Anyone on the team |
