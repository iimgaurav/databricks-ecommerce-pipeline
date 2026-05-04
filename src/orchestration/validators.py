"""
Pipeline Validators — Circuit breakers that prevent bad runs.

WHAT IS A CIRCUIT BREAKER?
Like an electrical circuit breaker that cuts power when there's a fault,
these validators run BEFORE the main pipeline logic and abort early
if upstream data doesn't meet quality standards.

WHY THIS MATTERS IN PRODUCTION:
Without validators, a pipeline might:
- Run Gold aggregations on an empty Silver table (bad deploy wiped it)
- Process stale data from yesterday (source feed was late)
- Write garbage because a schema changed upstream

With validators, you FAIL FAST and alert the on-call engineer.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max

from src.config.settings import PipelineConfig
from src.utils.helpers import get_logger

logger = get_logger(__name__)


def validate_table_exists(
    spark: SparkSession,
    full_table_name: str,
    min_rows: int = 100,
) -> tuple[bool, str]:
    """
    Check that a table exists and has at least `min_rows` rows.

    Returns (passed: bool, message: str)
    """
    try:
        df = spark.table(full_table_name)
        rows = df.count()

        if rows < min_rows:
            return False, f"FAIL — {full_table_name} has only {rows} rows (min: {min_rows})"

        return True, f"PASS — {full_table_name}: {rows:,} rows"

    except Exception as e:
        return False, f"FAIL — {full_table_name} not found: {str(e)}"


def validate_no_critical_nulls(
    spark: SparkSession,
    full_table_name: str,
    columns: list[str],
    max_null_pct: float = 0.0,
) -> tuple[bool, str]:
    """
    Check that critical columns don't exceed the null threshold.

    Parameters
    ----------
    max_null_pct : float — maximum allowed null percentage (0.0 = zero nulls)
    """
    try:
        df = spark.table(full_table_name)
        total = df.count()
        if total == 0:
            return False, f"FAIL — {full_table_name} is empty"

        issues = []
        for col_name in columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total) * 100
            if null_pct > max_null_pct:
                issues.append(f"{col_name}: {null_pct:.1f}% nulls")

        if issues:
            return False, f"FAIL — {full_table_name} null check: {', '.join(issues)}"
        return True, f"PASS — {full_table_name}: no critical nulls"

    except Exception as e:
        return False, f"FAIL — null check error: {str(e)}"


def validate_freshness(
    spark: SparkSession,
    full_table_name: str,
    timestamp_column: str,
    max_staleness_hours: int = 48,
) -> tuple[bool, str]:
    """
    Check that the latest data isn't too old.
    Catches scenarios where the source feed silently stopped.
    """
    try:
        df = spark.table(full_table_name)
        latest = df.agg(spark_max(timestamp_column)).collect()[0][0]

        if latest is None:
            return False, f"FAIL — {full_table_name}: {timestamp_column} is all NULL"

        age_hours = (datetime.now() - latest).total_seconds() / 3600
        if age_hours > max_staleness_hours:
            return False, (
                f"FAIL — {full_table_name}: latest data is {age_hours:.0f}h old "
                f"(max: {max_staleness_hours}h)"
            )

        return True, f"PASS — {full_table_name}: data is {age_hours:.0f}h fresh"

    except Exception as e:
        return False, f"FAIL — freshness check error: {str(e)}"


def run_preflight_checks(spark: SparkSession, config: PipelineConfig) -> bool:
    """
    Run all pre-flight validations before the pipeline executes.

    Returns True if all checks pass, raises Exception otherwise.
    """
    min_rows = config.min_row_threshold

    checks = [
        validate_table_exists(spark, config.bronze_table("orders"), min_rows),
        validate_table_exists(spark, config.bronze_table("order_items"), min_rows),
        validate_table_exists(spark, config.bronze_table("customers"), min_rows),
        validate_table_exists(spark, config.silver_table("orders_master"), min_rows * 5),
        validate_table_exists(spark, config.gold_table("revenue_by_category_monthly"), 10),
        validate_table_exists(spark, config.gold_table("customer_lifetime_value"), min_rows),
    ]

    all_passed = True
    logger.info("=== Pre-flight Validation ===")
    for passed, message in checks:
        status = "✓" if passed else "✗"
        logger.info(f"  {status} {message}")
        if not passed:
            all_passed = False

    if not all_passed:
        raise Exception("Pre-flight validation failed — pipeline aborted.")

    logger.info("All checks passed — pipeline cleared to run.")
    return True
