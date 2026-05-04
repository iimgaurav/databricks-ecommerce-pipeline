"""
Pipeline Metrics — Track every pipeline run in a Delta table.

This gives you observability:
- How long did each stage take?
- How many rows were processed?
- Did it succeed or fail?
- Row count trends over time (regression detection)
"""

import time
import uuid
from datetime import datetime

from pyspark.sql import Row, SparkSession

from src.config.settings import PipelineConfig
from src.utils.helpers import get_logger

logger = get_logger(__name__)


METRICS_DDL = """
    CREATE TABLE IF NOT EXISTS {table} (
        run_id          STRING,
        run_date        STRING,
        task_name       STRING,
        rows_read       LONG,
        rows_written    LONG,
        duration_secs   DOUBLE,
        status          STRING,
        error_message   STRING,
        recorded_at     TIMESTAMP
    )
    USING DELTA
"""


class MetricsCollector:
    """
    Context manager for collecting pipeline run metrics.

    Usage:
        with MetricsCollector(spark, config, "bronze_ingestion") as mc:
            mc.rows_read = 99441
            result = ingest_all_tables(spark, config)
            mc.rows_written = sum(result.values())
    """

    def __init__(
        self,
        spark: SparkSession,
        config: PipelineConfig,
        task_name: str,
        run_date: str = None,
    ):
        self.spark = spark
        self.config = config
        self.task_name = task_name
        self.run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")
        self.run_id = str(uuid.uuid4())[:8]
        self.rows_read = 0
        self.rows_written = 0
        self._start_time = None
        self._metrics_table = config.table(config.silver_schema, "pipeline_run_metrics")

    def __enter__(self):
        """Start timing when entering the context."""
        self._start_time = time.time()
        logger.info(f"[{self.run_id}] Starting task: {self.task_name}")

        # Ensure metrics table exists
        self.spark.sql(METRICS_DDL.format(table=self._metrics_table))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Log metrics when exiting the context (success or failure)."""
        duration = time.time() - self._start_time
        status = "SUCCESS" if exc_type is None else "FAILED"
        error_msg = str(exc_val) if exc_val else ""

        self._write_metric(duration, status, error_msg)

        logger.info(
            f"[{self.run_id}] {self.task_name}: {status} "
            f"({duration:.1f}s, {self.rows_written:,} rows)"
        )

        # Don't suppress exceptions — let them propagate
        return False

    def _write_metric(self, duration: float, status: str, error_message: str):
        """Write a single metric row to the Delta metrics table."""
        metric_row = self.spark.createDataFrame(
            [
                Row(
                    run_id=self.run_id,
                    run_date=self.run_date,
                    task_name=self.task_name,
                    rows_read=int(self.rows_read),
                    rows_written=int(self.rows_written),
                    duration_secs=round(duration, 2),
                    status=status,
                    error_message=error_message[:500],  # truncate long errors
                    recorded_at=datetime.utcnow(),
                )
            ]
        )
        metric_row.write.format("delta").mode("append").saveAsTable(self._metrics_table)
