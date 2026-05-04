"""
Pipeline Configuration — Centralized, environment-aware settings.

WHY THIS EXISTS:
In the original notebooks, config was hardcoded:
    CATALOG = "brazilian-ecommerce"
    VOLUME_PATH = "/Volumes/brazilian-ecommerce/filestore/olis/"

This is dangerous in production because:
1. You can't have separate dev/staging/prod without changing code
2. One typo in a catalog name can write to the wrong environment
3. No single source of truth for paths and settings

This module uses a dataclass-based config loaded from YAML files.
Each environment (dev/staging/prod) has its own YAML.
"""

from dataclasses import dataclass, field
from typing import Optional
import yaml
import os


@dataclass
class PipelineConfig:
    """Immutable pipeline configuration for a single environment."""

    # ── Core identifiers ──
    environment: str = "dev"
    catalog: str = "brazilian-ecommerce-dev"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    delta_demos_schema: str = "delta_demos"

    # ── Source paths ──
    volume_path: str = "/Volumes/brazilian-ecommerce-dev/filestore/olis/"

    # ── Feature flags ──
    enable_cdf: bool = True              # Change Data Feed
    enable_autoloader: bool = False       # Use Auto Loader instead of batch CSV
    enable_scd2: bool = True              # SCD Type 2 tracking

    # ── Write behaviour ──
    bronze_write_mode: str = "overwrite"  # "overwrite" for dev, "merge" for prod
    optimize_after_write: bool = True     # Run OPTIMIZE after each write

    # ── Delta maintenance ──
    vacuum_retention_hours: int = 168     # 7 days (never go below in prod)
    zorder_columns_silver: list = field(
        default_factory=lambda: ["order_purchase_timestamp", "product_category_name"]
    )

    # ── Monitoring ──
    min_row_threshold: int = 100          # Circuit breaker minimum
    alert_email: str = ""
    log_level: str = "INFO"

    # ── Derived properties ──
    @property
    def bronze_full(self) -> str:
        """Fully qualified bronze schema: `catalog`.`bronze`"""
        return f"`{self.catalog}`.`{self.bronze_schema}`"

    @property
    def silver_full(self) -> str:
        return f"`{self.catalog}`.`{self.silver_schema}`"

    @property
    def gold_full(self) -> str:
        return f"`{self.catalog}`.`{self.gold_schema}`"

    def table(self, schema: str, name: str) -> str:
        """Build a fully qualified table name: `catalog`.`schema`.`table`"""
        return f"`{self.catalog}`.`{schema}`.`{name}`"

    def bronze_table(self, name: str) -> str:
        return self.table(self.bronze_schema, name)

    def silver_table(self, name: str) -> str:
        return self.table(self.silver_schema, name)

    def gold_table(self, name: str) -> str:
        return self.table(self.gold_schema, name)


def load_config(environment: Optional[str] = None) -> PipelineConfig:
    """
    Load configuration for the specified environment.

    Resolution order:
    1. Explicit `environment` argument
    2. PIPELINE_ENV environment variable
    3. Falls back to "dev"

    In Databricks notebooks, you'd call:
        config = load_config()  # reads from widget or env var
    """
    env = environment or os.getenv("PIPELINE_ENV", "dev")

    # Look for config YAML relative to this file's location
    config_dir = os.path.join(os.path.dirname(__file__), "environments")
    config_file = os.path.join(config_dir, f"{env}.yaml")

    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            raw = yaml.safe_load(f) or {}
        return PipelineConfig(environment=env, **raw)
    else:
        # Return defaults (useful for testing without YAML files)
        return PipelineConfig(environment=env)


def load_config_from_widgets(dbutils) -> PipelineConfig:
    """
    Load config using Databricks widget values.
    This is how notebooks receive parameters from Workflows.

    Usage in notebook:
        config = load_config_from_widgets(dbutils)
    """
    try:
        env = dbutils.widgets.get("environment")
    except Exception:
        env = "dev"
    return load_config(env)
