"""Daily claims lakehouse DAG.

landing -> bronze -> silver -> quality gate -> dbt gold -> dbt tests

The quality gate raises on any `error` check, so gold is never rebuilt from
bad silver data. Retries are safe because every step is idempotent: bronze
skips files it has already fingerprinted and silver works from watermarks.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

PROJECT_DIR = Path(os.getenv("CLAIMS_PROJECT_DIR", Path(__file__).resolve().parents[2]))
DBT_DIR = PROJECT_DIR / "dbt"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="claims_lakehouse_daily",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["claims", "medallion", "phi"],
)
def claims_lakehouse_daily():
    @task
    def ingest_bronze() -> int:
        from claims_pipeline.config import Settings
        from claims_pipeline.ingest import ingest_landing
        from claims_pipeline.warehouse import connect

        s = Settings.from_env()
        with connect(s.warehouse_path) as con:
            return sum(r.rows for r in ingest_landing(con, s.landing_dir) if not r.skipped)

    @task
    def build_silver(_: int) -> dict:
        from claims_pipeline.config import Settings
        from claims_pipeline.silver import build_silver as run
        from claims_pipeline.warehouse import connect

        s = Settings.from_env()
        with connect(s.warehouse_path) as con:
            return {r.entity: r.changes for r in run(con, s.phi_salt)}

    @task
    def quality_gate(_: dict) -> None:
        from claims_pipeline.config import Settings
        from claims_pipeline.quality import run_checks
        from claims_pipeline.warehouse import connect

        s = Settings.from_env()
        with connect(s.warehouse_path) as con:
            run_checks(con)

    @task
    def dbt_build(_: None) -> None:
        subprocess.run(
            ["dbt", "build", "--profiles-dir", str(DBT_DIR), "--project-dir", str(DBT_DIR)],
            check=True,
        )

    dbt_build(quality_gate(build_silver(ingest_bronze())))


claims_lakehouse_daily()
