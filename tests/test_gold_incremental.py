"""Compare persisted incremental results with a full refresh after real CDC."""

from pathlib import Path

from dbt.cli.main import dbtRunner

from claims_pipeline.ingest import ingest_landing
from claims_pipeline.quality import run_checks
from claims_pipeline.silver import build_silver
from claims_pipeline.warehouse import connect

from .conftest import SALT, land


def test_incremental_claims_matches_full_refresh(tmp_path, source, landing, monkeypatch):
    warehouse = tmp_path / "warehouse.duckdb"
    project = Path(__file__).resolve().parents[1] / "dbt"
    monkeypatch.setenv("CLAIMS_WAREHOUSE", str(warehouse))
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "target"))
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "logs"))
    # dbt must open its own connection, so close Python's connection before each run.
    def apply(batch_no, batch):
        land(landing, batch_no, batch)
        with connect(warehouse) as con:
            ingest_landing(con, landing)
            build_silver(con, SALT)
            assert all(r["passed"] for r in run_checks(con) if r["severity"] == "error")

    def build(*extra):
        result = dbtRunner().invoke([
            "build", "--project-dir", str(project), "--profiles-dir", str(project),
            *extra,
        ])
        assert result.success, result.exception or result.result

    def rows():
        with connect(warehouse) as con:
            return con.execute("SELECT * FROM gold.fct_claims ORDER BY claim_id").fetchall()

    apply(0, source.snapshot(40, 5, 200))
    build()
    for batch_no in range(1, 4):
        batch = source.incremental(20)
        assert any(c["_op"] == "U" for c in batch["claims"])
        assert any(c["_op"] == "D" for c in batch["claims"])
        assert any(m["_op"] == "U" for m in batch["members"])
        apply(batch_no, batch)
        build()
        incremental = rows()
        assert len(incremental) == len(source.claims)
        assert {r[0] for r in incremental} == set(source.claims)
        build()  # Reprocess the same watermark without any new source data.
        assert rows() == incremental
        build("--full-refresh")
        assert rows() == incremental
