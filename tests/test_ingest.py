from pathlib import Path

import duckdb
import pytest

from claims_pipeline.ingest import ensure_bronze_tables, ingest_file, ingest_landing

from .conftest import land


def test_ingest_is_idempotent(con, source, landing):
    land(landing, 0, source.snapshot(20, 5, 50))
    first = ingest_landing(con, landing)
    second = ingest_landing(con, landing)

    assert sum(r.rows for r in first) == 75
    assert all(r.skipped for r in second)
    assert con.execute("SELECT count(*) FROM bronze.claims").fetchall()[0][0] == 50


def test_bronze_keeps_cdc_metadata(con, source, landing):
    land(landing, 0, source.snapshot(10, 2, 10))
    ingest_landing(con, landing)
    ops = con.execute("SELECT DISTINCT _op FROM bronze.members").fetchall()
    assert ops == [("I",)]


def test_corrected_file_with_same_name_is_loaded_again(con, source, landing):
    land(landing, 0, source.snapshot(5, 2, 5))
    ingest_landing(con, landing)
    path = landing / "claims" / "batch_0000.jsonl"
    path.write_text(path.read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8")
    results = [r for r in ingest_landing(con, landing) if not r.skipped]
    assert [(Path(r.file).name, r.rows) for r in results] == [("batch_0000.jsonl", 1)]


def test_failed_load_is_rolled_back(con, landing):
    ensure_bronze_tables(con)
    path = landing / "claims" / "batch_0000.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text('{"claim_id": "C1"\nnot json\n', encoding="utf-8")
    with pytest.raises(duckdb.Error):
        ingest_file(con, "claims", path)
    assert con.execute("SELECT count(*) FROM bronze.claims").fetchall()[0][0] == 0
    assert con.execute("SELECT count(*) FROM audit.ingest_log").fetchall()[0][0] == 0
