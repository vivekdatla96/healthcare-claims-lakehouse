from collections import Counter

from claims_pipeline.ingest import ingest_landing
from claims_pipeline.quality import run_checks
from claims_pipeline.silver import build_silver

from .conftest import SALT, land


def _run(con, landing):
    ingest_landing(con, landing)
    return build_silver(con, SALT)


def test_silver_matches_source_after_cdc(con, source, landing):
    land(landing, 0, source.snapshot(50, 8, 300))
    _run(con, landing)
    for b in (1, 2, 3):
        land(landing, b, source.incremental(30))
        _run(con, landing)

    assert con.execute("SELECT count(*) FROM silver.claims").fetchall()[0][0] == len(source.claims)
    status = dict(con.execute(
        "SELECT claim_status, count(*) FROM silver.claims GROUP BY 1").fetchall())
    assert status == dict(Counter(c["claim_status"] for c in source.claims.values()))
    current = con.execute(
        "SELECT count(*) FROM silver.member_history WHERE is_current").fetchall()[0][0]
    assert current == len(source.members)


def test_deleted_claims_are_removed(con, source, landing):
    land(landing, 0, source.snapshot(30, 5, 200))
    land(landing, 1, source.incremental(10))
    _run(con, landing)
    deleted = con.execute(
        "SELECT DISTINCT claim_id FROM bronze.claims WHERE _op = 'D'").fetchall()
    assert deleted
    still_there = con.execute(
        "SELECT count(*) FROM silver.claims WHERE claim_id IN (SELECT claim_id FROM bronze.claims "
        "WHERE _op = 'D')").fetchall()[0][0]
    assert still_there == 0


def test_scd2_keeps_history_when_batches_land_together(con, source, landing):
    land(landing, 0, source.snapshot(40, 5, 50))
    land(landing, 1, source.incremental(5))
    land(landing, 2, source.incremental(5))
    _run(con, landing)  # both incremental batches applied in one run
    history = con.execute("SELECT count(*) FROM silver.member_history").fetchall()[0][0]
    current = con.execute(
        "SELECT count(*) FROM silver.member_history WHERE is_current").fetchall()[0][0]
    assert history > current


def test_rerun_is_a_noop(con, source, landing):
    land(landing, 0, source.snapshot(20, 4, 80))
    _run(con, landing)
    again = _run(con, landing)
    assert all(r.changes == 0 for r in again)


def test_no_raw_phi_in_silver_and_quality_passes(con, source, landing):
    land(landing, 0, source.snapshot(20, 4, 80))
    land(landing, 1, source.incremental(10))
    _run(con, landing)
    results = run_checks(con)
    assert all(r["passed"] for r in results if r["severity"] == "error")
