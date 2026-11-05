import pytest

from claims_pipeline.ingest import ingest_landing
from claims_pipeline.quality import Check, DataQualityError, run_checks
from claims_pipeline.silver import build_silver

from .conftest import SALT, land


def test_quality_gate_blocks_bad_data(con, source, landing):
    land(landing, 0, source.snapshot(10, 3, 30))
    ingest_landing(con, landing)
    build_silver(con, SALT)
    con.execute("UPDATE silver.claims SET paid_amount = billed_amount + 1, "
                "allowed_amount = billed_amount WHERE claim_id = 'C00000001'")
    with pytest.raises(DataQualityError, match="claims_paid_le_allowed_le_billed"):
        run_checks(con)
    logged = con.execute(
        "SELECT count(*) FROM audit.dq_results WHERE NOT passed").fetchall()[0][0]
    assert logged >= 1


def test_warn_checks_are_recorded_but_do_not_block(con, source, landing):
    land(landing, 0, source.snapshot(10, 3, 30))
    ingest_landing(con, landing)
    build_silver(con, SALT)
    checks = [Check("always_warns", "SELECT 3", severity="warn"),
              Check("always_passes", "SELECT 0")]
    results = run_checks(con, checks)
    assert [(r["check"], r["passed"], r["failing_rows"]) for r in results] == [
        ("always_warns", False, 3), ("always_passes", True, 0)]
    stored = con.execute("SELECT check_name, severity, passed FROM audit.dq_results "
                         "ORDER BY check_name").fetchall()
    assert stored == [("always_passes", "error", True), ("always_warns", "warn", False)]


def test_each_run_gets_its_own_run_id(con, source, landing):
    land(landing, 0, source.snapshot(10, 3, 30))
    ingest_landing(con, landing)
    build_silver(con, SALT)
    run_checks(con)
    run_checks(con)
    runs = con.execute("SELECT count(DISTINCT run_id) FROM audit.dq_results").fetchall()[0][0]
    assert runs == 2
