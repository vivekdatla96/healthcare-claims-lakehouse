import pytest

from claims_pipeline.ingest import ingest_landing
from claims_pipeline.quality import DataQualityError, run_checks
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
