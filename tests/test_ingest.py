from claims_pipeline.ingest import ingest_landing

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
