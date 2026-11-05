from collections import Counter

from claims_pipeline.ingest import ingest_landing
from claims_pipeline.phi import tokenize
from claims_pipeline.quality import run_checks
from claims_pipeline.silver import build_silver, migrate_member_generalization

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


def test_member_history_from_before_zip3_masking_is_migrated(con, source, landing):
    land(landing, 0, source.snapshot(20, 4, 40))
    _run(con, landing)
    member_id = con.execute("SELECT min(member_id) FROM bronze.members").fetchall()[0][0]
    token = tokenize(member_id, SALT)
    # Recreate the member as the old code stored it: a restricted prefix kept raw
    # and hashed raw.
    con.execute("UPDATE bronze.members SET zip_code = '03601' WHERE member_id = ?", [member_id])
    con.execute("""
        UPDATE silver.member_history
        SET zip3 = '036',
            attr_hash = md5(concat_ws('|', birth_year, gender, '036', state, plan_code))
        WHERE member_token = ?""", [token])
    versions = con.execute("SELECT count(*) FROM silver.member_history").fetchall()[0][0]

    assert migrate_member_generalization(con) == 1
    assert migrate_member_generalization(con) == 0
    assert con.execute("SELECT zip3 FROM silver.member_history WHERE member_token = ?",
                       [token]).fetchall()[0][0] == "000"

    # Replay an unchanged update for that member: it must not open a new version.
    con.execute("""
        INSERT INTO bronze.members
        SELECT * REPLACE ('U' AS _op, (SELECT max(_seq) + 1 FROM bronze.members) AS _seq,
                          'replay.jsonl' AS _source_file)
        FROM bronze.members WHERE member_id = ? ORDER BY _seq DESC LIMIT 1""", [member_id])
    build_silver(con, SALT)
    assert con.execute("SELECT count(*) FROM silver.member_history").fetchall()[0][0] == versions


def _claim_status(con, claim_id):
    return con.execute("SELECT claim_status, _seq FROM silver.claims WHERE claim_id = ?",
                       [claim_id]).fetchall()[0]


def _add_claim_event(con, claim_id, status, seq, source_file):
    con.execute("""
        INSERT INTO bronze.claims
        SELECT * REPLACE ('U' AS _op, ? AS claim_status, ? AS _seq, ? AS _source_file)
        FROM bronze.claims WHERE claim_id = ? ORDER BY _seq DESC LIMIT 1""",
                [status, seq, source_file, claim_id])


def test_latest_seq_wins_even_when_files_arrive_out_of_order(con, source, landing):
    land(landing, 0, source.snapshot(10, 3, 30))
    _run(con, landing)
    top = con.execute("SELECT max(_seq) FROM bronze.claims").fetchall()[0][0]
    # The newer event sits in the file that sorts first.
    _add_claim_event(con, "C00000001", "DENIED", top + 2, "batch_9998.jsonl")
    _add_claim_event(con, "C00000001", "PENDED", top + 1, "batch_9999.jsonl")
    build_silver(con, SALT)
    assert _claim_status(con, "C00000001") == ("DENIED", top + 2)


def test_replayed_old_event_does_not_overwrite_newer_state(con, source, landing):
    land(landing, 0, source.snapshot(10, 3, 30))
    _run(con, landing)
    before = _claim_status(con, "C00000001")
    _add_claim_event(con, "C00000001", "DENIED", before[1], "replay.jsonl")
    results = {r.entity: r.changes for r in build_silver(con, SALT)}
    assert results["claims"] == 0
    assert _claim_status(con, "C00000001") == before
