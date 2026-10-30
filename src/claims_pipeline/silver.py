"""Silver layer: apply CDC from bronze incrementally.

* providers, claims : current-state tables maintained with MERGE (upsert + hard delete)
* members           : SCD Type 2 history (`member_history`) so claims can be joined to
                      the member's plan and state as of the service date

Each entity keeps a high-water mark (`audit.watermarks.last_seq`). A run only reads
bronze rows above it, dedupes them to the latest change per key, and applies them.
Out-of-order events are guarded by comparing `_seq` against the target row.

PHI is tokenized on the way in, so no direct identifier is ever written to silver.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import duckdb

from .phi import birth_year_sql, token_sql, zip3_sql

log = logging.getLogger(__name__)

SALT_SQL = "getvariable('salt')"


@dataclass
class SilverResult:
    entity: str
    changes: int
    watermark: int


def ensure_silver_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.watermarks (
            entity VARCHAR PRIMARY KEY, last_seq BIGINT NOT NULL, updated_at TIMESTAMP)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.providers (
            npi VARCHAR PRIMARY KEY, provider_name VARCHAR, specialty VARCHAR, state VARCHAR,
            in_network BOOLEAN, _seq BIGINT, _updated_at TIMESTAMP)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.claims (
            claim_id VARCHAR PRIMARY KEY, member_token VARCHAR, provider_npi VARCHAR,
            service_date DATE, icd10_code VARCHAR, cpt_code VARCHAR,
            billed_amount DECIMAL(12,2), allowed_amount DECIMAL(12,2), paid_amount DECIMAL(12,2),
            claim_status VARCHAR, _seq BIGINT, _updated_at TIMESTAMP)""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.member_history (
            member_token VARCHAR NOT NULL, birth_year INTEGER, gender VARCHAR, zip3 VARCHAR,
            state VARCHAR, plan_code VARCHAR, attr_hash VARCHAR NOT NULL,
            valid_from TIMESTAMP NOT NULL, valid_to TIMESTAMP, is_current BOOLEAN NOT NULL,
            _seq BIGINT)""")


def _watermark(con: duckdb.DuckDBPyConnection, entity: str) -> int:
    row = con.execute("SELECT last_seq FROM audit.watermarks WHERE entity = ?", [entity]).fetchall()
    return row[0][0] if row else 0


def _set_watermark(con: duckdb.DuckDBPyConnection, entity: str, seq: int) -> None:
    con.execute("""
        INSERT INTO audit.watermarks VALUES (?, ?, current_timestamp)
        ON CONFLICT (entity) DO UPDATE SET last_seq = excluded.last_seq,
                                           updated_at = excluded.updated_at""", [entity, seq])


def _stage_latest(con: duckdb.DuckDBPyConnection, entity: str, key: str, select_sql: str,
                  since: int, until: int | None = None) -> tuple[int, int]:
    """Create temp table `stg_<entity>` with the latest change per key in (since, until]."""
    upper = f"AND _seq <= {int(until)}" if until is not None else ""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE stg_{entity} AS
        SELECT {select_sql}
        FROM bronze.{entity}
        WHERE _seq > {int(since)} {upper}
        QUALIFY row_number() OVER (PARTITION BY {key} ORDER BY _seq DESC) = 1""")
    n, max_seq = con.execute(
        f"SELECT count(*), coalesce(max(_seq), {int(since)}) FROM stg_{entity}").fetchall()[0]
    return n, max_seq


def apply_providers(con: duckdb.DuckDBPyConnection) -> SilverResult:
    since = _watermark(con, "providers")
    n, max_seq = _stage_latest(con, "providers", "npi", "*", since)
    if n:
        con.execute("""
            MERGE INTO silver.providers AS t
            USING stg_providers AS s ON t.npi = s.npi
            WHEN MATCHED AND s._op = 'D' AND s._seq > t._seq THEN DELETE
            WHEN MATCHED AND s._seq > t._seq THEN UPDATE SET
                provider_name = s.provider_name, specialty = s.specialty, state = s.state,
                in_network = s.in_network, _seq = s._seq, _updated_at = s._ts
            WHEN NOT MATCHED AND s._op <> 'D' THEN INSERT VALUES
                (s.npi, s.provider_name, s.specialty, s.state, s.in_network, s._seq, s._ts)""")
    _set_watermark(con, "providers", max_seq)
    return SilverResult("providers", n, max_seq)


def apply_claims(con: duckdb.DuckDBPyConnection, salt: str) -> SilverResult:
    since = _watermark(con, "claims")
    con.execute("SET VARIABLE salt = ?", [salt])
    member_token = token_sql("member_id", SALT_SQL)
    select_sql = f"* EXCLUDE (member_id), {member_token} AS member_token"
    n, max_seq = _stage_latest(con, "claims", "claim_id", select_sql, since)
    if n:
        con.execute("""
            MERGE INTO silver.claims AS t
            USING stg_claims AS s ON t.claim_id = s.claim_id
            WHEN MATCHED AND s._op = 'D' AND s._seq > t._seq THEN DELETE
            WHEN MATCHED AND s._seq > t._seq THEN UPDATE SET
                member_token = s.member_token, provider_npi = s.provider_npi,
                service_date = s.service_date, icd10_code = s.icd10_code, cpt_code = s.cpt_code,
                billed_amount = s.billed_amount, allowed_amount = s.allowed_amount,
                paid_amount = s.paid_amount, claim_status = s.claim_status,
                _seq = s._seq, _updated_at = s._ts
            WHEN NOT MATCHED AND s._op <> 'D' THEN INSERT VALUES
                (s.claim_id, s.member_token, s.provider_npi, s.service_date, s.icd10_code,
                 s.cpt_code, s.billed_amount, s.allowed_amount, s.paid_amount, s.claim_status,
                 s._seq, s._ts)""")
    _set_watermark(con, "claims", max_seq)
    return SilverResult("claims", n, max_seq)


def _pending_batch_bounds(con: duckdb.DuckDBPyConnection, entity: str, since: int) -> list[int]:
    """Upper `_seq` bound of each unprocessed source file, oldest first."""
    rows = con.execute(f"""
        SELECT max(_seq) FROM bronze.{entity} WHERE _seq > ?
        GROUP BY _source_file ORDER BY 1""", [since]).fetchall()
    return [r[0] for r in rows]


def apply_members_scd2(con: duckdb.DuckDBPyConnection, salt: str) -> SilverResult:
    """Apply member changes one source batch at a time so every batch-level
    version is kept, even when several batches land between runs."""
    since = _watermark(con, "members")
    con.execute("SET VARIABLE salt = ?", [salt])
    select_sql = f"""
        {token_sql('member_id', SALT_SQL)} AS member_token,
        {birth_year_sql('date_of_birth')} AS birth_year,
        gender, {zip3_sql('zip_code')} AS zip3, state, plan_code, _op, _seq, _ts,
        md5(concat_ws('|', year(date_of_birth), gender, left(zip_code, 3), state, plan_code))
            AS attr_hash"""
    total = 0
    for until in _pending_batch_bounds(con, "members", since):
        n, since = _stage_latest(con, "members", "member_id", select_sql, since, until)
        total += n
        # 1. close the current version when the member was deleted or a tracked attribute changed
        con.execute("""
            UPDATE silver.member_history AS h
            SET valid_to = s._ts, is_current = false
            FROM stg_members AS s
            WHERE h.member_token = s.member_token AND h.is_current
              AND s._seq > h._seq AND (s._op = 'D' OR s.attr_hash <> h.attr_hash)""")
        # 2. open a new version for anything that no longer has a matching current row
        con.execute("""
            INSERT INTO silver.member_history
            SELECT s.member_token, s.birth_year, s.gender, s.zip3, s.state, s.plan_code,
                   s.attr_hash, s._ts, NULL, true, s._seq
            FROM stg_members AS s
            WHERE s._op <> 'D'
              AND NOT EXISTS (SELECT 1 FROM silver.member_history h
                              WHERE h.member_token = s.member_token AND h.is_current)""")
    _set_watermark(con, "members", since)
    return SilverResult("members", total, since)


def build_silver(con: duckdb.DuckDBPyConnection, salt: str) -> list[SilverResult]:
    ensure_silver_tables(con)
    results = []
    con.execute("BEGIN")
    try:
        results.append(apply_providers(con))
        results.append(apply_members_scd2(con, salt))
        results.append(apply_claims(con, salt))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    for r in results:
        log.info("silver.%s: applied %s changes (watermark=%s)", r.entity, r.changes, r.watermark)
    return results
