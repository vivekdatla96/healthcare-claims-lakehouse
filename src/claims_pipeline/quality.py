"""Data quality gate between silver and gold.

Each check is a SQL query that returns the offending rows' count. Results are
written to `audit.dq_results` for trend reporting. Any failing `error` check
raises, which stops the DAG before bad data reaches gold.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

import duckdb

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str  # must return a single integer: number of failing rows
    severity: str = "error"  # error | warn


CHECKS: list[Check] = [
    Check("claims_pk_not_null", "SELECT count(*) FROM silver.claims WHERE claim_id IS NULL"),
    Check("claims_member_resolves", """
        SELECT count(*) FROM silver.claims c
        LEFT JOIN silver.member_history m ON m.member_token = c.member_token
        WHERE m.member_token IS NULL"""),
    Check("claims_provider_resolves", """
        SELECT count(*) FROM silver.claims c
        LEFT JOIN silver.providers p ON p.npi = c.provider_npi
        WHERE p.npi IS NULL"""),
    Check("claims_paid_le_allowed_le_billed", """
        SELECT count(*) FROM silver.claims
        WHERE paid_amount > allowed_amount OR allowed_amount > billed_amount"""),
    Check("claims_paid_has_amounts", """
        SELECT count(*) FROM silver.claims
        WHERE claim_status = 'PAID' AND (paid_amount IS NULL OR allowed_amount IS NULL)"""),
    Check("claims_valid_status", """
        SELECT count(*) FROM silver.claims
        WHERE claim_status NOT IN ('SUBMITTED', 'PENDED', 'PAID', 'DENIED')"""),
    Check("claims_service_date_not_future",
          "SELECT count(*) FROM silver.claims WHERE service_date > current_date"),
    Check("members_one_current_version", """
        SELECT count(*) FROM (
            SELECT member_token FROM silver.member_history WHERE is_current
            GROUP BY 1 HAVING count(*) > 1)"""),
    Check("members_no_overlapping_versions", """
        SELECT count(*) FROM silver.member_history a
        JOIN silver.member_history b
          ON a.member_token = b.member_token AND a.valid_from < b.valid_from
         AND coalesce(a.valid_to, TIMESTAMP '9999-12-31') > b.valid_from"""),
    Check("providers_npi_10_digits",
          "SELECT count(*) FROM silver.providers WHERE NOT regexp_full_match(npi, '[0-9]{10}')"),
    Check("silver_has_no_raw_phi", """
        SELECT count(*) FROM information_schema.columns
        WHERE table_schema = 'silver'
          AND column_name IN ('member_id', 'first_name', 'last_name', 'email', 'phone',
                              'date_of_birth', 'zip_code')"""),
    Check("claims_denial_rate_under_40pct", """
        SELECT CASE WHEN count(*) FILTER (WHERE claim_status = 'DENIED')
                         > 0.4 * nullif(count(*) FILTER (WHERE claim_status IN ('PAID','DENIED')), 0)
               THEN 1 ELSE 0 END
        FROM silver.claims""", severity="warn"),
]


class DataQualityError(RuntimeError):
    pass


def run_checks(con: duckdb.DuckDBPyConnection, checks: list[Check] = CHECKS) -> list[dict]:
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.dq_results (
            run_id VARCHAR, check_name VARCHAR, severity VARCHAR,
            failing_rows BIGINT, passed BOOLEAN, checked_at TIMESTAMP DEFAULT current_timestamp)""")
    run_id = uuid.uuid4().hex[:12]
    results = []
    for check in checks:
        failing = con.execute(check.sql).fetchall()[0][0]
        passed = failing == 0
        results.append({"check": check.name, "severity": check.severity,
                        "failing_rows": failing, "passed": passed})
        con.execute(
            "INSERT INTO audit.dq_results (run_id, check_name, severity, failing_rows, passed) "
            "VALUES (?, ?, ?, ?, ?)", [run_id, check.name, check.severity, failing, passed])
        level = logging.INFO if passed else logging.WARNING
        log.log(level, "dq %-36s %s (%s failing)", check.name, "PASS" if passed else "FAIL", failing)

    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if errors:
        names = ", ".join(r["check"] for r in errors)
        raise DataQualityError(f"{len(errors)} data quality check(s) failed: {names}")
    return results
