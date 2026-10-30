"""Bronze layer: append raw CDC files exactly as received.

Ingestion is idempotent. Each file is fingerprinted (SHA-256) and recorded in
`audit.ingest_log`; a file that was already loaded is skipped, so retries and
backfills never duplicate rows.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .config import ENTITIES
from .schemas import full_schema

log = logging.getLogger(__name__)


@dataclass
class IngestResult:
    entity: str
    file: str
    rows: int
    skipped: bool


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_bronze_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.ingest_log (
            file_hash   VARCHAR PRIMARY KEY,
            entity      VARCHAR NOT NULL,
            file_path   VARCHAR NOT NULL,
            row_count   BIGINT  NOT NULL,
            loaded_at   TIMESTAMP DEFAULT current_timestamp
        )""")
    for entity in ENTITIES:
        cols = ",\n".join(f"{c} {t}" for c, t in full_schema(entity).items())
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze.{entity} (
                {cols},
                _source_file VARCHAR,
                _ingested_at TIMESTAMP
            )""")


def ingest_file(con: duckdb.DuckDBPyConnection, entity: str, path: Path) -> IngestResult:
    file_hash = _sha256(path)
    seen = con.execute("SELECT 1 FROM audit.ingest_log WHERE file_hash = ?", [file_hash]).fetchall()
    if seen:
        log.info("skip %s (already ingested)", path.name)
        return IngestResult(entity, str(path), 0, skipped=True)

    schema = full_schema(entity)
    columns_struct = "{" + ", ".join(f"'{c}': '{t}'" for c, t in schema.items()) + "}"
    col_list = ", ".join(schema)

    con.execute("BEGIN")
    try:
        rows = con.execute(f"""
            INSERT INTO bronze.{entity} ({col_list}, _source_file, _ingested_at)
            SELECT {col_list}, ?, current_timestamp
            FROM read_json(?, format = 'newline_delimited', columns = {columns_struct})
        """, [path.name, str(path)]).fetchall()[0][0]
        con.execute(
            "INSERT INTO audit.ingest_log (file_hash, entity, file_path, row_count) VALUES (?,?,?,?)",
            [file_hash, entity, str(path), rows],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    log.info("loaded %s rows from %s into bronze.%s", rows, path.name, entity)
    return IngestResult(entity, str(path), rows, skipped=False)


def ingest_landing(con: duckdb.DuckDBPyConnection, landing_dir: Path) -> list[IngestResult]:
    ensure_bronze_tables(con)
    results = []
    for entity in ENTITIES:
        for path in sorted((landing_dir / entity).glob("batch_*.jsonl")):
            results.append(ingest_file(con, entity, path))
    return results
