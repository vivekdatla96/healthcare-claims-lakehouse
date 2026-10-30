"""DuckDB connection and schema bootstrap."""

from __future__ import annotations

from pathlib import Path

import duckdb

LAYERS = ("bronze", "silver", "gold", "audit")


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    for layer in LAYERS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")
    return con
