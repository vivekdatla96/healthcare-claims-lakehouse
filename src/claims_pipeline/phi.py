"""PHI handling: deterministic tokenization and generalization.

Direct identifiers are replaced before data reaches silver:
  * member_id      -> member_token  (salted SHA-256, stable across loads so joins still work)
  * date_of_birth  -> birth_year
  * zip_code       -> zip3          (HIPAA Safe Harbor style generalization)
  * names, email, phone             dropped

The salt is a secret. Rotating it re-keys every token, so treat it like an
encryption key and keep it in a secrets manager.
"""

from __future__ import annotations

import hashlib


def tokenize(value: str | None, salt: str) -> str | None:
    """Python reference implementation; must match `token_sql`."""
    if value is None:
        return None
    return hashlib.sha256(f"{salt}|{value}".encode()).hexdigest()


def token_sql(column: str, salt_param: str = "$salt") -> str:
    """SQL expression equivalent to `tokenize` for use inside DuckDB queries."""
    return f"CASE WHEN {column} IS NULL THEN NULL ELSE sha256({salt_param} || '|' || {column}) END"


def zip3_sql(column: str) -> str:
    return f"left({column}, 3)"


def birth_year_sql(column: str) -> str:
    return f"year({column})"
