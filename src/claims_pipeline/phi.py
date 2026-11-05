"""PHI handling: deterministic tokenization and generalization.

Direct identifiers are replaced before data reaches silver:
  * member_id      -> member_token  (salted SHA-256, stable across loads so joins still work)
  * date_of_birth  -> birth_year
  * zip_code       -> zip3          (HIPAA Safe Harbor: sparse areas become '000')
  * names, email, phone             dropped

Ages over 89 are a Safe Harbor identifier too; the gold models suppress
birth_year for those members (see the dbt macro `safe_birth_year`).

The salt is a secret. Rotating it re-keys every token, so treat it like an
encryption key and keep it in a secrets manager.
"""

from __future__ import annotations

import hashlib

# 3-digit ZIP prefixes whose combined population is 20,000 or fewer, per the
# HHS de-identification guidance (2000 Census). Safe Harbor requires these to
# be reported as '000' instead of the real prefix.
RESTRICTED_ZIP3 = frozenset({
    "036", "059", "063", "102", "203", "556", "692", "790", "821",
    "823", "830", "831", "878", "879", "884", "890", "893",
})


def tokenize(value: str | None, salt: str) -> str | None:
    """Python reference implementation; must match `token_sql`."""
    if value is None:
        return None
    return hashlib.sha256(f"{salt}|{value}".encode()).hexdigest()


def token_sql(column: str, salt_param: str = "$salt") -> str:
    """SQL expression equivalent to `tokenize` for use inside DuckDB queries."""
    return f"CASE WHEN {column} IS NULL THEN NULL ELSE sha256({salt_param} || '|' || {column}) END"


def generalize_zip3(zip_code: str | None) -> str | None:
    """Python reference implementation; must match `zip3_sql`."""
    if zip_code is None:
        return None
    prefix = zip_code[:3]
    return "000" if prefix in RESTRICTED_ZIP3 else prefix


def zip3_sql(column: str) -> str:
    """SQL expression equivalent to `generalize_zip3`."""
    restricted = ", ".join(f"'{z}'" for z in sorted(RESTRICTED_ZIP3))
    return f"CASE WHEN left({column}, 3) IN ({restricted}) THEN '000' ELSE left({column}, 3) END"


def birth_year_sql(column: str) -> str:
    return f"year({column})"
