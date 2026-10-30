"""Explicit source contracts. Bronze tables are typed from these, never inferred,
so a malformed upstream file fails loudly instead of silently changing a schema."""

from __future__ import annotations

CDC_COLUMNS = {"_op": "VARCHAR", "_seq": "BIGINT", "_ts": "TIMESTAMP"}

SOURCE_SCHEMAS: dict[str, dict[str, str]] = {
    "members": {
        "member_id": "VARCHAR",
        "first_name": "VARCHAR",
        "last_name": "VARCHAR",
        "date_of_birth": "DATE",
        "gender": "VARCHAR",
        "email": "VARCHAR",
        "phone": "VARCHAR",
        "zip_code": "VARCHAR",
        "state": "VARCHAR",
        "plan_code": "VARCHAR",
    },
    "providers": {
        "npi": "VARCHAR",
        "provider_name": "VARCHAR",
        "specialty": "VARCHAR",
        "state": "VARCHAR",
        "in_network": "BOOLEAN",
    },
    "claims": {
        "claim_id": "VARCHAR",
        "member_id": "VARCHAR",
        "provider_npi": "VARCHAR",
        "service_date": "DATE",
        "icd10_code": "VARCHAR",
        "cpt_code": "VARCHAR",
        "billed_amount": "DECIMAL(12,2)",
        "allowed_amount": "DECIMAL(12,2)",
        "paid_amount": "DECIMAL(12,2)",
        "claim_status": "VARCHAR",
    },
}

PRIMARY_KEYS = {"members": "member_id", "providers": "npi", "claims": "claim_id"}

# Columns treated as direct identifiers. They never leave bronze.
PHI_COLUMNS = {"members": ["first_name", "last_name", "email", "phone", "date_of_birth", "zip_code"]}


def full_schema(entity: str) -> dict[str, str]:
    return {**SOURCE_SCHEMAS[entity], **CDC_COLUMNS}
