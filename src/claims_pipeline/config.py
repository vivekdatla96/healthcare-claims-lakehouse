"""Central paths and settings. Override with environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ENTITIES = ("members", "providers", "claims")


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    warehouse_path: Path
    phi_salt: str

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @classmethod
    def from_env(cls) -> Settings:
        data_dir = Path(os.getenv("CLAIMS_DATA_DIR", "data")).resolve()
        warehouse = Path(os.getenv("CLAIMS_WAREHOUSE", data_dir / "warehouse.duckdb")).resolve()
        # In production this comes from a secrets manager (Key Vault, Secrets Manager).
        salt = os.getenv("CLAIMS_PHI_SALT", "local-dev-salt-change-me")
        return cls(data_dir=data_dir, warehouse_path=warehouse, phi_salt=salt)
