"""Central paths and settings. Override with environment variables."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

ENTITIES = ("members", "providers", "claims")
DEV_PHI_SALT = "local-dev-salt-change-me"
PRODUCTION_ENVS = ("prod", "production")

log = logging.getLogger(__name__)


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
        # The dev default is public, so tokens made with it can be reversed by hashing
        # candidate member IDs: refuse it in production and warn everywhere else.
        salt = os.getenv("CLAIMS_PHI_SALT") or DEV_PHI_SALT
        if salt == DEV_PHI_SALT:
            if os.getenv("CLAIMS_ENV", "dev").lower() in PRODUCTION_ENVS:
                raise RuntimeError(
                    "CLAIMS_PHI_SALT must be set to a secret value when CLAIMS_ENV is production")
            log.warning("CLAIMS_PHI_SALT is not set; using the public dev salt. "
                        "Member tokens are not protected.")
        return cls(data_dir=data_dir, warehouse_path=warehouse, phi_salt=salt)
