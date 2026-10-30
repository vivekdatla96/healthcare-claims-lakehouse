from pathlib import Path

import pytest

from claims_pipeline.generate import SourceSystem, write_batch
from claims_pipeline.warehouse import connect

SALT = "test-salt"


@pytest.fixture
def con(tmp_path: Path):
    c = connect(tmp_path / "wh.duckdb")
    yield c
    c.close()


@pytest.fixture
def source():
    return SourceSystem(seed=7)


@pytest.fixture
def landing(tmp_path: Path) -> Path:
    return tmp_path / "landing"


def land(landing: Path, batch_no: int, batch: dict) -> None:
    write_batch(landing, batch_no, batch)
