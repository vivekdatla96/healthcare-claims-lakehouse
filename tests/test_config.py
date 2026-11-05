import logging

import pytest

from claims_pipeline.config import DEV_PHI_SALT, Settings


def test_salt_from_environment(monkeypatch):
    monkeypatch.setenv("CLAIMS_PHI_SALT", "from-vault")
    assert Settings.from_env().phi_salt == "from-vault"


def test_missing_salt_warns_outside_production(monkeypatch, caplog):
    monkeypatch.delenv("CLAIMS_PHI_SALT", raising=False)
    monkeypatch.delenv("CLAIMS_ENV", raising=False)
    with caplog.at_level(logging.WARNING, logger="claims_pipeline.config"):
        assert Settings.from_env().phi_salt == DEV_PHI_SALT
    assert "CLAIMS_PHI_SALT is not set" in caplog.text


@pytest.mark.parametrize("env", ["prod", "Production"])
def test_missing_salt_is_refused_in_production(monkeypatch, env):
    monkeypatch.delenv("CLAIMS_PHI_SALT", raising=False)
    monkeypatch.setenv("CLAIMS_ENV", env)
    with pytest.raises(RuntimeError, match="CLAIMS_PHI_SALT must be set"):
        Settings.from_env()


def test_public_dev_salt_is_refused_in_production(monkeypatch):
    monkeypatch.setenv("CLAIMS_PHI_SALT", DEV_PHI_SALT)
    monkeypatch.setenv("CLAIMS_ENV", "prod")
    with pytest.raises(RuntimeError, match="CLAIMS_PHI_SALT must be set"):
        Settings.from_env()
