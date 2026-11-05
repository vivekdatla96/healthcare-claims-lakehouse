from claims_pipeline.cli import main
from claims_pipeline.config import Settings
from claims_pipeline.warehouse import connect


def test_generate_then_run_end_to_end_and_rerun_is_a_noop(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("CLAIMS_WAREHOUSE", raising=False)
    monkeypatch.setenv("CLAIMS_PHI_SALT", "test-salt")

    assert main(["generate", "--batches", "2", "--members", "20", "--providers", "4",
                 "--claims", "50"]) == 0
    assert main(["run"]) == 0
    first = capsys.readouterr().out
    assert "bronze: 5 new files" in first
    assert "quality: " in first and "checks passed" in first

    assert main(["run"]) == 0
    second = capsys.readouterr().out
    assert "bronze: 0 new files, 0 rows" in second
    assert "silver.claims: 0 changes applied" in second


def test_quality_failure_returns_exit_code_2(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CLAIMS_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("CLAIMS_WAREHOUSE", raising=False)
    monkeypatch.setenv("CLAIMS_PHI_SALT", "test-salt")
    main(["generate", "--batches", "1", "--members", "10", "--providers", "3", "--claims", "20"])
    main(["run"])

    con = connect(Settings.from_env().warehouse_path)
    con.execute("UPDATE silver.claims SET claim_status = 'BOGUS' WHERE claim_id = 'C00000001'")
    con.close()

    assert main(["quality"]) == 2
    assert "claims_valid_status" in capsys.readouterr().err
