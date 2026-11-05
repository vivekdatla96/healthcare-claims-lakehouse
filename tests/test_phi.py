import pytest

from claims_pipeline.phi import RESTRICTED_ZIP3, generalize_zip3, token_sql, tokenize, zip3_sql


def test_tokenize_is_deterministic_and_salted():
    assert tokenize("M000001", "a") == tokenize("M000001", "a")
    assert tokenize("M000001", "a") != tokenize("M000001", "b")
    assert tokenize(None, "a") is None


def test_sql_token_matches_python(con):
    con.execute("SET VARIABLE salt = 'abc'")
    expr = token_sql("'M000001'", "getvariable('salt')")
    got = con.execute(f"SELECT {expr}").fetchall()[0][0]
    assert got == tokenize("M000001", "abc")


def test_zip3_keeps_populous_prefixes_and_masks_sparse_ones():
    assert generalize_zip3("10001") == "100"
    assert generalize_zip3("03601") == "000"
    assert generalize_zip3("89301") == "000"
    assert generalize_zip3(None) is None
    assert len(RESTRICTED_ZIP3) == 17


@pytest.mark.parametrize("zip_code", ["10001", "03601", "82301", "99999", None])
def test_sql_zip3_matches_python(con, zip_code):
    got = con.execute(f"SELECT {zip3_sql('z')} FROM (SELECT ?::VARCHAR AS z)", [zip_code]).fetchall()[0][0]
    assert got == generalize_zip3(zip_code)
