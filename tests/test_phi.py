from claims_pipeline.phi import token_sql, tokenize


def test_tokenize_is_deterministic_and_salted():
    assert tokenize("M000001", "a") == tokenize("M000001", "a")
    assert tokenize("M000001", "a") != tokenize("M000001", "b")
    assert tokenize(None, "a") is None


def test_sql_token_matches_python(con):
    con.execute("SET VARIABLE salt = 'abc'")
    expr = token_sql("'M000001'", "getvariable('salt')")
    got = con.execute(f"SELECT {expr}").fetchall()[0][0]
    assert got == tokenize("M000001", "abc")
