from pathlib import Path

from claims_pipeline.generate import SourceSystem, generate


def _contents(root: Path) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): p.read_text(encoding="utf-8")
            for p in sorted(root.rglob("*.jsonl"))}


def test_same_seed_writes_identical_batches(tmp_path):
    generate(tmp_path / "a", batches=2, members=20, providers=4, claims=50, seed=1)
    generate(tmp_path / "b", batches=2, members=20, providers=4, claims=50, seed=1)
    assert _contents(tmp_path / "a") == _contents(tmp_path / "b")


def test_different_seed_writes_different_data(tmp_path):
    generate(tmp_path / "a", batches=1, members=20, providers=4, claims=50, seed=1)
    generate(tmp_path / "b", batches=1, members=20, providers=4, claims=50, seed=2)
    assert _contents(tmp_path / "a") != _contents(tmp_path / "b")


def test_snapshot_is_all_inserts_with_unique_increasing_seq():
    batch = SourceSystem(seed=3).snapshot(10, 2, 30)
    assert {k: len(v) for k, v in batch.items()} == {"members": 10, "providers": 2, "claims": 30}
    rows = [r for entity in ("members", "providers", "claims") for r in batch[entity]]
    assert all(r["_op"] == "I" for r in rows)
    seqs = [r["_seq"] for r in rows]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


def test_incremental_continues_the_sequence_and_keeps_payment_logic():
    src = SourceSystem(seed=3)
    snapshot = src.snapshot(40, 5, 100)
    last_seq = max(r["_seq"] for rows in snapshot.values() for r in rows)
    batch = src.incremental(10)
    assert min(r["_seq"] for rows in batch.values() for r in rows) > last_seq
    paid = [c for c in batch["claims"] if c.get("claim_status") == "PAID"]
    assert paid
    assert all(c["paid_amount"] <= c["allowed_amount"] <= c["billed_amount"] for c in paid)
