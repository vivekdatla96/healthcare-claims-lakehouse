"""Synthetic source system that emits CDC batches as JSON Lines.

Batch 0 is a full snapshot (all inserts). Later batches contain the kind of
changes a claims platform produces day to day: claims moving through
adjudication, voided claims, members changing address, new members and claims.

Every record carries CDC metadata:
    _op   : I (insert), U (update) or D (delete)
    _seq  : monotonically increasing change sequence number
    _ts   : source commit timestamp (ISO 8601)

All data is fake. No real PHI is used anywhere in this project.
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

FIRST_NAMES = ["Ava", "Liam", "Mia", "Noah", "Zoe", "Ethan", "Aria", "Lucas", "Isla", "Ravi",
               "Priya", "Arjun", "Sofia", "Mateo", "Chloe", "Omar", "Leah", "Kenji", "Nina", "Sam"]
LAST_NAMES = ["Patel", "Smith", "Garcia", "Chen", "Johnson", "Khan", "Rossi", "Kim", "Nguyen",
              "Brown", "Lopez", "Reddy", "Murphy", "Silva", "Cohen", "Ali", "Rao", "Davis"]
STATES = ["NJ", "NY", "PA", "CT", "MA", "CA", "TX", "FL", "IL", "OH"]
SPECIALTIES = ["Cardiology", "Primary Care", "Orthopedics", "Dermatology", "Oncology",
               "Pediatrics", "Radiology", "Endocrinology"]
ICD10 = ["E11.9", "I10", "J45.909", "M54.5", "K21.9", "F41.1", "E78.5", "N39.0", "R51.9", "Z00.00"]
CPT = {"99213": 120.0, "99214": 180.0, "93000": 60.0, "71046": 95.0, "80053": 45.0,
       "20610": 210.0, "11102": 150.0, "96413": 640.0}
PLANS = ["COMMERCIAL_PPO", "COMMERCIAL_HMO", "MEDICARE_ADV", "MEDICAID"]


class SourceSystem:
    def __init__(self, seed: int = 42, start: datetime | None = None) -> None:
        self.rng = random.Random(seed)
        self.seq = 0
        self.clock = start or datetime(2026, 1, 1, 6, 0, 0)
        self.members: dict[str, dict] = {}
        self.providers: dict[str, dict] = {}
        self.claims: dict[str, dict] = {}

    # ---- helpers -------------------------------------------------------
    def _tick(self) -> tuple[int, str]:
        self.seq += 1
        self.clock += timedelta(seconds=self.rng.randint(1, 90))
        return self.seq, self.clock.isoformat()

    def _cdc(self, op: str, row: dict) -> dict:
        seq, ts = self._tick()
        return {**row, "_op": op, "_seq": seq, "_ts": ts}

    def _new_member(self) -> dict:
        n = len(self.members) + 1
        dob = date(1940, 1, 1) + timedelta(days=self.rng.randint(0, 365 * 80))
        first, last = self.rng.choice(FIRST_NAMES), self.rng.choice(LAST_NAMES)
        member = {
            "member_id": f"M{n:06d}",
            "first_name": first,
            "last_name": last,
            "date_of_birth": dob.isoformat(),
            "gender": self.rng.choice(["F", "M", "U"]),
            "email": f"{first.lower()}.{last.lower()}{n}@example.com",
            "phone": f"555-{self.rng.randint(100, 999)}-{self.rng.randint(1000, 9999)}",
            "zip_code": f"{self.rng.randint(1000, 99999):05d}",
            "state": self.rng.choice(STATES),
            "plan_code": self.rng.choice(PLANS),
        }
        self.members[member["member_id"]] = member
        return member

    def _new_provider(self) -> dict:
        n = len(self.providers) + 1
        provider = {
            "npi": f"{1000000000 + n * 7919}",
            "provider_name": f"Dr. {self.rng.choice(FIRST_NAMES)} {self.rng.choice(LAST_NAMES)}",
            "specialty": self.rng.choice(SPECIALTIES),
            "state": self.rng.choice(STATES),
            "in_network": self.rng.random() < 0.8,
        }
        self.providers[provider["npi"]] = provider
        return provider

    def _new_claim(self) -> dict:
        n = len(self.claims) + 1
        cpt = self.rng.choice(list(CPT))
        billed = round(CPT[cpt] * self.rng.uniform(0.9, 2.5), 2)
        claim = {
            "claim_id": f"C{n:08d}",
            "member_id": self.rng.choice(list(self.members)),
            "provider_npi": self.rng.choice(list(self.providers)),
            "service_date": (self.clock.date() - timedelta(days=self.rng.randint(1, 60))).isoformat(),
            "icd10_code": self.rng.choice(ICD10),
            "cpt_code": cpt,
            "billed_amount": billed,
            "allowed_amount": None,
            "paid_amount": None,
            "claim_status": "SUBMITTED",
        }
        self.claims[claim["claim_id"]] = claim
        return claim

    # ---- batches -------------------------------------------------------
    def snapshot(self, n_members: int, n_providers: int, n_claims: int) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {"members": [], "providers": [], "claims": []}
        for _ in range(n_members):
            out["members"].append(self._cdc("I", self._new_member()))
        for _ in range(n_providers):
            out["providers"].append(self._cdc("I", self._new_provider()))
        for _ in range(n_claims):
            out["claims"].append(self._cdc("I", self._new_claim()))
        return out

    def incremental(self, n_new_claims: int) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {"members": [], "providers": [], "claims": []}
        self.clock += timedelta(days=1)

        # adjudicate a share of open claims
        open_claims = [c for c in self.claims.values() if c["claim_status"] in ("SUBMITTED", "PENDED")]
        for claim in self.rng.sample(open_claims, k=int(len(open_claims) * 0.6)):
            roll = self.rng.random()
            if roll < 0.75:
                allowed = round(claim["billed_amount"] * self.rng.uniform(0.4, 0.8), 2)
                claim.update(claim_status="PAID", allowed_amount=allowed,
                             paid_amount=round(allowed * self.rng.uniform(0.7, 1.0), 2))
            elif roll < 0.9:
                claim.update(claim_status="DENIED", allowed_amount=0.0, paid_amount=0.0)
            else:
                claim.update(claim_status="PENDED")
            out["claims"].append(self._cdc("U", dict(claim)))

        # a few voided claims arrive as hard deletes
        voidable = [c for c in self.claims.values() if c["claim_status"] == "SUBMITTED"]
        for claim in self.rng.sample(voidable, k=min(2, len(voidable))):
            del self.claims[claim["claim_id"]]
            out["claims"].append(self._cdc("D", {"claim_id": claim["claim_id"]}))

        # members move or change plans (drives SCD Type 2 history downstream)
        for member in self.rng.sample(list(self.members.values()), k=max(1, len(self.members) // 20)):
            member.update(state=self.rng.choice(STATES), plan_code=self.rng.choice(PLANS),
                          zip_code=f"{self.rng.randint(1000, 99999):05d}")
            out["members"].append(self._cdc("U", dict(member)))

        for _ in range(max(1, len(self.members) // 50)):
            out["members"].append(self._cdc("I", self._new_member()))
        for _ in range(n_new_claims):
            out["claims"].append(self._cdc("I", self._new_claim()))
        return out


def write_batch(landing_dir: Path, batch_no: int, batch: dict[str, list[dict]]) -> list[Path]:
    written = []
    for entity, rows in batch.items():
        if not rows:
            continue
        folder = landing_dir / entity
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"batch_{batch_no:04d}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        written.append(path)
    return written


def generate(landing_dir: Path, batches: int = 3, members: int = 200, providers: int = 25,
             claims: int = 1000, seed: int = 42) -> list[Path]:
    """Write one snapshot batch plus `batches - 1` incremental CDC batches."""
    src = SourceSystem(seed=seed)
    files = write_batch(landing_dir, 0, src.snapshot(members, providers, claims))
    for b in range(1, batches):
        files += write_batch(landing_dir, b, src.incremental(n_new_claims=claims // 10))
    return files
