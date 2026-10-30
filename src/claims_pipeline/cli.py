"""Command line entry point.

    python -m claims_pipeline generate --batches 3
    python -m claims_pipeline ingest
    python -m claims_pipeline silver
    python -m claims_pipeline quality
    python -m claims_pipeline run          # ingest -> silver -> quality
"""

from __future__ import annotations

import argparse
import logging
import sys

from . import generate as gen
from .config import Settings
from .ingest import ingest_landing
from .quality import DataQualityError, run_checks
from .silver import build_silver
from .warehouse import connect


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="claims", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write synthetic CDC batches to the landing zone")
    g.add_argument("--batches", type=int, default=3)
    g.add_argument("--members", type=int, default=200)
    g.add_argument("--providers", type=int, default=25)
    g.add_argument("--claims", type=int, default=1000)
    g.add_argument("--seed", type=int, default=42)
    for name in ("ingest", "silver", "quality", "run"):
        sub.add_parser(name)
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parser().parse_args(argv)
    settings = Settings.from_env()

    if args.cmd == "generate":
        files = gen.generate(settings.landing_dir, args.batches, args.members, args.providers,
                             args.claims, args.seed)
        print(f"wrote {len(files)} files to {settings.landing_dir}")
        return 0

    con = connect(settings.warehouse_path)
    try:
        if args.cmd in ("ingest", "run"):
            loaded = [r for r in ingest_landing(con, settings.landing_dir) if not r.skipped]
            print(f"bronze: {len(loaded)} new files, {sum(r.rows for r in loaded)} rows")
        if args.cmd in ("silver", "run"):
            for r in build_silver(con, settings.phi_salt):
                print(f"silver.{r.entity}: {r.changes} changes applied")
        if args.cmd in ("quality", "run"):
            results = run_checks(con)
            print(f"quality: {sum(r['passed'] for r in results)}/{len(results)} checks passed")
    except DataQualityError as exc:
        print(f"quality gate failed: {exc}", file=sys.stderr)
        return 2
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
