from __future__ import annotations

import argparse
import json
import logging
import platform
import socket
import sys
from importlib import metadata

from src.config import REQUIRED_ENV, get_settings
from src.errors import PipelineStageError, run_stage

log = logging.getLogger("pipeline")


def _show(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def cmd_validate_env(args):
    import os

    s = get_settings()
    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")
    pkgs = {p: metadata.version(p) for p in ("pandas", "pyarrow", "psycopg2-binary", "PyYAML", "python-dotenv")}
    info = {"python": platform.python_version(), "packages": pkgs,
            "postgres_host": os.environ["POSTGRES_HOST"],
            "source_files_present": all((s.source_dir / n).exists()
                                        for n in ("customers.csv", "products.json", "orders.csv"))}
    try:
        socket.create_connection((os.environ["POSTGRES_HOST"], int(os.environ["POSTGRES_PORT"])), timeout=3).close()
        info["postgres_reachable"] = True
    except OSError:
        info["postgres_reachable"] = False
        if args.check_db:
            raise
    _show(info)
    print("Environment validation successful!")


def cmd_extract(args):
    from src.extract.files import extract_sources
    rid = run_stage("extract", lambda: extract_sources(args.run_id), {"run_id": args.run_id})
    print(f"run_id={rid}")


def cmd_transform(args):
    from src.transform.pipeline import run_transform
    _show(run_stage("transform", lambda: run_transform(args.run_id), {"run_id": args.run_id}))


def cmd_run_all(args):
    from src.extract.files import extract_sources
    from src.transform.pipeline import run_transform
    rid = run_stage("extract", lambda: extract_sources(args.run_id), {"run_id": args.run_id})
    _show(run_stage("transform", lambda: run_transform(rid), {"run_id": rid}))


def cmd_load(args):
    from src.load.postgres import load_curated
    _show(run_stage("load", lambda: load_curated(args.run_id), {"run_id": args.run_id}))


def cmd_validate(args):
    from src.validate.quality import run_quality_checks
    _show(run_stage("validate", lambda: run_quality_checks(args.run_id, args.year, args.month),
                    {"run_id": args.run_id}))


def cmd_benchmark(args):
    from src.benchmark.formats import run_benchmark
    _show(run_stage("benchmark", lambda: run_benchmark(args.run_id, args.repeats), {"run_id": args.run_id}))


def cmd_load_partition(args):
    from src.load.postgres import load_partition
    _show(run_stage("load-partition", lambda: load_partition(args.run_id, args.year, args.month),
                    {"run_id": args.run_id, "year": args.year, "month": args.month}))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DSS150P Lab 3 pipeline CLI")
    sub = p.add_subparsers(dest="command", required=True)

    def add(name, fn, run_id=True, **kw):
        sp = sub.add_parser(name, **kw)
        sp.set_defaults(func=fn)
        if run_id:
            sp.add_argument("--run-id", default="latest")
        return sp

    add("validate-env", cmd_validate_env, run_id=False).add_argument("--check-db", action="store_true")
    add("extract", cmd_extract)
    add("transform", cmd_transform)
    add("run-all", cmd_run_all)
    add("load", cmd_load)
    v = add("validate", cmd_validate)
    v.add_argument("--year", type=int)
    v.add_argument("--month", type=int)
    add("benchmark", cmd_benchmark).add_argument("--repeats", type=int, default=5)
    lp = add("load-partition", cmd_load_partition)
    lp.add_argument("--year", type=int, required=True)
    lp.add_argument("--month", type=int, required=True)
    return p


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except PipelineStageError as exc:
        print(f"PIPELINE FAILURE: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - reported with non-zero exit so Airflow marks the task failed
        print(f"PIPELINE FAILURE [validate-env]: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())