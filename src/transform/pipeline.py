from __future__ import annotations

from src.config import get_settings, resolve_run_id
from src.transform.curated import build_curated
from src.transform.quarantine import combine_quarantine
from src.transform.staging import build_staging


def run_transform(run_id: str | None) -> dict:
    s = get_settings()
    rid = resolve_run_id(run_id, s.raw_dir)
    stg = build_staging(rid)
    curated, q_curated = build_curated(rid)
    quarantine = combine_quarantine([stg["quarantine"], q_curated])
    qdir = s.quarantine_dir / f"run_id={rid}"
    qdir.mkdir(parents=True, exist_ok=True)
    quarantine.to_parquet(qdir / "quarantine.parquet", index=False)
    by_reason = quarantine.groupby(["source", "reason"]).size().to_dict() if len(quarantine) else {}
    return {
        "run_id": rid,
        "raw": stg["raw_counts"],
        "staging": {k: len(stg[k]) for k in ("customers", "products", "orders")},
        "curated": len(curated),
        "quarantine_total": len(quarantine),
        "quarantine_by_reason": {f"{a}:{b}": n for (a, b), n in by_reason.items()},
    }