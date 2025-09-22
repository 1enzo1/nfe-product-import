"""Arena/benchmark entrypoint wrapper.

This module exposes a light-weight ``run`` function used by the arena
variants (A–D). It adapts the project ``Settings`` to point to the arena
data/output folders, invokes the core ``run_pipeline`` helper and then
standardises outputs and basic metrics under the variant ``out`` folder.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import asdict
import sys
from pathlib import Path
from typing import Dict, Optional

from ..config import Settings
from .pipeline import run_pipeline, ProcessingResult


def _discover_master_data(data_dir: Path) -> Optional[Path]:
    for ext in (".xlsx", ".xlsm", ".xls"):
        for candidate in sorted(data_dir.glob(f"*{ext}")):
            return candidate
    return None


def _ensure_out(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


def _write_empty_csv(path: Path, *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)


def _save_metrics(out_dir: Path, metrics: Dict[str, object]) -> None:
    metrics_path = out_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")


def run(*, mode: str, data_dir: str, out_path: str, config_path: str = "config.yaml",
        matcher_threshold: Optional[float] = None) -> Dict[str, object]:
    """Run the pipeline for a given variant.

    Parameters
    - mode: Variant identifier (e.g., "A", "B", ...). Used as execution label.
    - data_dir: Folder containing inputs (XML/CSV/XLSX) for the run.
    - out_path: Target CSV path for the consolidated output of this run.
    - config_path: Base YAML configuration to start from.
    - matcher_threshold: Optional override for fuzzy match auto-approval threshold.

    Returns a metrics dictionary and also persists ``metrics.json`` alongside
    the outputs (in ``out_path``'s directory).
    """

    base_settings = Settings.load(config_path)

    data_dir_path = Path(data_dir).expanduser().resolve()
    out_csv_path = Path(out_path).expanduser().resolve()
    out_dir = out_csv_path.parent
    _ensure_out(out_dir)

    # Build a copy of settings with arena-specific paths
    settings = Settings.parse_obj(base_settings.dict())
    settings.paths.nfe_input_folder = data_dir_path
    settings.paths.output_folder = out_dir
    settings.paths.log_folder = out_dir / "logs"
    settings.paths.pendings_folder = out_dir
    # Isolar cache de sinônimos por variante
    settings.paths.synonym_cache_file = out_dir / "synonyms.json"

    discovered_master = _discover_master_data(data_dir_path)
    if discovered_master is not None:
        settings.paths.master_data_file = discovered_master

    # Optional matcher tuning via the configuration hook
    def configure_processor(processor):  # type: ignore[no-redef]
        if matcher_threshold is not None:
            try:
                processor.matcher.auto_threshold = float(matcher_threshold)
            except Exception:
                pass

    # Execute pipeline (directory mode uses settings.paths.nfe_input_folder)
    result: Optional[ProcessingResult] = run_pipeline(
        settings=settings,
        files=None,
        mode=f"arena-{mode}",
        user=os.getenv("ARENA_USER"),
        configure_processor=configure_processor,
    )

    # If nothing was processed (e.g., no XML), create a stub CSV to keep flow
    if result is None:
        _write_empty_csv(out_csv_path, columns=settings.csv_output.columns)
        metrics = {
            "variant": mode,
            "run_id": None,
            "dataframe_rows": 0,
            "pendings_rows": 0,
            "matched": 0,
            "unmatched": 0,
            "csv": str(out_csv_path),
            "pendings_csv": None,
        }
        _save_metrics(out_dir, metrics)
        return metrics

    # Standardise outputs
    out_csv_path.write_text(result.dataframe_path.read_text(encoding="utf-8"), encoding="utf-8")
    pendings_out: Optional[Path] = None
    if result.pendings_path and result.pendings_path.exists():
        pendings_out = out_dir / "pendings.csv"
        pendings_out.write_text(result.pendings_path.read_text(encoding="utf-8"), encoding="utf-8")

    # Collect metrics
    summary = result.summary
    # Map invoices for lookups
    invoice_paths = {inv.access_key: str(inv.file_path) for inv in summary.invoices}
    invoice_items_total = {inv.access_key: len(inv.items) for inv in summary.invoices}
    # Per-invoice aggregation
    from collections import Counter
    matched_keys = Counter(md.item.invoice_key for md in summary.matched)
    unmatched_keys = Counter(um.item.invoice_key for um in summary.unmatched)
    per_invoice = []
    for access_key, total in invoice_items_total.items():
        m = int(matched_keys.get(access_key, 0))
        u = int(unmatched_keys.get(access_key, 0))
        per_invoice.append({
            "access_key": access_key,
            "file_path": invoice_paths.get(access_key),
            "items_total": int(total),
            "matched": m,
            "unmatched": u,
            "pct_matched": round(100 * m / total, 2) if total else 0.0,
        })

    metrics = {
        "variant": mode,
        "run_id": summary.run_id,
        "dataframe_rows": len(summary.matched) and sum(1 for _ in open(out_csv_path, "r", encoding="utf-8", newline="")) - 1 or 0,
        "pendings_rows": int((pendings_out.read_text(encoding="utf-8").count("\n") - 1) if pendings_out and pendings_out.exists() else 0),
        "matched": len(summary.matched),
        "unmatched": len(summary.unmatched),
        "csv": str(out_csv_path),
        "pendings_csv": str(pendings_out) if pendings_out else None,
        "per_invoice": per_invoice,
    }

    # Header validation against expected columns
    expected_headers = list(settings.csv_output.columns)
    # Append metafields columns in the same way as generator does
    namespace = settings.metafields.namespace
    expected_headers += [f"product.metafields.{namespace}.{key}" for key in settings.metafields.keys.values()]

    try:
        with out_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            actual_headers = next(reader, [])
    except Exception:
        actual_headers = []

    header_ok = actual_headers == expected_headers
    metrics.update({
        "header_ok": header_ok,
        "expected_headers": expected_headers,
        "actual_headers": actual_headers,
        "threshold_used": matcher_threshold,
    })

    # Confidence buckets by SKU (use max confidence per SKU)
    sku_conf: Dict[str, float] = {}
    for md in summary.matched:
        sku = md.product.sku
        conf = float(getattr(md, "confidence", 0.0) or 0.0)
        if conf > sku_conf.get(sku, 0.0):
            sku_conf[sku] = conf

    buckets = {"high": 0, "mid": 0, "low": 0}
    for conf in sku_conf.values():
        if conf >= 0.95:
            buckets["high"] += 1
        elif conf >= 0.90:
            buckets["mid"] += 1
        else:
            buckets["low"] += 1
    metrics["confidence_buckets"] = buckets

    # Detailed samples of first 5 pendings with suggestions
    pend_samples = []
    for pending in summary.unmatched[:5]:
        item = pending.item
        suggs = []
        for s in (pending.suggestions or [])[:3]:
            try:
                suggs.append({
                    "sku": s.product.sku,
                    "title": s.product.title,
                    "confidence": round(float(getattr(s, "confidence", 0.0) or 0.0), 3),
                })
            except Exception:
                continue
        pend_samples.append({
            "invoice_key": item.invoice_key,
            "file_path": invoice_paths.get(item.invoice_key),
            "cProd": item.sku,
            "barcode": item.barcode,
            "description": item.description,
            "reason": pending.reason,
            "top_suggestions": suggs,
        })
    metrics["pendings_samples"] = pend_samples

    # Persist metrics before potential exit
    _save_metrics(out_dir, metrics)

    # Optionally split outputs into high confidence and review files
    try:
        with out_csv_path.open("r", encoding="utf-8-sig", newline="") as rf:
            r = csv.DictReader(rf)
            rows = list(r)
            header = r.fieldnames or expected_headers
        def write_subset(path: Path, predicate):
            with path.open("w", encoding="utf-8-sig", newline="") as wf:
                w = csv.DictWriter(wf, fieldnames=header)
                w.writeheader()
                for row in rows:
                    sku = row.get("SKU", "")
                    if predicate(sku_conf.get(sku, 0.0)):
                        w.writerow(row)
        write_subset(out_dir / "result_high_conf.csv", lambda c: c >= 0.95)
        write_subset(out_dir / "result_review.csv", lambda c: 0.90 <= c < 0.95)
    except Exception:
        # Best-effort: ignore splitting errors
        pass

    # Fail runner if headers diverge from expected
    if not header_ok:
        diff_missing = [h for h in expected_headers if h not in actual_headers]
        diff_extra = [h for h in actual_headers if h not in expected_headers]
        print("[arena] Header validation failed", file=sys.stderr)
        print("Expected:", expected_headers, file=sys.stderr)
        print("Actual  :", actual_headers, file=sys.stderr)
        if diff_missing:
            print("Missing :", diff_missing, file=sys.stderr)
        if diff_extra:
            print("Extra   :", diff_extra, file=sys.stderr)
        # Exit non-zero per requirement
        sys.exit(2)
    # Convenience artifacts
    (out_dir / "run_id.txt").write_text(str(summary.run_id), encoding="utf-8")
    return metrics


__all__ = ["run"]
