import csv
import os
import json
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

VARIANTS = ["A", "B", "C", "D"]
# Repository root (directory that contains 'arena/')
REPO = Path(__file__).resolve().parents[1]
RUNNERS = {v: REPO / f"arena/variants/{v}/runner.py" for v in VARIANTS}
OUTS = {v: REPO / f"arena/variants/{v}/out/result.csv" for v in VARIANTS}
OUT_DIRS = {v: p.parent for v, p in OUTS.items()}


def _load_yaml_defaults():
    ns = "custom"
    ncm_key = "ncm"
    unidade_key = "unidade"
    cest_key = "cest"
    cfg_path = REPO / "config.yaml"
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        ns = (data.get("metafields", {}) or {}).get("namespace", ns)
        keys = (data.get("metafields", {}) or {}).get("keys", {}) or {}
        ncm_key = keys.get("ncm", ncm_key)
        unidade_key = keys.get("unidade", unidade_key)
        cest_key = keys.get("cest", cest_key)
    except Exception:
        pass
    return ns, ncm_key, unidade_key, cest_key


def _fieldnames_for_metrics():
    namespace, ncm_key, _, _ = _load_yaml_defaults()
    ean_col = "Barcode"
    ncm_col = f"product.metafields.{namespace}.{ncm_key}"
    desc_col = "Title"
    return ean_col, ncm_col, desc_col


def _run_tests_optional() -> str:
    try:
        t0 = time.time()
        env = dict(**os.environ)
        env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
        res = subprocess.run([sys.executable, "-m", "pytest", "-q"], cwd=str(REPO), env=env)
        dt = time.time() - t0
        return f"pass ({dt:.2f}s)" if res.returncode == 0 else f"fail ({dt:.2f}s)"
    except Exception:
        return "skipped"


def run_variant(v: str):
    OUT_DIRS[v].mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    proc = subprocess.run([sys.executable, str(RUNNERS[v])], cwd=str(REPO))
    dt = time.time() - t0

    # Base metrics from runner
    metrics_path = OUT_DIRS[v] / "metrics.json"
    base = {}
    if metrics_path.exists():
        try:
            base = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            base = {}

    # CSV-derived completeness metrics
    total = ok = 0
    field_errors = defaultdict(int)
    ean_col, ncm_col, desc_col = _fieldnames_for_metrics()
    if OUTS[v].exists():
        with OUTS[v].open(newline="", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                total += 1
                missing = []
                if not (row.get(ean_col) or "").strip():
                    missing.append("ean")
                if not (row.get(ncm_col) or "").strip():
                    missing.append("ncm")
                if not (row.get(desc_col) or "").strip():
                    missing.append("descricao")
                if not missing:
                    ok += 1
                for m in missing:
                    field_errors[m] += 1

    return {
        "variant": v,
        "itens_total": total,
        "itens_ok": ok,
        "pct_completo": round(100 * ok / total, 2) if total else 0.0,
        "tempo_s": round(dt, 3),
        "erros_por_campo": dict(field_errors),
        "matched": base.get("matched"),
        "unmatched": base.get("unmatched"),
        "run_id": base.get("run_id"),
        "exit_code": proc.returncode,
        "header_ok": base.get("header_ok"),
        "threshold_used": base.get("threshold_used"),
        "confidence_buckets": base.get("confidence_buckets", {}),
    }


def main():
    reports = REPO / "arena/reports"
    reports.mkdir(parents=True, exist_ok=True)

    test_status = _run_tests_optional()

    results = [run_variant(v) for v in VARIANTS]

    # Scoreboard CSV
    scoreboard = reports / "scoreboard.csv"
    with scoreboard.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "variant",
            "itens_total",
            "itens_ok",
            "pct_completo",
            "tempo_s",
            "matched",
            "unmatched",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k) for k in fieldnames})

    # Markdown summary
    md_lines = [
        "# Arena – Relatório",
        "",
        f"Testes: {test_status}",
        "",
        "| Variante | Itens | OK | % Completo | Tempo (s) | Matched | Unmatched |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        md_lines.append(
            f"| {r['variant']} | {r['itens_total']} | {r['itens_ok']} | {r['pct_completo']} | {r['tempo_s']} | {r.get('matched','')} | {r.get('unmatched','')} |"
        )

    md_lines += ["", "## Confiabilidade", ""]
    for r in results:
        buckets = r.get("confidence_buckets", {}) or {}
        md_lines.append(f"### {r['variant']}")
        md_lines.append(f"- threshold_used: {r.get('threshold_used')}")
        md_lines.append(f"- buckets: high={buckets.get('high',0)}, mid={buckets.get('mid',0)}, low={buckets.get('low',0)}")
        md_lines.append("")

    # Match source distribution
    md_lines += ["", "## Distribuição por fonte de match", ""]
    for v in VARIANTS:
        metrics_path = OUT_DIRS[v] / "metrics.json"
        if not metrics_path.exists():
            continue
        try:
            base = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            base = {}
        src = base.get("match_source_counts") or {}
        md_lines.append(f"### {v}")
        if src:
            md_lines.append("| fonte | contagem |")
            md_lines.append("|---|---:|")
            for k, val in src.items():
                md_lines.append(f"| {k} | {val} |")
        else:
            md_lines.append("- (sem dados)")
        md_lines.append("")

    # Per-XML breakdown
    md_lines += ["", "## Por XML (por variante)", ""]
    for v in VARIANTS:
        metrics_path = OUT_DIRS[v] / "metrics.json"
        if not metrics_path.exists():
            continue
        try:
            base = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            base = {}
        table = base.get("per_invoice") or []
        if table:
            md_lines.append(f"### {v}")
            md_lines.append("| Arquivo | Itens | Matched | Unmatched | % Matched |")
            md_lines.append("|---|---:|---:|---:|---:|")
            for row in table:
                fname = (row.get("file_path") or "").split("\\")[-1].split("/")[-1]
                md_lines.append(f"| {fname} | {row.get('items_total',0)} | {row.get('matched',0)} | {row.get('unmatched',0)} | {row.get('pct_matched',0)} |")
            md_lines.append("")

    # Detailed pendings samples
    md_lines += ["", "## Pendências (amostras detalhadas)", ""]
    for v in VARIANTS:
        metrics_path = OUT_DIRS[v] / "metrics.json"
        if not metrics_path.exists():
            continue
        try:
            base = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            base = {}
        samples = base.get("pendings_samples") or []
        md_lines.append(f"### {v}")
        if samples:
            md_lines.append("| Arquivo | cProd | barcode | description | reason | top_suggestions |")
            md_lines.append("|---|---|---|---|---|---|")
            for s in samples:
                fname = (s.get("file_path") or "").split("\\")[-1].split("/")[-1]
                tops = "; ".join([f"{x.get('sku')} ({x.get('confidence')})" for x in (s.get('top_suggestions') or [])])
                md_lines.append(f"| {fname} | {s.get('cProd','')} | {s.get('barcode','')} | {s.get('description','').replace('|','/')} | {s.get('reason','')} | {tops} |")
        else:
            md_lines.append("- (sem pendências)")
        md_lines.append("")

    md_lines += ["", "## Erros por campo", ""]
    for r in results:
        md_lines.append(f"### {r['variant']}")
        if r["erros_por_campo"]:
            for k, v in r["erros_por_campo"].items():
                md_lines.append(f"- {k}: {v}")
        else:
            md_lines.append("- Nenhum erro mapeado.")
        md_lines.append("")

    # Snippets
    namespace, ncm_key, unidade_key, cest_key = _load_yaml_defaults()
    ncm_col = f"product.metafields.{namespace}.{ncm_key}"
    uni_col = f"product.metafields.{namespace}.{unidade_key}"
    cest_col = f"product.metafields.{namespace}.{cest_key}"
    core_cols = ["Handle", "Title", "SKU", "Barcode", ncm_col, uni_col, cest_col]

    md_lines += ["", "## Amostras", ""]
    for v in VARIANTS:
        md_lines.append(f"### {v} – primeiras 5 linhas (colunas principais)")
        p = OUTS[v]
        if p.exists():
            try:
                with p.open("r", encoding="utf-8-sig", newline="") as f:
                    r = csv.DictReader(f)
                    rows = [row for _, row in zip(range(5), r)]
                if rows:
                    md_lines.append("| " + " | ".join(core_cols) + " |")
                    md_lines.append("|" + "|".join(["---"] * len(core_cols)) + "|")
                    for row in rows:
                        md_lines.append("| " + " | ".join([str(row.get(c, "")) for c in core_cols]) + " |")
                else:
                    md_lines.append("- (sem linhas)")
            except Exception:
                md_lines.append("- (falha ao ler result.csv)")
        else:
            md_lines.append("- (arquivo ausente)")

        pend = OUT_DIRS[v] / "pendings.csv"
        md_lines.append(f"{v} – primeiras 5 pendências (cProd, barcode, description)")
        if pend.exists():
            try:
                with pend.open("r", encoding="utf-8-sig", newline="") as f:
                    r = csv.DictReader(f)
                    rows = [row for _, row in zip(range(5), r)]
                if rows:
                    md_lines.append("| cProd | barcode | description |")
                    md_lines.append("|---|---|---|")
                    for row in rows:
                        md_lines.append("| " + " | ".join([str(row.get(k, "")) for k in ["cProd", "barcode", "description"]]) + " |")
                else:
                    md_lines.append("- (sem pendências)")
            except Exception:
                md_lines.append("- (falha ao ler pendings.csv)")
        else:
            md_lines.append("- (pendings.csv ausente)")
        md_lines.append("")

    (reports / "summary.md").write_text("\n".join(md_lines), encoding="utf-8")

    # Also generate an HTML summary with the same data/snippets
    def html_escape(s: str) -> str:
        import html as _html
        return _html.escape(s, quote=True)

    def render_table(headers, rows):
        parts = ["<table><thead><tr>"]
        for h in headers:
            parts.append(f"<th>{html_escape(str(h))}</th>")
        parts.append("</tr></thead><tbody>")
        for row in rows:
            parts.append("<tr>")
            for cell in row:
                parts.append(f"<td>{html_escape(str(cell))}</td>")
            parts.append("</tr>")
        parts.append("</tbody></table>")
        return "".join(parts)

    html_parts = [
        "<!doctype html>",
        "<meta charset=\"utf-8\">",
        "<title>Arena – Summary</title>",
        "<style>body{font-family:system-ui,Segoe UI,Arial,sans-serif;padding:24px;max-width:1100px;margin:auto} table{border-collapse:collapse;width:100%;margin:12px 0} th,td{border:1px solid #ddd;padding:8px} th{background:#f5f5f5;text-align:left} tr:nth-child(even){background:#fafafa} h1,h2,h3{margin:18px 0 8px}</style>",
        "<h1>Arena – Relatório</h1>",
        f"<p>Testes: {html_escape(test_status)}</p>",
        "<h2>Scoreboard</h2>",
    ]

    sb_headers = ["Variante", "Itens", "OK", "% Completo", "Tempo (s)", "Matched", "Unmatched"]
    sb_rows = [
        [r['variant'], r['itens_total'], r['itens_ok'], r['pct_completo'], r['tempo_s'], r.get('matched',''), r.get('unmatched','')]
        for r in results
    ]
    html_parts.append(render_table(sb_headers, sb_rows))

    # Confiabilidade
    html_parts.append("<h2>Confiabilidade</h2>")
    conf_headers = ["Variante", "threshold_used", "high", "mid", "low"]
    conf_rows = []
    for r in results:
        b = r.get("confidence_buckets", {}) or {}
        conf_rows.append([r['variant'], r.get('threshold_used'), b.get('high',0), b.get('mid',0), b.get('low',0)])
    html_parts.append(render_table(conf_headers, conf_rows))

    # Match source distribution
    html_parts.append("<h2>Distribuição por fonte de match</h2>")
    for v in VARIANTS:
        metrics_path = OUT_DIRS[v] / "metrics.json"
        base = {}
        if metrics_path.exists():
            try:
                base = json.loads(metrics_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        src = base.get("match_source_counts") or {}
        html_parts.append(f"<h3>{v}</h3>")
        if src:
            headers = ["fonte", "contagem"]
            rows = [[k, v] for k, v in src.items()]
            html_parts.append(render_table(headers, rows))
        else:
            html_parts.append("<p>(sem dados)</p>")

    # Erros por campo
    html_parts.append("<h2>Erros por campo</h2>")
    for r in results:
        html_parts.append(f"<h3>{html_escape(r['variant'])}</h3>")
        errs = r.get("erros_por_campo") or {}
        if errs:
            headers = ["campo", "contagem"]
            rows = [[k, v] for k, v in errs.items()]
            html_parts.append(render_table(headers, rows))
        else:
            html_parts.append("<p>Nenhum erro mapeado.</p>")

    # Amostras
    html_parts.append("<h2>Amostras</h2>")
    for v in VARIANTS:
        html_parts.append(f"<h3>{html_escape(v)} – primeiras 5 linhas (colunas principais)</h3>")
        p = OUTS[v]
        rows = []
        if p.exists():
            try:
                with p.open("r", encoding="utf-8-sig", newline="") as f:
                    r = csv.DictReader(f)
                    rows = [row for _, row in zip(range(5), r)]
            except Exception:
                pass
        if rows:
            headers = core_cols
            data = [[row.get(c, "") for c in core_cols] for row in rows]
            html_parts.append(render_table(headers, data))
        else:
            html_parts.append("<p>(sem linhas)</p>")

        html_parts.append(f"<p>{html_escape(v)} – primeiras 5 pendências (cProd, barcode, description)</p>")
        pend = OUT_DIRS[v] / "pendings.csv"
        prow = []
        if pend.exists():
            try:
                with pend.open("r", encoding="utf-8-sig", newline="") as f:
                    r = csv.DictReader(f)
                    prow = [row for _, row in zip(range(5), r)]
            except Exception:
                pass
        if prow:
            headers = ["cProd", "barcode", "description"]
            data = [[row.get("cProd",""), row.get("barcode",""), row.get("description","")] for row in prow]
            html_parts.append(render_table(headers, data))
        else:
            html_parts.append("<p>(sem pendências)</p>")

    (reports / "summary.html").write_text("".join(html_parts), encoding="utf-8")

    # Full pendings report (HTML) per variant
    pendings_html = [
        "<!doctype html>",
        "<meta charset=\"utf-8\">",
        "<title>Arena – Pendências</title>",
        "<style>body{font-family:system-ui,Segoe UI,Arial,sans-serif;padding:24px;max-width:1200px;margin:auto} table{border-collapse:collapse;width:100%;margin:12px 0} th,td{border:1px solid #ddd;padding:6px} th{background:#f5f5f5;text-align:left} tr:nth-child(even){background:#fafafa} h1,h2,h3{margin:18px 0 8px}</style>",
        "<h1>Pendências – Listagem Completa</h1>",
    ]
    for v in VARIANTS:
        pend = OUT_DIRS[v] / "pendings.csv"
        metrics_path = OUT_DIRS[v] / "metrics.json"
        inv_map = {}
        if metrics_path.exists():
            try:
                base = json.loads(metrics_path.read_text(encoding="utf-8"))
                for row in base.get("per_invoice", []) or []:
                    inv_map[row.get("access_key")] = row.get("file_path")
            except Exception:
                pass
        pendings_html.append(f"<h2>Variante {v}</h2>")
        if pend.exists():
            try:
                with pend.open("r", encoding="utf-8-sig", newline="") as f:
                    r = csv.DictReader(f)
                    rows = list(r)
                if rows:
                    headers = ["invoice_key", "file", "item_number", "cProd", "barcode", "description", "reason"]
                    table_rows = []
                    for row in rows:
                        file_name = inv_map.get(row.get("invoice_key")) or ""
                        file_name = (file_name.replace("\\", "/").split("/")[-1]) if file_name else ""
                        table_rows.append([
                            row.get("invoice_key",""),
                            file_name,
                            row.get("item_number",""),
                            row.get("cProd",""),
                            row.get("barcode",""),
                            row.get("description",""),
                            row.get("reason",""),
                        ])
                    pendings_html.append(render_table(headers, table_rows))
                else:
                    pendings_html.append("<p>(sem pendências)</p>")
            except Exception:
                pendings_html.append("<p>(falha ao ler pendings.csv)</p>")
        else:
            pendings_html.append("<p>(pendings.csv ausente)</p>")

    (reports / "pendings.html").write_text("".join(pendings_html), encoding="utf-8")


if __name__ == "__main__":
    main()
