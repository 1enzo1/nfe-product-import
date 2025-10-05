from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if SRC_DIR.exists():
    src_str = str(SRC_DIR)
    if src_str not in sys.path:
        sys.path.append(src_str)

from nfe_importer.core.generator import (
    CAPACITY_PATTERN,
    COLOR_TOKEN_SET,
    SIZE_NUMERIC_RANGE,
    SIZE_TOKEN_CHOICES,
    SIZE_TOKEN_PATTERN,
    WEIGHT_PATTERN,
)
from nfe_importer.core.text_splitters import DEFAULT_USAGE_MARKERS, normalise_markers, usage_score

EMPTY_MARKERS = {"", "nan", "none", "null"}


COMPOSITION_WARNING_KEYWORDS = (
    'polirresina',
)
MAX_SNIPPET_LEN = 120
USAGE_WARNING_FIELD = 'Body (HTML)'


def get_reports_dir() -> Path:
    directory = Path('reports') / 'validation'
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def pick_warning_name(csv_path: Path) -> Path:
    reports_dir = get_reports_dir()
    try:
        relative_name = csv_path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        relative_name = csv_path.name
    slug = re.sub(r'[^A-Za-z0-9_.-]+', '_', str(relative_name))
    return reports_dir / f"{slug}_warnings.csv"


def extract_snippet(text: str, index: int, marker_length: int) -> str:
    start = max(0, index - 40)
    end = min(len(text), index + marker_length + 80)
    snippet = text[start:end].replace('\r', ' ').replace('\n', ' ').strip()
    if len(snippet) > MAX_SNIPPET_LEN:
        snippet = snippet[: MAX_SNIPPET_LEN - 3].rstrip() + '...'
    return snippet


def find_marker_in_text(text: str, markers: Sequence[str]) -> tuple[str | None, int]:
    lowered = text.casefold()
    best_idx = -1
    best_marker: str | None = None
    for marker in markers:
        idx = lowered.find(marker)
        if idx != -1 and (best_idx == -1 or idx < best_idx):
            best_marker = marker
            best_idx = idx
    return best_marker, best_idx






def classify_option_value(value: str) -> str:
    if not value:
        return ''
    text = str(value).strip()
    if not text:
        return ''
    upper = text.upper()
    if upper in SIZE_TOKEN_CHOICES:
        return 'size'
    token_match = SIZE_TOKEN_PATTERN.search(text)
    if token_match:
        token = token_match.group(1).upper()
        if token in SIZE_TOKEN_CHOICES:
            return 'size'
    if text.isdigit() and len(text) == 2:
        try:
            numeric = int(text)
        except ValueError:
            numeric = None
        if numeric in SIZE_NUMERIC_RANGE:
            return 'size'
    capacity_match = CAPACITY_PATTERN.search(text)
    if capacity_match:
        return 'capacity'
    weight_match = WEIGHT_PATTERN.search(text)
    if weight_match:
        return 'weight'
    lowered = text.casefold()
    for color in COLOR_TOKEN_SET:
        if color in lowered:
            return 'color'
    return 'other'


def collect_warnings(dataframe: pd.DataFrame, markers: Sequence[str]) -> List[Dict[str, str]]:
    warnings: List[Dict[str, str]] = []
    markers = normalise_markers(markers)
    body_col = USAGE_WARNING_FIELD
    comp_col = 'product.metafields.custom.composicao'
    modo_col = 'product.metafields.custom.modo_de_uso'

    index = dataframe.index
    body_series = dataframe[body_col].fillna('').astype(str) if body_col in dataframe.columns else pd.Series([''] * len(index), index=index, dtype=str)
    composition_series = dataframe[comp_col].fillna('').astype(str) if comp_col in dataframe.columns else pd.Series([''] * len(index), index=index, dtype=str)
    modo_series = dataframe[modo_col].fillna('').astype(str) if modo_col in dataframe.columns else pd.Series([''] * len(index), index=index, dtype=str)

    sku_columns = [col for col in ('Variant SKU', 'SKU', 'Handle') if col in dataframe.columns]
    composition_keywords = tuple(keyword.casefold() for keyword in COMPOSITION_WARNING_KEYWORDS)
    taxonomy_fields = ['Product Category', 'Type', 'Collection']

    def resolve_sku(idx) -> str:
        for col in sku_columns:
            raw = dataframe.at[idx, col] if col in dataframe.columns else ''
            if pd.notna(raw):
                candidate = str(raw).strip()
                if candidate:
                    return candidate
        return ''

    handle_info: Dict[str, Dict[str, object]] = {}

    for idx in index:
        sku_value = resolve_sku(idx)
        body_text = body_series.get(idx, '').strip()
        modo_text = modo_series.get(idx, '').strip()
        composition_text = composition_series.get(idx, '').strip()
        composition_lower = composition_text.casefold()
        handle = ''
        if 'Handle' in dataframe.columns:
            raw_handle = dataframe.at[idx, 'Handle']
            if pd.notna(raw_handle):
                handle = str(raw_handle).strip()

        info = handle_info.setdefault(handle, {
            'count': 0,
            'names': set(),
            'names_display': set(),
            'values': [],
            'skus': []
        })
        info['count'] += 1
        if sku_value and sku_value not in info['skus']:
            info['skus'].append(sku_value)

        # enforce empty taxonomy fields (must remain blank)
        taxonomy_non_empty = False
        for field in taxonomy_fields:
            if field in dataframe.columns:
                raw = dataframe.at[idx, field]
                value = str(raw).strip() if pd.notna(raw) else ''
                if value:
                    taxonomy_non_empty = True
                    break
        if taxonomy_non_empty:
            warnings.append({
                'sku': sku_value,
                'handle': handle,
                'field': 'Product Category/Type/Collection',
                'warning_type': 'empty_enforced',
                'snippet': 'must be empty',
            })

        # option consistency
        for option in range(1, 4):
            name_key = f'Option{option} Name'
            value_key = f'Option{option} Value'
            name = ''
            value = ''
            if name_key in dataframe.columns:
                raw = dataframe.at[idx, name_key]
                if pd.notna(raw):
                    name = str(raw).strip()
            if value_key in dataframe.columns:
                raw = dataframe.at[idx, value_key]
                if pd.notna(raw):
                    value = str(raw).strip()
            if option == 1:
                if name:
                    info['names'].add(name.casefold())
                    info['names_display'].add(name)
                if value:
                    info['values'].append(value)
            if value and not name:
                warnings.append({
                    'sku': sku_value,
                    'handle': handle,
                    'field': f'Option{option}',
                    'warning_type': 'option_consistency',
                    'snippet': 'missing_name',
                })
            elif name and not value:
                warnings.append({
                    'sku': sku_value,
                    'handle': handle,
                    'field': f'Option{option}',
                    'warning_type': 'option_consistency',
                    'snippet': 'missing_value',
                })
            elif value.casefold() == 'default title':
                warnings.append({
                    'sku': sku_value,
                    'handle': handle,
                    'field': f'Option{option}',
                    'warning_type': 'option_consistency',
                    'snippet': 'default_title',
                })

        if not body_text:
            if modo_text:
                modo_score = usage_score(modo_text, markers)
                if modo_score <= 1:
                    snippet = modo_text
                    if len(snippet) > MAX_SNIPPET_LEN:
                        snippet = snippet[: MAX_SNIPPET_LEN - 3].rstrip() + '...'
                    warnings.append({
                        'sku': sku_value,
                        'handle': handle,
                        'field': modo_col,
                        'warning_type': 'desc_missing_but_usage_weak',
                        'snippet': snippet,
                    })
            continue

        marker, marker_idx = find_marker_in_text(body_text, markers)
        if marker and marker_idx != -1:
            snippet = extract_snippet(body_text, marker_idx, len(marker))
            warnings.append({
                'sku': sku_value,
                'handle': handle,
                'field': body_col,
                'warning_type': 'usage_in_body',
                'snippet': snippet,
            })

        body_lower = body_text.casefold()
        for keyword in composition_keywords:
            idx_kw = body_lower.find(keyword)
            if idx_kw != -1 and keyword not in composition_lower:
                snippet = extract_snippet(body_text, idx_kw, len(keyword))
                warnings.append({
                    'sku': sku_value,
                    'handle': handle,
                    'field': body_col,
                    'warning_type': 'composition_mismatch',
                    'snippet': snippet,
                })
                break

        # Body source heuristics
        if len(body_text) < 40:
            snippet = extract_snippet(body_text, 0, len(body_text))
            warnings.append({
                'sku': sku_value,
                'handle': handle,
                'field': body_col,
                'warning_type': 'body_source',
                'snippet': 'body-too-short: ' + snippet,
            })
        elif modo_text and body_lower == modo_text.casefold():
            snippet = extract_snippet(body_text, 0, len(body_text))
            warnings.append({
                'sku': sku_value,
                'handle': handle,
                'field': body_col,
                'warning_type': 'body_source',
                'snippet': 'same-as-modo-de-uso: ' + snippet,
            })

    for handle, info in handle_info.items():
        if not handle or info.get('count', 0) <= 1:
            continue
        normalized_names = {name for name in info['names'] if name}
        if len(normalized_names) > 1:
            names_display = sorted({name for name in info['names_display'] if name})
            warnings.append({
                'sku': info['skus'][0] if info['skus'] else '',
                'handle': handle,
                'field': 'Option1',
                'warning_type': 'option_axis_mixed',
                'snippet': ', '.join(names_display) or 'mixed axis',
            })
        values = [val for val in info['values'] if val]
        if len(values) > 1:
            classes = [classify_option_value(val) for val in values]
            primary = {cls for cls in classes if cls not in {'', 'other'}}
            mixed_types = len(primary) > 1
            has_other_with_primary = 'other' in classes and primary
            if mixed_types or has_other_with_primary:
                warnings.append({
                    'sku': info['skus'][0] if info['skus'] else '',
                    'handle': handle,
                    'field': 'Option1',
                    'warning_type': 'option_value_incoherent',
                    'snippet': ', '.join(sorted(set(values))),
                })

    return warnings




@dataclass(frozen=True)
class Validator:
    check: Callable[[pd.Series, pd.DataFrame], pd.Series]
    message: str
    skip_blank: bool = True


@dataclass(frozen=True)
class FieldRule:
    column: str
    doc_label: str
    required: bool
    validators: Sequence[Validator] = field(default_factory=tuple)
    notes: str = ""


def normalize_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _mask_blank(normalized: pd.Series) -> pd.Series:
    return normalized.str.lower().isin(EMPTY_MARKERS)


def validator_positive_number(normalized: pd.Series, _df: pd.DataFrame) -> pd.Series:
    cleaned = normalized.str.replace(",", ".", regex=False)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    return numeric.isna() | (numeric <= 0)


def validator_non_negative_int(normalized: pd.Series, _df: pd.DataFrame) -> pd.Series:
    cleaned = normalized.str.replace(",", ".", regex=False)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    invalid = numeric.isna() | (numeric < 0)
    if not numeric.isna().all():
        fractional = numeric.fillna(0) % 1
        invalid |= fractional.ne(0)
    return invalid


def allowed_values_validator(values: Iterable[str]) -> Callable[[pd.Series, pd.DataFrame], pd.Series]:
    allowed = {str(v).strip().lower() for v in values}

    def _check(normalized: pd.Series, _df: pd.DataFrame) -> pd.Series:
        lowered = normalized.str.lower()
        return ~lowered.isin(allowed)

    return _check


def weight_rule_validator(normalized: pd.Series, df: pd.DataFrame) -> pd.Series:
    cleaned = normalized.str.replace(",", ".", regex=False)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    if "Variant Weight Unit" not in df.columns:
        return pd.Series(True, index=normalized.index)
    unit_normalized = normalize_series(df["Variant Weight Unit"]).str.lower()
    invalid = numeric.isna() | (numeric <= 0)
    invalid |= ~unit_normalized.isin({"g", "kg"})
    invalid |= unit_normalized.eq("g") & (numeric >= 1000)
    invalid |= unit_normalized.eq("kg") & (numeric < 1)
    return invalid


FIELD_RULES: Dict[str, FieldRule] = {
    "Handle": FieldRule(column="Handle", doc_label="Handle", required=True),
    "Title": FieldRule(column="Title", doc_label="Title", required=True),
    "Vendor": FieldRule(column="Vendor", doc_label="Vendor", required=True),
    "Product Category": FieldRule(column="Product Category", doc_label="Product Category", required=True),
    "Published": FieldRule(
        column="Published",
        doc_label="Published",
        required=True,
        validators=(
            Validator(allowed_values_validator(["TRUE", "FALSE"]), "valor deve ser TRUE ou FALSE"),
        ),
    ),
    "Option1 Name": FieldRule(column="Option1 Name", doc_label="Option1 Name", required=True),
    "Option1 Value": FieldRule(column="Option1 Value", doc_label="Option1 Value", required=True),
    "Variant SKU": FieldRule(column="Variant SKU", doc_label="SKU", required=True),
    "Variant Price": FieldRule(
        column="Variant Price",
        doc_label="Variant Price",
        required=True,
        validators=(
            Validator(validator_positive_number, "valor deve ser numerico > 0"),
        ),
    ),
    "Variant Inventory Qty": FieldRule(
        column="Variant Inventory Qty",
        doc_label="Variant Inventory Qty",
        required=True,
        validators=(
            Validator(validator_non_negative_int, "valor deve ser inteiro >= 0"),
        ),
    ),
    "Variant Inventory Policy": FieldRule(
        column="Variant Inventory Policy",
        doc_label="Variant Inventory Policy",
        required=True,
        validators=(
            Validator(allowed_values_validator(["deny", "continue"]), "usar deny ou continue"),
        ),
    ),
    "Variant Fulfillment Service": FieldRule(
        column="Variant Fulfillment Service",
        doc_label="Variant Fulfillment Service",
        required=True,
        validators=(
            Validator(allowed_values_validator(["manual"]), "usar manual"),
        ),
    ),
    "Variant Requires Shipping": FieldRule(
        column="Variant Requires Shipping",
        doc_label="Variant Requires Shipping",
        required=True,
        validators=(
            Validator(allowed_values_validator(["TRUE", "FALSE"]), "valor deve ser TRUE ou FALSE"),
        ),
    ),
    "Variant Taxable": FieldRule(
        column="Variant Taxable",
        doc_label="Variant Taxable",
        required=True,
        validators=(
            Validator(allowed_values_validator(["TRUE", "FALSE"]), "valor deve ser TRUE ou FALSE"),
        ),
    ),
    "Variant Weight": FieldRule(
        column="Variant Weight",
        doc_label="Variant Weight",
        required=True,
        validators=(
            Validator(weight_rule_validator, "peso invalido ou inconsistente com unidade"),
        ),
    ),
    "Variant Weight Unit": FieldRule(
        column="Variant Weight Unit",
        doc_label="Variant Weight Unit",
        required=True,
        validators=(
            Validator(allowed_values_validator(["g", "kg"]), "usar g ou kg"),
        ),
    ),
    "product.metafields.custom.ncm": FieldRule(
        column="product.metafields.custom.ncm",
        doc_label="product.metafields.custom.ncm",
        required=True,
    ),
    "product.metafields.custom.ipi": FieldRule(
        column="product.metafields.custom.ipi",
        doc_label="product.metafields.custom.ipi",
        required=True,
    ),
    "Body (HTML)": FieldRule(column="Body (HTML)", doc_label="Body (HTML)", required=False, notes="Opcional no guia"),
    "Tags": FieldRule(column="Tags", doc_label="Tags", required=False, notes="Opcional no guia"),
    "Type": FieldRule(column="Type", doc_label="Type", required=False, notes="Opcional no guia"),
    "Variant Barcode": FieldRule(column="Variant Barcode", doc_label="Barcode (EAN)", required=False, notes="Opcional no guia"),
    "Variant Compare At Price": FieldRule(column="Variant Compare At Price", doc_label="Variant Compare At Price", required=False, notes="Opcional no guia"),
    "product.metafields.custom.unidade": FieldRule(
        column="product.metafields.custom.unidade",
        doc_label="product.metafields.custom.unidade",
        required=False,
        notes="Opcional no guia",
    ),
    "product.metafields.custom.composicao": FieldRule(
        column="product.metafields.custom.composicao",
        doc_label="product.metafields.custom.composicao",
        required=False,
        notes="Opcional no guia",
    ),
    "product.metafields.custom.dimensoes_do_produto": FieldRule(
        column="product.metafields.custom.dimensoes_do_produto",
        doc_label="product.metafields.custom.dimensoes_do_produto",
        required=False,
        notes="Opcional no guia",
    ),
}


def default_rule(column: str) -> FieldRule:
    return FieldRule(column=column, doc_label=column, required=False, notes="Nao mapeado no guia")


def pick_report_name(csv_path: Path) -> Path:
    reports_dir = get_reports_dir()
    try:
        relative_name = csv_path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        relative_name = csv_path.name
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(relative_name))
    return reports_dir / f"{slug}_field_report.csv"


def summarise_series(handles: pd.Series, mask: pd.Series) -> str:
    if mask.sum() == 0:
        return ""
    selected = handles[mask]
    deduped: List[str] = []
    for value in selected:
        if not value:
            continue
        if value not in deduped:
            deduped.append(value)
        if len(deduped) == 5:
            break
    return ", ".join(deduped)


def validate_csv(csv_path: Path) -> Dict[str, object]:
    dataframe = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    total_rows = len(dataframe)
    handle_series: pd.Series
    if "Handle" in dataframe.columns:
        handle_series = normalize_series(dataframe["Handle"])
        fallback = pd.Series([f"row_{idx + 1}" for idx in range(total_rows)], index=dataframe.index)
        handle_series = handle_series.where(handle_series != "", fallback)
    else:
        handle_series = pd.Series([f"row_{idx + 1}" for idx in range(total_rows)], index=dataframe.index)

    columns_to_check: List[str] = []
    for column in dataframe.columns:
        if column not in columns_to_check:
            columns_to_check.append(column)
    for column in FIELD_RULES:
        if column not in columns_to_check:
            columns_to_check.append(column)

    report_rows: List[Dict[str, object]] = []
    alerts: List[str] = []

    for column in columns_to_check:
        rule = FIELD_RULES.get(column, default_rule(column))
        exists = column in dataframe.columns
        normalized = normalize_series(dataframe[column]) if exists else pd.Series(dtype=str)
        blank_mask = _mask_blank(normalized) if exists else pd.Series(dtype=bool)
        missing_count = int(blank_mask.sum()) if exists else total_rows
        filled_count = max(total_rows - missing_count, 0)
        coverage = (filled_count / total_rows * 100) if total_rows else 0.0
        invalid_mask = pd.Series([False] * total_rows, index=dataframe.index)
        validator_details: List[str] = []

        if exists:
            for validator in rule.validators:
                mask = validator.check(normalized, dataframe)
                if validator.skip_blank:
                    mask &= ~blank_mask
                mask = mask.fillna(False)
                if mask.any():
                    invalid_mask |= mask
                    sample = summarise_series(handle_series, mask)
                    detail = validator.message
                    if sample:
                        detail = f"{detail} -> {sample}"
                    validator_details.append(detail)
        else:
            coverage = 0.0

        invalid_count = int(invalid_mask.sum()) if exists else 0
        status_parts: List[str] = []

        if not exists:
            status_parts.append("coluna ausente")
        if rule.required and (not exists or missing_count > 0):
            status_parts.append("faltam valores obrigatorios")
        if invalid_count > 0:
            status_parts.append("valores invalidos")

        status = "; ".join(status_parts) if status_parts else "ok"
        missing_examples = summarise_series(handle_series, blank_mask) if exists else summarise_series(handle_series, pd.Series([True] * total_rows))
        invalid_examples = summarise_series(handle_series, invalid_mask) if exists else ""

        report_rows.append(
            {
                "column": column,
                "doc_label": rule.doc_label,
                "requirement": "obrigatorio" if rule.required else "opcional",
                "status": status,
                "missing_values": missing_count if exists else total_rows,
                "invalid_values": invalid_count,
                "coverage_pct": round(coverage, 2),
                "missing_examples": missing_examples,
                "invalid_examples": invalid_examples,
                "notes": rule.notes,
                "validator_details": "; ".join(validator_details),
            }
        )

        if status != "ok" and rule.required:
            base = f"{column}: {status}"
            if missing_examples:
                base += f" | faltando em: {missing_examples}"
            if invalid_examples:
                base += f" | invalidos em: {invalid_examples}"
            alerts.append(base)

    report_df = pd.DataFrame(report_rows)
    report_path = pick_report_name(csv_path)
    report_df.to_csv(report_path, index=False, encoding='utf-8')

    warnings_list = collect_warnings(dataframe, DEFAULT_USAGE_MARKERS)
    warnings_path = None
    if warnings_list:
        warnings_df = pd.DataFrame(warnings_list, columns=['sku', 'handle', 'field', 'warning_type', 'snippet'])
        warnings_path = pick_warning_name(csv_path)
        warnings_df.to_csv(warnings_path, index=False, encoding='utf-8')

    return {
        'csv_path': csv_path,
        'rows': total_rows,
        'report_path': report_path,
        'alerts': alerts,
        'report': report_df,
        'warnings': warnings_list,
        'warnings_path': warnings_path,
    }


def discover_csv_paths(paths: Sequence[str]) -> List[Path]:
    discovered: List[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            discovered.extend(sorted(path.rglob("*.csv")))
        elif any(char in raw for char in "*?["):
            discovered.extend(sorted(Path().glob(raw)))
        elif path.exists():
            discovered.append(path)
        else:
            raise FileNotFoundError(f"Path not found: {raw}")
    return discovered


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Valida campos obrigatorios e opcionais dos CSVs gerados pelo conversor.")
    parser.add_argument("paths", nargs="+", help="Arquivos CSV, pastas ou glob patterns")
    args = parser.parse_args(argv)

    csv_paths = discover_csv_paths(args.paths)
    if not csv_paths:
        raise SystemExit("Nenhum CSV encontrado para validar")

    for csv_path in csv_paths:
        result = validate_csv(csv_path)
        print(f"\nArquivo: {csv_path}")
        print(f"Linhas: {result['rows']}")
        if result["alerts"]:
            print("Problemas identificados:")
            for alert in result["alerts"]:
                print(f"  - {alert}")
        else:
            print("Nenhum problema em campos obrigatorios.")
        if result['warnings']:
            print('Warnings:')
            for warning in result['warnings']:
                sku = warning.get('sku') or '(sem sku)'
                handle = warning.get('handle') or '(sem handle)'
                print(f"  - {sku} [{handle}]: {warning['warning_type']} -> {warning['snippet']}")
        print(f"Relatorio salvo em: {result['report_path']}")
        if result['warnings_path']:
            print(f"Warnings detalhados em: {result['warnings_path']}")


if __name__ == "__main__":
    main()
