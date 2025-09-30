from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import pandas as pd

EMPTY_MARKERS = {"", "nan", "none", "null"}


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
    reports_dir = Path("reports") / "validation"
    reports_dir.mkdir(parents=True, exist_ok=True)
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
    report_df.to_csv(report_path, index=False, encoding="utf-8")

    return {
        "csv_path": csv_path,
        "rows": total_rows,
        "report_path": report_path,
        "alerts": alerts,
        "report": report_df,
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
        print(f"Relatorio salvo em: {result['report_path']}")


if __name__ == "__main__":
    main()
