"""Output generation for the Shopify compatible CSV files."""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, cast

import pandas as pd

from ..config import Settings
from .models import CatalogProduct, MatchDecision, NFEItem, Suggestion, UnmatchedItem
from .utils import clean_multiline_text, gtin_is_valid, normalize_barcode, round_money, slugify
from .text_splitters import (
    DEFAULT_USAGE_MARKERS,
    USAGE_STRONG_LABELS,
    normalise_markers,
    split_usage_from_text,
    starts_with_strong_label,
    usage_score,
)


LOGGER = logging.getLogger(__name__)

SIZE_TOKEN_SET = {
    'pp', 'p', 'm', 'g', 'gg', 'xg', 'xp', 'xxg', 'eg', 'xl', 'xxl',
}
SIZE_NUMERIC_RANGE = set(range(30, 61))
SIZE_COMPACT_SET = {token.replace(' ', '').replace('-', '') for token in SIZE_TOKEN_SET}
COLOR_TOKEN_SET = {
    'preto', 'branco', 'azul', 'verde', 'vermelho', 'amarelo', 'rosa', 'roxo', 'prata', 'dourado', 'marrom', 'cinza', 'bege', 'cobre', 'grafite', 'laranja', 'marfim', 'vinho', 'lilas', 'turquesa', 'off white', 'off-white', 'branco gelo', 'branco neve', 'chumbo', 'preto fosco', 'preto brilho',
}
CAPACITY_UNITS = ('ml', 'l', 'litro', 'litros', 'g', 'kg')

SIZE_TOKEN_CHOICES = tuple(sorted({token.upper() for token in SIZE_TOKEN_SET}, key=len, reverse=True))
SIZE_TOKEN_PATTERN = re.compile(r'(?:^|[-_\s])(' + '|'.join(SIZE_TOKEN_CHOICES) + r')(?:$|[-_\s])', re.IGNORECASE)
SIZE_NUMERIC_PATTERN = re.compile(r'(?:^|[-_\s])(\d{2})(?:$|[-_\s])')
CAPACITY_PATTERN = re.compile(r'(\d+(?:[.,]\d+)?)\s*(ml|l|litros?)', re.IGNORECASE)
WEIGHT_PATTERN = re.compile(r'(\d+(?:[.,]\d+)?)\s*(g|kg)', re.IGNORECASE)
BODY_USAGE_PREFIXES = (
    'limpe o produto',
    'use um pano',
    'manter o produto',
)

SIZE_TOKENS = {"PP", "P", "M", "G", "GG"}
DIM_PATTERN = re.compile(r"\b(\d{1,3})\s*[xX]\s*(\d{1,3})(?:\s*[xX]\s*(\d{1,3}))?\b")
MODEL_ALPHA = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
MODEL_NUM = re.compile(r"^\d{1,3}$")
CAP_PATTERN = re.compile(r"\b(\d{1,4})\s*(ml|l|litros)\b", re.IGNORECASE)
WGT_PATTERN = re.compile(r"\b(\d{1,4})\s*(g|kg)\b", re.IGNORECASE)


def _last_token(token: str) -> str:
    text = (token or "").strip()
    for separator in ("-", "–", "_", " "):
        if separator in text:
            text = text.split(separator)[-1].strip()
    return text


def _extract_size(token: str) -> str | None:
    candidate = (token or "").strip().upper()
    return candidate if candidate in SIZE_TOKENS else None


def _extract_dim(token: str) -> str | None:
    match = DIM_PATTERN.search(token or "")
    if not match:
        return None
    normalized: List[str] = []
    for part in match.groups():
        if not part:
            continue
        try:
            normalized.append(str(int(part)))
        except ValueError:
            cleaned = part.lstrip("0")
            normalized.append(cleaned or "0")
    return "x".join(normalized) if normalized else None


def _extract_model(token: str) -> str | None:
    candidate = (token or "").strip().upper()
    if len(candidate) == 1 and candidate in MODEL_ALPHA:
        return candidate
    if MODEL_NUM.match(candidate):
        return candidate
    return None


def _extract_capacity_or_weight(token: str) -> tuple[str, str] | None:
    match = CAP_PATTERN.search(token or "")
    if match:
        unit = match.group(2).lower()
        normalized = f"{match.group(1)} ml" if unit == "ml" else f"{match.group(1)} L"
        return "Capacidade", normalized
    match = WGT_PATTERN.search(token or "")
    if match:
        unit = match.group(2).lower()
        normalized = f"{match.group(1)} g" if unit == "g" else f"{match.group(1)} kg"
        return "Peso", normalized
    return None


SHOPIFY_HEADER = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Price",
    "Variant Compare At Price",
    "Variant Inventory Qty",
    "Variant Weight",
    "Variant Weight Unit",
    "Variant Requires Shipping",
    "Image Src",
    "Variant Barcode",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "product.metafields.custom.unidade",
    "product.metafields.custom.catalogo",
    "product.metafields.custom.dimensoes_do_produto",
    "product.metafields.custom.composicao",
    "product.metafields.custom.capacidade",
    "product.metafields.custom.modo_de_uso",
    "product.metafields.custom.icms",
    "product.metafields.custom.ncm",
    "product.metafields.custom.pis",
    "product.metafields.custom.ipi",
    "product.metafields.custom.cofins",
    "product.metafields.custom.componente_de_kit",
    "product.metafields.custom.resistencia_a_agua",
    "Variant Taxable",
    "Cost per item",
    "Image Position",
    "Variant Image",
    "Product Category",
    "Type",
    "Collection",
    "Status",
]


REQUIRED_FIELDS = [
    "Handle",
    "Title",
    "Vendor",
    "Variant SKU",
    "Variant Price",
    "Variant Inventory Qty",
    "Variant Weight",
    "Variant Weight Unit",
]

ImageResolver = Callable[[CatalogProduct], Optional[str]]


class CSVGenerator:
    def _validate_header(self) -> None:
        configured = list(self.settings.csv_output.columns)
        if configured != SHOPIFY_HEADER:
            raise ValueError("Configured csv_output.columns does not match the Shopify template header")


    def _validate_required_fields(self, dataframe: pd.DataFrame) -> None:
        if "Handle" not in dataframe.columns:
            raise ValueError("Generated CSV missing 'Handle' column")
        handles = dataframe["Handle"].astype(str).str.strip()
        missing: Dict[str, List[str]] = {}

        def record(field: str, mask) -> None:
            if mask.any():
                missing[field] = sorted(set(handles[mask]))

        for field in REQUIRED_FIELDS:
            if field not in dataframe.columns:
                missing[field] = sorted(set(handles))
                continue
            series = dataframe[field]
            if field == "Variant Weight":
                numeric = pd.to_numeric(series, errors="coerce")
                mask = numeric.isna() | (numeric <= 0)
            elif field == "Variant Weight Unit":
                mask = ~series.astype(str).str.strip().str.lower().isin({"g", "kg"})
            else:
                mask = series.astype(str).str.strip().str.lower().isin({"", "nan", "none", "null"})
            record(field, mask)

        if missing:
            details = '; '.join(f"{field}: {', '.join(values)}" for field, values in missing.items())
            raise ValueError(f"Missing required fields in generated CSV -> {details}")

    def __init__(self, settings: Settings, image_resolver: Optional[ImageResolver] = None) -> None:
        self.settings = settings
        self._validate_header()
        self.image_resolver = image_resolver or (lambda product: None)
        self.default_status = "active"

    def generate(
        self,
        matched: Iterable[MatchDecision],
        unmatched: Iterable[UnmatchedItem],
        run_id: str,
    ) -> Tuple[Path, Optional[Path], pd.DataFrame, pd.DataFrame]:
        dataframe = self._build_dataframe(matched)
        self._validate_required_fields(dataframe)
        csv_path = self._write_dataframe(dataframe, run_id)
        pendings_df = self._build_pendings(unmatched)
        pendings_path = self._write_pendings(pendings_df, run_id)
        return csv_path, pendings_path, dataframe, pendings_df

    def _write_dataframe(self, dataframe: pd.DataFrame, run_id: str) -> Path:
        output_folder = self.settings.paths.output_folder
        output_folder.mkdir(parents=True, exist_ok=True)
        filename = f"{self.settings.csv_output.filename_prefix}{run_id}.csv"
        path = output_folder / filename
        dataframe.to_csv(path, index=False, sep=self.settings.csv_output.delimiter, encoding="utf-8-sig")
        LOGGER.info("Wrote %s rows to %s", len(dataframe), path)
        return path

    def _write_pendings(self, dataframe: pd.DataFrame, run_id: str) -> Optional[Path]:
        if dataframe.empty:
            return None
        folder = self.settings.paths.pendings_folder or self.settings.paths.output_folder
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"pendencias_{run_id}.csv"
        dataframe.to_csv(path, index=False, encoding="utf-8-sig")
        LOGGER.info("Wrote pending reconciliation file with %s items to %s", len(dataframe), path)
        return path

    def _build_dataframe(self, matches: Iterable[MatchDecision]) -> pd.DataFrame:
        rows: Dict[str, Dict[str, object]] = {}
        for decision in matches:
            product = decision.product
            item = decision.item
            sku = product.sku
            row = rows.setdefault(sku, self._base_row(product))

            row.setdefault("_total_qty", 0.0)
            row.setdefault("_total_cost", 0.0)
            row.setdefault("_cfops", set())
            row.setdefault("_ncm", set())
            row.setdefault("_cest", set())
            row.setdefault("_units", set())
            # capture item-level notes/description to enrich Body (HTML)
            if not row.get("_infAdProd") and isinstance(item.additional_data, dict):
                info = item.additional_data.get("infAdProd")
                if info:
                    row["_infAdProd"] = str(info)

            row["_total_qty"] += item.quantity
            row["_total_cost"] += item.quantity * item.unit_value
            if item.cfop:
                row["_cfops"].add(item.cfop)
            if item.ncm:
                row["_ncm"].add(item.ncm)
            if item.cest:
                row["_cest"].add(item.cest)
            if item.unit:
                row["_units"].add(item.unit)

        for sku, row in rows.items():
            quantity = row.pop("_total_qty", 0.0)
            total_cost = row.pop("_total_cost", 0.0)
            cost_per_item = total_cost / quantity if quantity else 0.0

            # Inventory and pricing
            # - Inventory Qty as integer when possible (Shopify prefers integer here)
            inv_qty_int = int(round(quantity)) if abs(quantity - round(quantity)) < 1e-9 else quantity
            row["Inventory Qty"] = inv_qty_int
            row["Cost per item"] = round_money(cost_per_item)
            row["Price"] = self._compute_price(row, cost_per_item)

            cfops = row.pop("_cfops", set())
            ncms = row.pop("_ncm", set())
            cests = row.pop("_cest", set())
            units = row.pop("_units", set())

            self._fill_metafields(row, cfops=cfops, ncms=ncms, cests=cests, units=units)


            # Shopify Variant template defaults and mapping
            # Options start empty; inference happens after aggregation
            row.setdefault("Option1 Name", "")
            row.setdefault("Option1 Value", "")
            row.setdefault("Option2 Name", "")
            row.setdefault("Option2 Value", "")
            row.setdefault("Option3 Name", "")
            row.setdefault("Option3 Value", "")

            # Inventory tracker/policy/fulfillment
            row["Variant Inventory Tracker"] = "shopify"
            row.setdefault("Variant Inventory Policy", "deny")
            row.setdefault("Variant Fulfillment Service", "manual")

            # Shipping and weight
            self._apply_weight_fields(row)

            # Taxable by default (unless explicitly set elsewhere)
            row.setdefault("Variant Taxable", "TRUE")

            # Tags: include category (product_type) plus existing tags without códigos internos
            product_type = row.get("Product Type") or ""
            tags = []
            if isinstance(row.get("Tags"), str) and row["Tags"].strip():
                tags = [t.strip() for t in str(row["Tags"]).split(",") if t.strip()]
            if product_type and product_type not in tags:
                tags.insert(0, product_type)
            row["Tags"] = ",".join(self._sanitize_tags(tags))

            # Body (HTML): prefer catalogue description (textos) when available
            if not row.get("Body (HTML)"):
                description = self._clean_text(row.get("_description") or "")
                composition_raw = self._clean_text(row.get("composition") or row.get("composicao") or "")
                inf_ad = self._clean_text(row.get("_infAdProd") or "")
                composition_meta_value = ""
                composition_key = self.settings.metafields.keys.get("composicao")
                if composition_key:
                    meta_column = f"product.metafields.{self.settings.metafields.namespace}.{composition_key}"
                    composition_meta_value = self._clean_text(row.get(meta_column) or "")
                composition_body = composition_raw if (composition_raw and not composition_meta_value) else ""
                parts = [p for p in [description, composition_body, inf_ad] if p]
                if parts:
                    row["Body (HTML)"] = "\n\n".join(parts)

            row["Body (HTML)"] = self._finalize_body(row)

            # Shopify taxonomy must be inferred via tags; keep explicit fields empty
            row["Product Category"] = ""
            row["Type"] = ""
            row["Collection"] = ""

            row["Status"] = self._determine_status(row)

            # cleanup helper-only fields
            row.pop("_features", None)
            row.pop("_description", None)
            row.pop("_infAdProd", None)
            row.pop("composition", None)
            row.pop("_status_override", None)
            row.pop("_create_as_draft", None)

        dataframe = pd.DataFrame(rows.values())

        # Map internal canonical columns to Shopify Variant template columns when requested
        # This enables producing CSVs aligned with a provided header (e.g., example template)
        column_map: Dict[str, str] = {
            "SKU": "Variant SKU",
            "Price": "Variant Price",
            "Compare At Price": "Variant Compare At Price",
            "Inventory Qty": "Variant Inventory Qty",
            "Barcode": "Variant Barcode",
            # Keep 'Type' empty by default as per Shopify template used
        }
        for src, dst in column_map.items():
            if src in dataframe.columns:
                dataframe[dst] = dataframe[src]

        # Ensure all expected output columns exist (CSV + metafields) without duplicates
        expected_csv_columns = list(self.settings.csv_output.columns)
        expected_meta_columns = self._metafield_columns()
        expected_columns: List[str] = []
        for col in [*expected_csv_columns, *expected_meta_columns]:
            if col not in expected_columns:
                expected_columns.append(col)
        for column in expected_columns:
            if column not in dataframe.columns:
                dataframe[column] = ""

        # Robust reindex to avoid KeyError even when some columns are missing
        dataframe = dataframe.reindex(columns=expected_columns, fill_value="")
        df = dataframe.fillna("")
        df = self._apply_option_axes(df)

        # Ensure Option1 values are unique per Handle when multiple rows exist.
        if "Handle" in df.columns and "Option1 Value" in df.columns:
            for handle, grp in df.groupby("Handle"):
                if len(grp) <= 1:
                    continue
                seen_counts: Dict[str, int] = {}
                for idx, val in grp["Option1 Value"].items():
                    base = str(val).strip() if val is not None else ""
                    if not base:
                        continue
                    key = base.casefold()
                    n = seen_counts.get(key, 0) + 1
                    seen_counts[key] = n
                    new_val = base if n == 1 else f"{base}-{n}"
                    df.at[idx, "Option1 Value"] = new_val

        return df

    def _base_row(self, product: CatalogProduct) -> Dict[str, object]:
        collection_value = product.collection or ""
        if isinstance(collection_value, str):
            collection_value = collection_value.strip()
            if collection_value.lower() == "nan":
                collection_value = ""
        product_category = None
        if product.extra:
            product_category = product.extra.get("product_category")
        if isinstance(product_category, str):
            product_category = product_category.strip()
            if product_category.lower() == "nan":
                product_category = None
        if not product_category:
            product_category = product.product_type or ""
        product_type_value = product.product_type or ""
        type_value = ""
        if product.extra:
            raw_type = product.extra.get("subcateg")
            if isinstance(raw_type, str) and raw_type.strip().lower() != "nan":
                type_value = raw_type.strip()
        row: Dict[str, object] = {
            "Handle": slugify(product.title or product.sku),
            "Title": self._refine_title(product.title),
            "Vendor": product.vendor or self.settings.default_vendor or "",
            "Product Type": product_type_value,
            "SKU": product.sku,
            "Barcode": self._valid_barcode(product.barcode),
            "Status": self._default_status(),
            "Published": "TRUE",
            # Tags are finalised after aggregation to include category
            "Tags": ",".join(product.tags) if product.tags else "",
            "Collection": (collection_value or product_type_value or ""),
            "Product Category": product_category,
            "Type": type_value,
            "Compare At Price": "",
            "Weight": product.weight or "",
            "Image Src": self.image_resolver(product) or "",
            # Shopify Variant defaults (filled/confirmed later)
            "Variant Inventory Policy": "deny",
            "Variant Fulfillment Service": "manual",
            "Variant Requires Shipping": "TRUE",
            "Variant Taxable": "TRUE",
        }
        self._apply_variant_options(row, product)
        if product.metafields:
            row["composition"] = self._clean_text(product.metafields.get("composition", ""))
        self._apply_catalog_details(row, product)
        if product.extra:
            # Dynamic metafields mapping from catalogue extra columns
            dm = getattr(self.settings.metafields, "dynamic_mapping", None)
            if dm and dm.enabled and isinstance(dm.map, dict):
                ns = self.settings.metafields.namespace
                for meta_key, col in dm.map.items():
                    if not col:
                        continue
                    val = product.extra.get(col) or product.extra.get(str(col).lower())
                    if val is None:
                        continue
                    text = str(val).strip()
                    if text and text.lower() != "nan":
                        row[f"product.metafields.{ns}.{meta_key}"] = text
            if "status" in product.extra:
                row["_status_override"] = product.extra["status"]
            if "create_as_draft" in product.extra:
                row["_create_as_draft"] = product.extra["create_as_draft"]
        return row

    @staticmethod
    def _clean_text(value: Optional[str]) -> str:
        return clean_multiline_text(value)

    @staticmethod
    def _normalise_dynamic_value(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return clean_multiline_text(value)
        if isinstance(value, (int, float)):
            if isinstance(value, float) and pd.isna(value):
                return ""
            return CSVGenerator._format_number(float(value))
        return str(value).strip()

    @staticmethod
    def _format_number(value: float, max_decimals: int = 3) -> str:
        formatted = f"{value:.{max_decimals}f}"
        if "." in formatted:
            formatted = formatted.rstrip("0").rstrip(".")
        return formatted or "0"

    def _parse_weight(self, value: object, unit_hint: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        unit = (unit_hint or "").strip().lower()
        numeric: Optional[float]
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            lower = text.lower()
            if lower.endswith("kg"):
                unit = unit or "kg"
                text = lower[:-2]
            elif lower.endswith("g"):
                unit = unit or "g"
                text = lower[:-1]
            text = re.sub(r"[^0-9,.-]", "", text)
            if not text:
                return None
            text = text.replace(",", ".")
            try:
                numeric = float(text)
            except ValueError:
                return None
        elif isinstance(value, (int, float)):
            if isinstance(value, float) and pd.isna(value):
                return None
            numeric = float(value)
        else:
            return None
        if numeric <= 0:
            return None
        if unit in {"g", "grama", "gramas"}:
            return numeric / 1000.0
        return numeric


    def _default_status(self) -> str:
        export_cfg = getattr(self.settings, "export", None)
        if export_cfg and getattr(export_cfg, "status", None):
            return export_cfg.status
        return "draft"

    def _determine_status(self, row: Dict[str, object]) -> str:
        default_status = self._default_status()
        override = row.get("_status_override")
        if isinstance(override, str) and override.strip():
            normalized = override.strip().lower()
            if normalized in {"draft", "active"}:
                return normalized
        elif override is not None:
            normalized = str(override).strip().lower()
            if normalized in {"draft", "active"}:
                return normalized

        draft_flag = self._coerce_bool(row.get("_create_as_draft"))
        if draft_flag is True:
            return "draft"
        return default_status

    def _apply_weight_fields(self, row: Dict[str, object]) -> None:
        weight = self._coerce_float(row.get("Weight"))
        if weight is None or weight <= 0:
            row.setdefault("Variant Requires Shipping", "TRUE")
            row.pop("Variant Weight", None)
            row.pop("Variant Weight Unit", None)
            row["Variant Grams"] = ""
            return

        grams = int(round(weight * 1000))
        row["Variant Grams"] = str(grams)
        row["Variant Requires Shipping"] = "TRUE"
        if weight < 1.0:
            row["Variant Weight Unit"] = "g"
            row["Variant Weight"] = str(grams)
        else:
            row["Variant Weight Unit"] = "kg"
            row["Variant Weight"] = self._format_decimal(weight)
        row["Weight"] = self._format_decimal(weight)

    def _sanitize_tags(self, tags: List[str]) -> List[str]:
        config = getattr(self.settings, "tags", None)
        drop_short = bool(config and config.drop_short_codes)
        min_alpha = config.min_alpha_len if config else 3
        seen: set[str] = set()
        cleaned: List[str] = []
        for tag in tags:
            if not isinstance(tag, str):
                continue
            text = " ".join(tag.split()).strip()
            if not text:
                continue
            if drop_short:
                alpha_count = sum(1 for ch in text if ch.isalpha())
                if alpha_count < min_alpha:
                    continue
            if text not in seen:
                seen.add(text)
                cleaned.append(text)
        return cleaned

    def _composition_column(self) -> Optional[str]:
        key = self.settings.metafields.keys.get("composicao")
        if not key:
            return None
        namespace = self.settings.metafields.namespace
        return f"product.metafields.{namespace}.{key}"

    def _finalize_body(self, row: Dict[str, object]) -> str:
        body_raw = row.get("Body (HTML)") or ""
        body = self._clean_text(body_raw)
        comp_column = self._composition_column()
        composition_value = ""
        if comp_column and isinstance(row.get(comp_column), str):
            composition_value = row.get(comp_column, "")
        if not composition_value and isinstance(row.get("composition"), str):
            composition_value = row.get("composition", "")
        if composition_value:
            body = self._remove_composition(body, composition_value)
        if body:
            body = self._strip_usage_prefix_paragraphs(body)
        return body


    def _strip_usage_prefix_paragraphs(self, body: str) -> str:
        markers = normalise_markers(DEFAULT_USAGE_MARKERS)
        segments = [segment.strip() for segment in re.split(r"\n{2,}", body) if segment.strip()]
        if not segments:
            return ""

        cleaned: List[str] = []
        for segment in segments:
            lowered = segment.casefold()
            prefix_match = self._starts_with_usage_prefix(lowered)
            score = usage_score(segment, markers)
            if prefix_match:
                score += 1
            if (
                prefix_match
                and len(segment) <= 240
                and score >= 2
                and not starts_with_strong_label(segment, USAGE_STRONG_LABELS)
            ):
                continue
            cleaned.append(segment)

        return "\n\n".join(cleaned)

    @staticmethod
    def _starts_with_usage_prefix(text: str) -> bool:
        stripped = text.lstrip()
        return any(stripped.startswith(prefix) for prefix in BODY_USAGE_PREFIXES)

    def _remove_composition(self, body: str, composition: str) -> str:
        comp_clean = self._clean_text(composition)
        if not comp_clean:
            return body
        normalized_comp = self._normalize_for_compare(comp_clean)
        if not normalized_comp:
            return body
        segments = [seg.strip() for seg in re.split(r"\n{2,}", body) if seg.strip()]
        kept = [seg for seg in segments if self._normalize_for_compare(seg) != normalized_comp]
        if kept and len(kept) != len(segments):
            return "\n\n".join(kept)
        lines = [line for line in (part.strip() for part in body.split("\n")) if line]
        filtered_lines = [line for line in lines if self._normalize_for_compare(line) != normalized_comp]
        if filtered_lines and len(filtered_lines) != len(lines):
            return "\n".join(filtered_lines)
        pattern = re.compile(re.escape(comp_clean), re.IGNORECASE)
        cleaned = pattern.sub("", body).strip()
        return self._clean_text(cleaned)

    @staticmethod
    def _normalize_for_compare(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value)
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip().lower()

    def _apply_variant_options(self, row: Dict[str, object], product: CatalogProduct) -> None:
        variants_cfg = getattr(self.settings, "variants", None)
        option_mappings = [
            ("Option1 Name", "Option1 Value", getattr(variants_cfg, "option1", None) if variants_cfg else None),
            ("Option2 Name", "Option2 Value", getattr(variants_cfg, "option2", None) if variants_cfg else None),
            ("Option3 Name", "Option3 Value", None),
        ]

        if not variants_cfg or not variants_cfg.enabled:
            self._clear_option_fields(row)
            return

        variant_axes: List[Tuple[str, str]] = []
        for name_key, value_key, option_cfg in option_mappings:
            raw_value = ""
            axis_name = ""
            if option_cfg is not None:
                value = self._get_variant_option_value(product, option_cfg)
                raw_value = value.strip() if value else ""
                axis_name = (option_cfg.name or "").strip()
            if axis_name.lower() == "title":
                axis_name = ""

            if raw_value:
                inferred_name = axis_name or self._infer_option_name_from_value(raw_value)
                if inferred_name:
                    row[name_key] = inferred_name
                    row[value_key] = raw_value
                    variant_axes.append((name_key, value_key))
                else:
                    row[name_key] = ""
                    row[value_key] = ""
            else:
                row[name_key] = ""
                row[value_key] = ""

        if not variant_axes:
            self._clear_option_fields(row)


    @staticmethod
    def _clear_option_fields(row: Dict[str, object]) -> None:
        for idx in range(1, 4):
            row[f"Option{idx} Name"] = ""
            row[f"Option{idx} Value"] = ""

    def _infer_option_name_from_value(self, value: str) -> str:
        text = (value or "").strip()
        if not text:
            return ""
        lowered = text.casefold()
        compact = re.sub(r"[ .-]", "", lowered)
        if lowered in SIZE_TOKEN_SET or compact in SIZE_COMPACT_SET:
            return "Tamanho"
        if lowered.isdigit():
            try:
                numeric = int(lowered)
            except ValueError:
                numeric = None
            else:
                if numeric in SIZE_NUMERIC_RANGE:
                    return "Tamanho"
        if self._looks_like_capacity(lowered):
            return "Capacidade"
        if lowered in COLOR_TOKEN_SET:
            return "Cor"
        return ""

    @staticmethod
    def _looks_like_capacity(value: str) -> bool:
        compact = value.replace(" ", "")
        for unit in CAPACITY_UNITS:
            if compact.endswith(unit):
                number = compact[: -len(unit)]
                number = number.replace(',', '.').strip()
                if not number:
                    continue
                try:
                    float(number)
                except ValueError:
                    continue
                return True
        return False


    def _get_variant_option_value(self, product: CatalogProduct, option_cfg) -> str:
        column = getattr(option_cfg, "column", None)
        if not column:
            return ""
        candidates = {
            column,
            str(column).lower(),
            str(column).replace(" ", "_").lower(),
            str(column).replace("-", "_").lower(),
        }
        for key in list(candidates):
            candidates.add(key.replace("__", "_"))

        for key in candidates:
            if key in product.extra:
                value = product.extra.get(key)
                if value is None:
                    continue
                text = str(value).strip()
                if text and text.lower() != "nan":
                    return self._clean_text(text)
        return ""

    def _apply_default_metafield_values(self, row: Dict[str, object]) -> None:
        for logical in ("icms", "ipi", "pis", "cofins"):
            value = row.get(logical)
            if value is None or (isinstance(value, str) and not value.strip()):
                row[logical] = "0"
            else:
                text = str(value).strip()
                row[logical] = text or "0"

        kit_flag = self._coerce_bool(row.get("componente_de_kit"))
        row["componente_de_kit"] = "TRUE" if kit_flag else "FALSE"

        resistencia = row.get("resistencia_a_agua")
        if isinstance(resistencia, str):
            resistencia = resistencia.strip()
        elif resistencia is None:
            resistencia = ""
        else:
            resistencia = str(resistencia).strip()
        row["resistencia_a_agua"] = resistencia or "Não se aplica"

    @staticmethod
    def _coerce_bool(value: object) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "sim"}:
                return True
            if normalized in {"false", "0", "no", "nao", "não"}:
                return False
        return None

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        if isinstance(value, str):
            try:
                return float(value.replace(",", "."))
            except ValueError:
                return None
        return None

    @staticmethod
    def _format_decimal(value: float) -> str:
        return f"{value:.3f}".rstrip("0").rstrip(".")

    @staticmethod
    def _refine_title(title: Optional[str]) -> str:
        """Refine product title for CSV output.

        Rules:
        - first alphabetical character uppercase, all others lowercase;
        - if there's a hyphen ('-'), uppercase the next alphabetical character
          after the hyphen (brand-title style: "Marca - Produto").
        """
        if not title:
            return ""
        s = str(title).strip().lower()
        result_chars = []
        capitalize_next = True
        for ch in s:
            if ch.isalpha() and capitalize_next:
                result_chars.append(ch.upper())
                capitalize_next = False
            else:
                result_chars.append(ch)
            if ch == '-':
                capitalize_next = True
        return "".join(result_chars)

    def _compute_price(self, row: Dict[str, object], cost_per_item: float) -> object:
        strategy = self.settings.pricing.strategy
        if strategy == "somente_custo":
            return ""
        if strategy == "tabela":
            price = row.get("Price")
            if price:
                return price
            return ""
        markup = self.settings.pricing.markup_factor
        price = cost_per_item * markup
        return round_money(price)

    def _fill_metafields(self, row: Dict[str, object], *, cfops: Iterable[str], ncms: Iterable[str], cests: Iterable[str], units: Iterable[str]) -> None:
        self._apply_default_metafield_values(row)
        metafield_columns = self.settings.metafields.keys
        namespace = self.settings.metafields.namespace
        for logical, key in metafield_columns.items():
            column = f"product.metafields.{namespace}.{key}"
            if logical == "cfop":
                value = ";".join(sorted(cfops))
            elif logical == "ncm":
                value = ";".join(sorted(ncms)) or row.get(logical, "")
            elif logical == "cest":
                value = ";".join(sorted(cests)) or row.get(logical, "")
            elif logical == "unidade":
                value = row.get(logical) or ";".join(sorted(units))
            elif logical == "composicao":
                value = row.get(logical) or row.get("composition") or ""
            else:
                value = row.get(logical, "")
            if isinstance(value, str):
                value = self._clean_text(value)
            row[column] = value

    def _metafield_columns(self) -> List[str]:
        namespace = self.settings.metafields.namespace
        return [f"product.metafields.{namespace}.{key}" for key in self.settings.metafields.keys.values()]


    def _apply_option_axes(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Handle" not in df.columns or "Option1 Name" not in df.columns or "Option1 Value" not in df.columns:
            return df

        option_columns = [
            "Option1 Name",
            "Option1 Value",
            "Option2 Name",
            "Option2 Value",
            "Option3 Name",
            "Option3 Value",
        ]
        for column in option_columns:
            if column not in df.columns:
                df[column] = ""
        df[option_columns] = ""

        def _coerce_text(idx: int, key: str) -> str:
            if key not in df.columns:
                return ""
            raw = df.at[idx, key]
            if isinstance(raw, str):
                candidate = raw.strip()
            else:
                if pd.isna(raw):
                    return ""
                candidate = str(raw).strip()
            if not candidate or candidate.casefold() == "nan":
                return ""
            return candidate

        def _variant_position(idx: int) -> int:
            for key in ("Variant Position", "Variant Pos"):
                candidate = _coerce_text(idx, key)
                if candidate:
                    normalized = candidate.replace(",", ".")
                    try:
                        return int(float(normalized))
                    except (TypeError, ValueError):
                        continue
            return 0

        def _variant_sku(idx: int) -> str:
            sku = _coerce_text(idx, "Variant SKU")
            if sku:
                return sku
            return _coerce_text(idx, "SKU")

        for handle, group in df.groupby("Handle", sort=False):
            indices = list(group.index)
            sorted_indices = sorted(
                indices,
                key=lambda idx: (_variant_position(idx), _variant_sku(idx)),
            )

            if len(sorted_indices) < 2:
                for idx in sorted_indices:
                    df.at[idx, "Option1 Name"] = ""
                    df.at[idx, "Option1 Value"] = ""
                    df.at[idx, "Option2 Name"] = ""
                    df.at[idx, "Option2 Value"] = ""
                    df.at[idx, "Option3 Name"] = ""
                    df.at[idx, "Option3 Value"] = ""
                continue

            tokens_per_variant: List[str] = []
            for idx in sorted_indices:
                token = (
                    _coerce_text(idx, "Variant SKU")
                    or _coerce_text(idx, "SKU")
                    or _coerce_text(idx, "Title")
                )
                tokens_per_variant.append(token)

            axis: Optional[str] = None
            values: Optional[List[str]] = None

            sizes = [
                _extract_size(_last_token(token)) or _extract_size(token)
                for token in tokens_per_variant
            ]
            if all(sizes) and len(set(sizes)) > 1:
                axis = "Tamanho"
                values = [cast(str, size) for size in sizes]
            else:
                dims = [_extract_dim(token) for token in tokens_per_variant]
                if all(dims) and len(set(dims)) > 1:
                    axis = "Formato"
                    values = [cast(str, dimension) for dimension in dims]
                else:
                    models = [
                        _extract_model(_last_token(token)) or _extract_model(token)
                        for token in tokens_per_variant
                    ]
                    if all(models) and len(set(models)) > 1 and (
                        all(len(model) == 1 for model in models if model)
                        or all(model.isdigit() for model in models if model)
                    ):
                        axis = "Modelo"
                        values = [cast(str, model) for model in models]
                    else:
                        capw_candidates = [_extract_capacity_or_weight(token) for token in tokens_per_variant]
                        capw_pairs = [pair for pair in capw_candidates if pair]
                        if (
                            len(capw_pairs) == len(tokens_per_variant)
                            and len({key for key, _ in capw_pairs}) == 1
                            and len({value for _, value in capw_pairs}) > 1
                        ):
                            axis = capw_pairs[0][0]
                            values = [value for _, value in capw_pairs]

            for position, idx in enumerate(sorted_indices):
                df.at[idx, "Option2 Name"] = ""
                df.at[idx, "Option2 Value"] = ""
                df.at[idx, "Option3 Name"] = ""
                df.at[idx, "Option3 Value"] = ""
                if axis and values:
                    df.at[idx, "Option1 Name"] = axis
                    df.at[idx, "Option1 Value"] = str(values[position])
                else:
                    df.at[idx, "Option1 Name"] = ""
                    df.at[idx, "Option1 Value"] = ""

                name_text = str(df.at[idx, "Option1 Name"]).strip().lower()
                if name_text == "title":
                    df.at[idx, "Option1 Name"] = ""
                value_text = str(df.at[idx, "Option1 Value"]).strip().lower()
                if value_text == "default title":
                    df.at[idx, "Option1 Value"] = ""

            if not axis and handle and len(sorted_indices) > 1:
                LOGGER.info("Option axis not inferred for handle %s; leaving options blank", handle)

        if "Option1 Name" in df.columns:
            name_series = df["Option1 Name"].fillna("")
            mask_title_name = name_series.str.strip().str.casefold() == "title"
            if mask_title_name.any():
                df.loc[mask_title_name, "Option1 Name"] = ""

        if "Option1 Value" in df.columns:
            option_series = df["Option1 Value"].fillna("")
            mask_default = option_series.str.casefold() == "default title"
            if mask_default.any():
                df.loc[mask_default, "Option1 Value"] = ""

        return df


    def _apply_catalog_details(self, row: Dict[str, object], product: CatalogProduct) -> None:
        extra = product.extra or {}
        text_settings = getattr(self.settings, "text_splitting", None)
        markers_override = None
        if text_settings and getattr(text_settings, "usage_markers", None):
            markers_override = list(text_settings.usage_markers)

        catalog_text = self._coerce_extra_text(extra.get("catalog_description"))
        features_text = self._coerce_extra_text(extra.get("features"))
        modo_source_text = self._coerce_extra_text(extra.get("modo_de_uso"))

        desc_cat, uso_cat = split_usage_from_text(catalog_text, markers_override)
        desc_feat, uso_feat = split_usage_from_text(features_text, markers_override)

        markers_for_scoring = normalise_markers(markers_override or DEFAULT_USAGE_MARKERS)

        catalog_has_text = bool(catalog_text and catalog_text.strip())
        desc_cat_clean = desc_cat.strip() if desc_cat and desc_cat.strip() else ""
        desc_feat_clean = desc_feat.strip() if desc_feat and desc_feat.strip() else ""

        desc_parts: List[str] = []
        if desc_cat_clean:
            desc_parts.append(desc_cat_clean)
        if desc_feat_clean and (not catalog_has_text or desc_cat_clean):
            desc_parts.append(desc_feat_clean)

        modo_clean = modo_source_text.strip() if modo_source_text and modo_source_text.strip() else ""
        uso_cat_clean = uso_cat.strip() if uso_cat and uso_cat.strip() else ""
        uso_feat_clean = uso_feat.strip() if uso_feat and uso_feat.strip() else ""

        uso_segments: List[Tuple[str, str]] = []
        if modo_clean:
            uso_segments.append(("modo", modo_clean))
        if uso_cat_clean:
            uso_segments.append(("catalog", uso_cat_clean))
        if uso_feat_clean:
            uso_segments.append(("features", uso_feat_clean))

        desc_final = " ".join(desc_parts).strip()
        uso_final = " ".join(segment for _, segment in uso_segments).strip()

        if not desc_final and uso_final:
            total_usage_score = usage_score(uso_final, markers_for_scoring)
            has_strong_heading = starts_with_strong_label(uso_final, USAGE_STRONG_LABELS)
            if total_usage_score <= 1 and not has_strong_heading:
                desc_final = uso_final
                uso_final = ""

        if desc_feat_clean:
            row["_features"] = desc_feat_clean
        else:
            row.pop("_features", None)

        if desc_final:
            row["_description"] = desc_final
        else:
            row.pop("_description", None)

        if uso_final:
            row["modo_de_uso"] = uso_final
        else:
            row.pop("modo_de_uso", None)
        unit_value = self._coerce_extra_text(product.unit or extra.get("unidade"))
        if unit_value:
            row["unidade"] = unit_value
        catalog_value = self._coerce_extra_text(extra.get("catalogo"))
        if catalog_value:
            row["catalogo"] = catalog_value
        capacity_source = extra.get("capacidade") or extra.get("capacidade__ml_ou_peso_suportado")
        capacity_value = self._coerce_extra_text(capacity_source)
        if capacity_value:
            row["capacidade"] = capacity_value
        resistance_value = self._coerce_extra_text(extra.get("resistencia_a_agua"))
        if resistance_value:
            row["resistencia_a_agua"] = resistance_value

        dimension_value = (
            extra.get("dimensoes_sem_embalagem")
            or extra.get("dimensoes_com_embalagem")
            or extra.get("medidas_s_emb")
            or extra.get("medidas_c_emb")
        )
        dims = self._normalize_dimensions(dimension_value)
        if dims:
            row["dimensoes_do_produto"] = dims
        for fiscal_key in ("icms", "ipi", "pis", "cofins"):
            formatted = self._format_catalog_number(extra.get(fiscal_key))
            if formatted:
                row[fiscal_key] = formatted

    def _coerce_extra_text(self, value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if isinstance(value, float) and pd.isna(value):
                return ""
            return self._format_number(float(value), max_decimals=2)
        text = self._clean_text(str(value))
        lowered = text.lower()
        if lowered in {"nan", "none", "null"}:
            return ""
        return text

    def _format_catalog_number(self, value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if isinstance(value, float) and pd.isna(value):
                return ""
            return self._format_number(float(value), max_decimals=2)
        text = self._clean_text(str(value))
        if not text:
            return ""
        lowered = text.lower()
        if lowered in {"nan", "none", "null"}:
            return ""
        sanitized = text.replace("%", "").replace(",", ".")
        try:
            return self._format_number(float(sanitized), max_decimals=2)
        except ValueError:
            return text

    def _normalize_dimensions(self, value: object) -> str:
        text = self._coerce_extra_text(value)
        if not text:
            return ""
        normalized = text.replace("\u00d7", "x").replace("X", "x")
        contains_cm = "cm" in normalized.lower()
        numeric_part = normalized.lower().replace("cm", "")
        numeric_part = re.sub(r"\s+", "", numeric_part)
        numeric_part = numeric_part.replace(",", ".")
        parts = [part for part in re.split(r"x", numeric_part) if part]
        if len(parts) == 3:
            formatted = " x ".join(part.strip() for part in parts)
            if contains_cm:
                formatted = f"{formatted} cm"
            return formatted
        return text

    def _build_tags(self, raw_tags: object, product_type: str) -> str:
        tags: List[str] = []
        seen = set()
        if isinstance(raw_tags, str):
            for candidate in raw_tags.split(','):
                tag = candidate.strip()
                if not tag:
                    continue
                normalized = tag.casefold()
                if normalized in {"nan", "none", "null"}:
                    continue
                if self._tag_looks_like_internal_code(tag):
                    continue
                if normalized not in seen:
                    tags.append(tag)
                    seen.add(normalized)
        if product_type:
            pt = product_type.strip()
            if pt:
                normalized_pt = pt.casefold()
                if normalized_pt not in seen:
                    tags.insert(0, pt)
                    seen.add(normalized_pt)
        return ','.join(tags)

    @staticmethod
    def _tag_looks_like_internal_code(tag: str) -> bool:
        candidate = tag.strip()
        if not candidate:
            return True
        if re.fullmatch(r'\dT\d{2}', candidate, flags=re.IGNORECASE):
            return True
        return False

    def _build_pendings(self, unmatched: Iterable[UnmatchedItem]) -> pd.DataFrame:
        records = []
        for pending in unmatched:
            item = pending.item
            record = {
                "invoice_key": item.invoice_key,
                "item_number": item.item_number,
                "cProd": item.sku,
                "description": item.description,
                "barcode": item.barcode,
                "ncm": item.ncm,
                "cest": item.cest,
                "cfop": item.cfop,
                "quantity": item.quantity,
                "unit_value": item.unit_value,
                "total_value": item.total_value,
                "reason": pending.reason or "",
                "suggestions": self._format_suggestions(pending.suggestions),
            }
            records.append(record)

        if not records:
            return pd.DataFrame(columns=[
                "invoice_key",
                "item_number",
                "cProd",
                "description",
                "barcode",
                "ncm",
                "cest",
                "cfop",
                "quantity",
                "unit_value",
                "total_value",
                "reason",
                "suggestions",
            ])

        return pd.DataFrame(records)

    @staticmethod
    def _format_suggestions(suggestions: Iterable[Suggestion]) -> str:
        formatted = []
        for suggestion in suggestions:
            formatted.append(
                f"{suggestion.product.sku} | {suggestion.product.title} | {suggestion.confidence:.2f}"
            )
        return "\n".join(formatted)

    @staticmethod
    def _valid_barcode(barcode: Optional[str]) -> str:
        normalized = normalize_barcode(barcode)
        if normalized and gtin_is_valid(normalized):
            return normalized
        if normalized:
            LOGGER.warning("Barcode %s failed GTIN validation", normalized)
        return ""


__all__ = ["CSVGenerator", "SHOPIFY_HEADER"]





