"""Output generation for the Shopify compatible CSV files."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from ..config import Settings
from .models import CatalogProduct, MatchDecision, NFEItem, Suggestion, UnmatchedItem
from .utils import clean_multiline_text, gtin_is_valid, normalize_barcode, round_money, slugify


LOGGER = logging.getLogger(__name__)

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
            # Option handling: single-variant default
            row.setdefault("Option1 Name", "Title")
            row.setdefault("Option1 Value", "Default Title")

            # Inventory tracker/policy/fulfillment
            row["Variant Inventory Tracker"] = "shopify"
            row.setdefault("Variant Inventory Policy", "deny")
            row.setdefault("Variant Fulfillment Service", "manual")

            # Shipping and weight
            weight_raw = row.pop("Weight", None)
            weight_unit_hint = row.pop("_weight_unit", None)
            weight_value = self._parse_weight(weight_raw, weight_unit_hint)
            if weight_value is not None:
                grams_value = weight_value * 1000.0
                if weight_value < 1.0:
                    row["Variant Weight"] = self._format_number(grams_value, max_decimals=0)
                    row["Variant Weight Unit"] = "g"
                else:
                    row["Variant Weight"] = self._format_number(weight_value)
                    row["Variant Weight Unit"] = "kg"
                row["Variant Grams"] = self._format_number(grams_value, max_decimals=0)
                row["Weight"] = self._format_number(weight_value)
                row.setdefault("Variant Requires Shipping", "TRUE")
            else:
                row["Variant Weight"] = ""
                row["Variant Weight Unit"] = ""
                row["Variant Grams"] = ""
                row.setdefault("Variant Requires Shipping", "TRUE")
            # Taxable by default (unless explicitly set elsewhere)
            row.setdefault("Variant Taxable", "TRUE")

            # Tags: include category (product_type) plus existing tags without códigos internos
            product_type = row.get("Product Type") or ""
            row["Tags"] = self._build_tags(row.get("Tags"), product_type)

            # Body (HTML): prefer catalogue description (textos) when available
            if not row.get("Body (HTML)"):
                description = self._clean_text(row.get("_description") or "")
                if not description:
                    description = self._clean_text(row.get("_features") or "")
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
            # cleanup helper-only fields
            row.pop("_features", None)
            row.pop("_description", None)
            row.pop("_infAdProd", None)

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

        # Ensure Option1 values are unique per Handle when multiple rows exist.
        if "Handle" in df.columns and "Option1 Value" in df.columns:
            for handle, grp in df.groupby("Handle"):
                if len(grp) <= 1:
                    continue
                seen_counts: Dict[str, int] = {}
                for idx, val in grp["Option1 Value"].items():
                    base = val or "Default Title"
                    n = seen_counts.get(base, 0) + 1
                    seen_counts[base] = n
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
            "Status": self.default_status,
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
        if product.ncm:
            row.setdefault("ncm", str(product.ncm).strip())
        if product.unit:
            row.setdefault("unidade", str(product.unit).strip())
        weight_unit_column = getattr(self.settings.weights, "unit_column", None)
        if product.extra and weight_unit_column:
            unit_column = str(weight_unit_column)
            unit_value = product.extra.get(unit_column) or product.extra.get(unit_column.lower())
            if unit_value:
                row["_weight_unit"] = str(unit_value).strip()
        if product.metafields:
            composition_value = self._clean_text(product.metafields.get("composition"))
            if composition_value:
                row["composition"] = composition_value
                row.setdefault("composicao", composition_value)
        features_value = product.extra.get("features") if product.extra else None
        if isinstance(features_value, str):
            cleaned_features = self._clean_text(features_value)
            if cleaned_features and cleaned_features.lower() != "nan":
                row["_features"] = cleaned_features
        description_value = product.extra.get("textos") if product.extra else None
        if isinstance(description_value, str):
            cleaned_description = self._clean_text(description_value)
            if cleaned_description and cleaned_description.lower() != "nan":
                row["_description"] = cleaned_description
        dm = getattr(self.settings.metafields, "dynamic_mapping", None)
        if dm and dm.enabled and isinstance(dm.map, dict):
            for logical_key, column in dm.map.items():
                if not column:
                    continue
                source = None
                if product.extra:
                    source = product.extra.get(column)
                    if source is None and isinstance(column, str):
                        source = product.extra.get(column.lower())
                if source is None and isinstance(column, str):
                    if hasattr(product, column):
                        source = getattr(product, column)
                    elif hasattr(product, column.lower()):
                        source = getattr(product, column.lower())
                normalised = self._normalise_dynamic_value(source)
                if normalised:
                    marker = normalised.strip().lower()
                    if marker not in {"", "nan", "none", "null"}:
                        row.setdefault(logical_key, normalised)
        if "ipi" not in row:
            ipi_value = None
            if product.extra and "ipi" in product.extra:
                ipi_value = product.extra.get("ipi")
            elif product.metafields and "ipi" in product.metafields:
                ipi_value = product.metafields.get("ipi")
            normalised_ipi = self._normalise_dynamic_value(ipi_value)
            if normalised_ipi:
                marker = normalised_ipi.strip().lower()
                if marker not in {"", "nan", "none", "null"}:
                    row["ipi"] = normalised_ipi
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





