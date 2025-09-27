"""Output generation for the Shopify compatible CSV files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from ..config import Settings
from .models import CatalogProduct, MatchDecision, NFEItem, Suggestion, UnmatchedItem
from .utils import gtin_is_valid, normalize_barcode, round_money, slugify


LOGGER = logging.getLogger(__name__)

ImageResolver = Callable[[CatalogProduct], Optional[str]]


class CSVGenerator:
    def __init__(self, settings: Settings, image_resolver: Optional[ImageResolver] = None) -> None:
        self.settings = settings
        self.image_resolver = image_resolver or (lambda product: None)
        self.default_status = "active"

    def generate(
        self,
        matched: Iterable[MatchDecision],
        unmatched: Iterable[UnmatchedItem],
        run_id: str,
    ) -> Tuple[Path, Optional[Path], pd.DataFrame, pd.DataFrame]:
        dataframe = self._build_dataframe(matched)
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
            if isinstance(inv_qty_int, (int, float)) and float(inv_qty_int) > 0:
                row["Variant Inventory Tracker"] = "shopify"
            row.setdefault("Variant Inventory Policy", "deny")
            row.setdefault("Variant Fulfillment Service", "manual")

            # Shipping and weight
            weight = row.get("Weight")
            if isinstance(weight, (int, float)) and float(weight) > 0:
                v = float(weight)
                # If < 1 kg, export as grams to match guidance
                if v < 1.0:
                    row["Variant Weight"] = round(v * 1000, 3)
                    row["Variant Weight Unit"] = "g"
                else:
                    row["Variant Weight"] = v
                    row.setdefault("Variant Weight Unit", "kg")
                row.setdefault("Variant Requires Shipping", "TRUE")
            else:
                row.setdefault("Variant Requires Shipping", "TRUE")

            # Taxable by default (unless explicitly set elsewhere)
            row.setdefault("Variant Taxable", "TRUE")

            # Tags: include category (product_type) plus existing tags
            product_type = row.get("Product Type") or ""
            tags = []
            if isinstance(row.get("Tags"), str) and row["Tags"].strip():
                tags = [t.strip() for t in str(row["Tags"]).split(",") if t.strip()]
            if product_type and product_type not in tags:
                tags.insert(0, product_type)
            row["Tags"] = ",".join(tags)

            # Body (HTML): prefer catalogue features or composition if present
            if not row.get("Body (HTML)"):
                features = self._clean_text(row.get("_features") or "")
                composition = self._clean_text(row.get("composition") or "")
                inf_ad = self._clean_text(row.get("_infAdProd") or "")
                parts = [p for p in [features, composition, inf_ad] if p]
                if parts:
                    row["Body (HTML)"] = "\n\n".join(parts)
            # cleanup helper-only fields
            row.pop("_features", None)
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
        row: Dict[str, object] = {
            "Handle": slugify(product.title or product.sku),
            "Title": self._refine_title(product.title),
            "Vendor": product.vendor or self.settings.default_vendor or "",
            "Product Type": product.product_type or "",
            "SKU": product.sku,
            "Barcode": self._valid_barcode(product.barcode),
            "Status": self.default_status,
            "Published": "TRUE",
            # Tags are finalised after aggregation to include category
            "Tags": ",".join(product.tags) if product.tags else "",
            "Compare At Price": "",
            "Weight": product.weight or "",
            "Image Src": self.image_resolver(product) or "",
            # Shopify Variant defaults (filled/confirmed later)
            "Variant Inventory Policy": "deny",
            "Variant Fulfillment Service": "manual",
            "Variant Requires Shipping": "TRUE",
            "Variant Taxable": "TRUE",
        }
        if product.metafields:
            row["composition"] = self._clean_text(product.metafields.get("composition", ""))
        if product.extra:
            row["_features"] = self._clean_text(product.extra.get("features", ""))
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
        return row

    @staticmethod
    def _clean_text(value: Optional[str]) -> str:
        if not value:
            return ""
        s = str(value).replace("_x000D_", " ")
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        # collapse consecutive whitespace but keep single newlines
        parts = [" ".join(line.split()).strip() for line in s.split("\n")]
        return "\n".join([p for p in parts if p])

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
                value = ";".join(sorted(ncms))
            elif logical == "cest":
                value = ";".join(sorted(cests))
            elif logical == "unidade":
                value = ";".join(sorted(units))
            elif logical == "composicao":
                value = row.get("composition") or ""
            else:
                value = row.get(logical, "")
            row[column] = value

    def _metafield_columns(self) -> List[str]:
        namespace = self.settings.metafields.namespace
        return [f"product.metafields.{namespace}.{key}" for key in self.settings.metafields.keys.values()]

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


__all__ = ["CSVGenerator"]

