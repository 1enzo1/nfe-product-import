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

            row["Inventory Qty"] = round(quantity, 4)
            row["Cost per item"] = round_money(cost_per_item)

            row["Price"] = self._compute_price(row, cost_per_item)

            cfops = row.pop("_cfops", set())
            ncms = row.pop("_ncm", set())
            cests = row.pop("_cest", set())
            units = row.pop("_units", set())

            self._fill_metafields(row, cfops=cfops, ncms=ncms, cests=cests, units=units)

        dataframe = pd.DataFrame(rows.values())
        # Ensure all expected output columns exist (CSV + metafields)
        expected_csv_columns = list(self.settings.csv_output.columns)
        expected_meta_columns = self._metafield_columns()
        expected_columns = expected_csv_columns + expected_meta_columns
        for column in expected_columns:
            if column not in dataframe.columns:
                dataframe[column] = ""

        # Robust reindex to avoid KeyError even when some columns are missing
        dataframe = dataframe.reindex(columns=expected_columns, fill_value="")
        return dataframe.fillna("")

    def _base_row(self, product: CatalogProduct) -> Dict[str, object]:
        row: Dict[str, object] = {
            "Handle": slugify(product.title or product.sku),
            "Title": product.title,
            "Vendor": product.vendor or self.settings.default_vendor or "",
            "Product Type": product.product_type or "",
            "SKU": product.sku,
            "Barcode": self._valid_barcode(product.barcode),
            "Status": "active",
            "Published": "TRUE",
            "Tags": ",".join(product.tags) if product.tags else "",
            "Compare At Price": "",
            "Weight": product.weight or "",
            "Image Src": self.image_resolver(product) or "",
        }
        if product.metafields:
            row["composition"] = product.metafields.get("composition", "")
        return row

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

