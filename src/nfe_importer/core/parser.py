"""Parsers for NF-e XML files and the master catalogue Excel sheet."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from lxml import etree

from .models import CatalogProduct, InvoiceInfo, NFEItem
from .utils import normalize_barcode, normalize_sku, safe_float


LOGGER = logging.getLogger(__name__)


def _to_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            LOGGER.debug("Unable to parse datetime value '%s'", value)
            return None


class NFEParser:
    """Parser responsible for extracting :class:`InvoiceInfo` objects from XML files."""

    def __init__(self) -> None:
        self._parser = etree.XMLParser(encoding="utf-8", recover=True)

    def parse_file(self, file_path: Path) -> InvoiceInfo:
        file_path = Path(file_path)
        tree = etree.parse(str(file_path), parser=self._parser)
        root = tree.getroot()
        namespaces = self._build_namespaces(root)

        inf_nfe = root.find(".//nfe:infNFe", namespaces=namespaces)
        if inf_nfe is None:
            raise ValueError(f"Could not find infNFe node in file {file_path}")

        access_key = (inf_nfe.get("Id") or file_path.stem).replace("NFe", "")
        invoice_number = self._text(inf_nfe, "ide/nNF", namespaces)
        issue_date = _to_datetime(self._text(inf_nfe, "ide/dhEmi", namespaces) or self._text(inf_nfe, "ide/dEmi", namespaces))
        supplier_name = self._text(inf_nfe, "emit/xNome", namespaces)
        supplier_cnpj = self._text(inf_nfe, "emit/CNPJ", namespaces)

        items: List[NFEItem] = []
        for det in inf_nfe.findall("nfe:det", namespaces=namespaces):
            item_number = int(det.get("nItem") or len(items) + 1)
            prod = det.find("nfe:prod", namespaces=namespaces)
            if prod is None:
                LOGGER.warning("Skipping det without prod node in %s", file_path)
                continue

            def prod_text(tag: str) -> Optional[str]:
                return self._text(prod, tag, namespaces)

            sku = prod_text("cProd")
            description = prod_text("xProd") or ""
            barcode = normalize_barcode(prod_text("cEAN") or prod_text("cEANTrib"))
            ncm = prod_text("NCM")
            cest = prod_text("CEST")
            cfop = prod_text("CFOP")
            unit = prod_text("uCom")
            quantity = safe_float(prod_text("qCom"), default=0.0)
            unit_value = safe_float(prod_text("vUnCom"), default=0.0)
            total_value = safe_float(prod_text("vProd"), default=unit_value * quantity)

            additional = {}
            inf_ad_prod = det.find("nfe:infAdProd", namespaces=namespaces)
            if inf_ad_prod is not None and inf_ad_prod.text:
                additional["infAdProd"] = inf_ad_prod.text.strip()

            item = NFEItem(
                invoice_key=access_key,
                item_number=item_number,
                sku=sku,
                description=description,
                barcode=barcode,
                ncm=ncm,
                cest=cest,
                cfop=cfop,
                unit=unit,
                quantity=quantity,
                unit_value=unit_value,
                total_value=total_value,
                additional_data=additional,
            )
            items.append(item)

        return InvoiceInfo(
            access_key=access_key,
            invoice_number=invoice_number,
            issue_date=issue_date,
            supplier_name=supplier_name,
            supplier_cnpj=supplier_cnpj,
            file_path=file_path,
            items=items,
        )

    def parse_many(self, files: Iterable[Path]) -> List[InvoiceInfo]:
        invoices: List[InvoiceInfo] = []
        for file_path in files:
            try:
                invoices.append(self.parse_file(Path(file_path)))
            except Exception:  # pragma: no cover - logged for debugging
                LOGGER.exception("Failed to parse NF-e file %s", file_path)
        return invoices

    @staticmethod
    def _build_namespaces(root) -> dict:
        namespaces = dict(root.nsmap)
        if None in namespaces:
            namespaces["nfe"] = namespaces.pop(None)
        if "nfe" not in namespaces:
            namespaces["nfe"] = "http://www.portalfiscal.inf.br/nfe"
        return namespaces

    @staticmethod
    def _text(node, path: str, namespaces: dict) -> Optional[str]:
        if node is None:
            return None
        parts = [part for part in path.split("/") if part]
        xpath = "/".join(f"nfe:{part}" for part in parts)
        element = node.find(xpath, namespaces=namespaces)
        if element is None or element.text is None:
            return None
        return element.text.strip() or None


def _sanitise_column(name: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in name).strip("_")


class CatalogLoader:
    """Helper responsible for reading the master catalogue Excel file."""

    COLUMN_MAPPING = {
        "codigo": "sku",
        "cod": "sku",
        "sku": "sku",
        "descricao": "title",
        "descricao_do_produto": "title",
        "name": "title",
        "ean13": "barcode",
        "ean": "barcode",
        "codigo_barras": "barcode",
        "marca": "vendor",
        "fabricante": "vendor",
        "categoria": "product_type",
        "subcategoria": "product_type",
        "colecao": "collection",
        "unid": "unit",
        "unid_": "unit",
        "ncm": "ncm",
        "cest": "cest",
        "peso_prod_c_emb_kg": "weight",
        "peso": "weight",
        "tags": "tags",
        "features": "features",
        "composicao": "composition",
        "cfop": "cfop",
        "preco": "price",
        "preco_venda": "price",
        "preco_sugerido": "price",
    }

    def __init__(self, excel_path: Path, sheet_name=0) -> None:
        self.excel_path = Path(excel_path)
        self.sheet_name = sheet_name

    def load_dataframe(self) -> pd.DataFrame:
        df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name, engine="openpyxl")
        df.columns = [_sanitise_column(str(col)) for col in df.columns]
        return df

    def to_products(self) -> List[CatalogProduct]:
        df = self.load_dataframe()
        products: List[CatalogProduct] = []
        for _, row in df.iterrows():
            data = {self.COLUMN_MAPPING.get(col, col): row[col] for col in row.index}

            sku = normalize_sku(data.get("sku"))
            if not sku:
                continue

            title = str(data.get("title") or sku).strip()
            barcode = normalize_barcode(data.get("barcode"))
            vendor = str(data.get("vendor") or "").strip() or None
            product_type = str(data.get("product_type") or "").strip() or None
            collection = str(data.get("collection") or "").strip() or None
            unit = str(data.get("unit") or "").strip() or None
            ncm = str(data.get("ncm") or "").strip() or None
            cest = str(data.get("cest") or "").strip() or None
            weight = None
            if pd.notna(data.get("weight")):
                try:
                    weight = float(str(data.get("weight")).replace(",", "."))
                except (ValueError, TypeError):
                    weight = None
            tags: List[str] = []
            raw_tags = data.get("tags")
            if isinstance(raw_tags, str):
                tags = [tag.strip() for tag in raw_tags.split(",") if tag.strip()]

            metafields = {}
            composition_value = data.get("composition")
            if isinstance(composition_value, str) and composition_value.strip():
                metafields["composition"] = composition_value.strip()

            # Capture all additional columns into 'extra' so feature flags can map them later
            extra: Dict[str, str] = {}
            features_value = data.get("features")
            if isinstance(features_value, str) and features_value.strip():
                extra["features"] = features_value.strip()
            price_value = data.get("price")
            if isinstance(price_value, (int, float)):
                extra["price"] = float(price_value)

            # Any non-empty residual fields from 'data' not consumed above become part of 'extra'
            consumed = {
                "sku",
                "title",
                "barcode",
                "vendor",
                "product_type",
                "collection",
                "unit",
                "ncm",
                "cest",
                "weight",
                "tags",
                "features",
                "composition",
                "cfop",
                "price",
            }
            for key, value in data.items():
                if key in consumed:
                    continue
                if value is None:
                    continue
                text = str(value).strip()
                if text and text.lower() != "nan":
                    extra[key] = text

            products.append(
                CatalogProduct(
                    sku=sku,
                    title=title,
                    barcode=barcode,
                    vendor=vendor,
                    product_type=product_type,
                    collection=collection,
                    unit=unit,
                    ncm=ncm,
                    cest=cest,
                    weight=weight,
                    tags=tags,
                    metafields=metafields,
                    extra=extra,
                )
            )

        LOGGER.info("Loaded %s products from %s", len(products), self.excel_path)
        return products


__all__ = ["NFEParser", "CatalogLoader"]

