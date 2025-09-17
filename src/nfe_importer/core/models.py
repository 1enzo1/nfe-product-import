"""Dataclasses describing the core domain objects used by the importer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class NFEItem:
    """Representation of a line item read from an NF-e XML file."""

    invoice_key: str
    item_number: int
    sku: Optional[str]
    description: str
    barcode: Optional[str]
    ncm: Optional[str]
    cest: Optional[str]
    cfop: Optional[str]
    unit: Optional[str]
    quantity: float
    unit_value: float
    total_value: float
    additional_data: Dict[str, str] = field(default_factory=dict)


@dataclass
class InvoiceInfo:
    access_key: str
    invoice_number: Optional[str]
    issue_date: Optional[datetime]
    supplier_name: Optional[str]
    supplier_cnpj: Optional[str]
    file_path: Path
    items: List[NFEItem]


@dataclass
class CatalogProduct:
    sku: str
    title: str
    barcode: Optional[str] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    collection: Optional[str] = None
    unit: Optional[str] = None
    ncm: Optional[str] = None
    cest: Optional[str] = None
    weight: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    metafields: Dict[str, str] = field(default_factory=dict)
    extra: Dict[str, str] = field(default_factory=dict)


@dataclass
class MatchDecision:
    item: NFEItem
    product: CatalogProduct
    confidence: float
    match_source: str
    notes: Optional[str] = None


@dataclass
class Suggestion:
    product: CatalogProduct
    confidence: float


@dataclass
class UnmatchedItem:
    item: NFEItem
    suggestions: List[Suggestion] = field(default_factory=list)
    reason: Optional[str] = None


@dataclass
class ProcessingSummary:
    run_id: str
    created_at: datetime
    invoices: List[InvoiceInfo]
    matched: List[MatchDecision]
    unmatched: List[UnmatchedItem]
    csv_path: Path
    pendings_path: Optional[Path]
    mode: str
    user: Optional[str]


__all__ = [
    "NFEItem",
    "InvoiceInfo",
    "CatalogProduct",
    "MatchDecision",
    "Suggestion",
    "UnmatchedItem",
    "ProcessingSummary",
]

