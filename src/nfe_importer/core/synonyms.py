"""Persistence helpers for synonym/equivalence mappings."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from .utils import dump_json, load_json, normalize_barcode, normalize_sku, normalize_text


LOGGER = logging.getLogger(__name__)


@dataclass
class SynonymCache:
    """Stores mappings between NF-e references and catalogue SKUs."""

    path: Path
    data: Dict[str, Dict[str, str]] = field(default_factory=lambda: {"cprod": {}, "barcode": {}, "description": {}})
    history: Dict[str, list] = field(default_factory=lambda: {"decisions": []})

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        existing = load_json(self.path)
        if existing:
            self.data.update(existing.get("data", {}))
            self.history.update(existing.get("history", {}))

    def lookup_by_cprod(self, value: Optional[str]) -> Optional[str]:
        normalized = normalize_sku(value)
        if not normalized:
            return None
        return self.data.setdefault("cprod", {}).get(normalized)

    def lookup_by_barcode(self, value: Optional[str]) -> Optional[str]:
        normalized = normalize_barcode(value)
        if not normalized:
            return None
        return self.data.setdefault("barcode", {}).get(normalized)

    def lookup_by_description(self, value: Optional[str]) -> Optional[str]:
        normalized = normalize_text(value or "")
        if not normalized:
            return None
        return self.data.setdefault("description", {}).get(normalized)

    def register(self, *, sku: str, cprod: Optional[str] = None, barcode: Optional[str] = None, description: Optional[str] = None) -> None:
        sku = normalize_sku(sku) or sku
        if cprod:
            normalized = normalize_sku(cprod)
            if normalized:
                self.data.setdefault("cprod", {})[normalized] = sku
        if barcode:
            normalized = normalize_barcode(barcode)
            if normalized:
                self.data.setdefault("barcode", {})[normalized] = sku
        if description:
            normalized = normalize_text(description)
            if normalized:
                self.data.setdefault("description", {})[normalized] = sku

    def record_manual_choice(self, *, invoice_key: str, item_number: int, sku: str, user: Optional[str]) -> None:
        entry = {
            "invoice_key": invoice_key,
            "item_number": item_number,
            "sku": normalize_sku(sku),
            "user": user,
        }
        self.history.setdefault("decisions", []).append(entry)

    def save(self) -> None:
        payload = {"data": self.data, "history": self.history}
        try:
            dump_json(self.path, payload)
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to persist synonym cache to %s", self.path)


__all__ = ["SynonymCache"]

