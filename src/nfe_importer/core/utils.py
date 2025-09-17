"""Utility helpers used across the project."""

from __future__ import annotations

import json
import logging
import math
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional


LOGGER = logging.getLogger(__name__)


STOPWORDS_PT = {
    "de",
    "da",
    "do",
    "das",
    "dos",
    "para",
    "com",
    "em",
    "sem",
    "uma",
    "um",
    "e",
    "ou",
    "a",
    "o",
    "as",
    "os",
}


def ensure_directory(path: Path) -> None:
    """Create ``path`` when it does not exist."""

    path.mkdir(parents=True, exist_ok=True)


def strip_accents(text: str) -> str:
    """Remove diacritics from ``text``."""

    normalized = unicodedata.normalize("NFD", text)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def normalize_text(text: str, stopwords: Iterable[str] | None = None) -> str:
    """Normalise text for comparisons.

    * lowercase
    * remove accents
    * remove punctuation
    * collapse whitespace
    * drop stop words
    """

    if text is None:
        return ""

    text = strip_accents(text).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    words = [word for word in text.split() if word]
    if stopwords is None:
        stopwords = STOPWORDS_PT
    filtered = [word for word in words if word not in stopwords]
    return " ".join(filtered)


def normalize_sku(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    return value.upper()


def normalize_barcode(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits or None


def slugify(value: str) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    return re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")


def safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def gtin_is_valid(gtin: str) -> bool:
    """Validate a GTIN/EAN code using its check digit."""

    digits = normalize_barcode(gtin)
    if not digits or len(digits) not in {8, 12, 13, 14}:
        return False

    reversed_digits = list(map(int, digits[::-1]))
    check_digit = reversed_digits[0]
    factors = [3 if (i % 2 == 0) else 1 for i in range(len(reversed_digits) - 1)]
    total = sum(d * f for d, f in zip(reversed_digits[1:], factors))
    calculated = (10 - (total % 10)) % 10
    return calculated == check_digit


def now_timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")


def dump_json(path: Path, data) -> None:
    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)


def load_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def round_money(value: float) -> float:
    return math.floor(value * 100 + 0.5) / 100.0


__all__ = [
    "ensure_directory",
    "strip_accents",
    "normalize_text",
    "normalize_sku",
    "normalize_barcode",
    "slugify",
    "safe_float",
    "safe_int",
    "gtin_is_valid",
    "now_timestamp",
    "dump_json",
    "load_json",
    "round_money",
    "STOPWORDS_PT",
]

