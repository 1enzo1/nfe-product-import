"""Matching logic between NF-e items and catalogue products."""

from __future__ import annotations

import difflib
import logging
from typing import Iterable, List, Optional, Tuple

from .models import CatalogProduct, MatchDecision, NFEItem, Suggestion, UnmatchedItem
from .synonyms import SynonymCache
from .utils import normalize_barcode, normalize_sku, normalize_text


LOGGER = logging.getLogger(__name__)


class ProductMatcher:
    """Resolve NF-e items to products from the master catalogue."""

    def __init__(self, products: Iterable[CatalogProduct], synonyms: SynonymCache, *, auto_threshold: float = 0.92) -> None:
        self.products = list(products)
        self.synonyms = synonyms
        self.auto_threshold = auto_threshold
        self._build_indexes()

    def _build_indexes(self) -> None:
        self._sku_index = {}
        self._barcode_index = {}
        self._normalized_titles = {}

        for product in self.products:
            sku = normalize_sku(product.sku)
            if sku:
                self._sku_index[sku] = product

            barcode = normalize_barcode(product.barcode)
            if barcode:
                self._barcode_index[barcode] = product

            normalized_title = normalize_text(product.title or "")
            if product.collection:
                normalized_title = f"{normalized_title} {normalize_text(product.collection)}"
            if product.product_type:
                normalized_title = f"{normalized_title} {normalize_text(product.product_type)}"
            self._normalized_titles[product.sku] = normalized_title.strip()

    def refresh_products(self, products: Iterable[CatalogProduct]) -> None:
        self.products = list(products)
        self._build_indexes()

    def match_items(self, items: Iterable[NFEItem]) -> Tuple[List[MatchDecision], List[UnmatchedItem]]:
        matched: List[MatchDecision] = []
        unmatched: List[UnmatchedItem] = []

        for item in items:
            decision = self.match_item(item)
            if decision:
                matched.append(decision)
            else:
                unmatched.append(
                    UnmatchedItem(
                        item=item,
                        suggestions=self.suggest(item),
                        reason="No match found",
                    )
                )

        return matched, unmatched

    def match_item(self, item: NFEItem) -> Optional[MatchDecision]:
        # 1) Synonym from previous reconciliations using cProd
        if item.sku:
            sku = self.synonyms.lookup_by_cprod(item.sku)
            product = self._product_from_sku(sku)
            if product:
                return self._decision(item, product, confidence=0.99, source="synonym-sku")

        # 2) Exact SKU
        product = self._product_from_sku(item.sku)
        if product:
            self.synonyms.register(sku=product.sku, cprod=item.sku, barcode=item.barcode, description=item.description)
            return self._decision(item, product, confidence=1.0, source="sku")

        # 3) Synonym by barcode
        if item.barcode:
            sku = self.synonyms.lookup_by_barcode(item.barcode)
            product = self._product_from_sku(sku)
            if product:
                return self._decision(item, product, confidence=0.98, source="synonym-barcode")

        # 4) Barcode direct
        product = self._product_from_barcode(item.barcode)
        if product:
            self.synonyms.register(sku=product.sku, barcode=item.barcode, description=item.description)
            return self._decision(item, product, confidence=0.97, source="barcode")

        # 5) Synonym by description
        sku = self.synonyms.lookup_by_description(item.description)
        product = self._product_from_sku(sku)
        if product:
            return self._decision(item, product, confidence=0.95, source="synonym-description")

        # 6) Fuzzy description match
        product, score = self._best_fuzzy_match(item)
        if product and score >= self.auto_threshold:
            self.synonyms.register(sku=product.sku, description=item.description)
            return self._decision(item, product, confidence=score, source="fuzzy")

        return None

    def suggest(self, item: NFEItem, top_n: int = 5) -> List[Suggestion]:
        candidates = []
        normalized = normalize_text(item.description)
        normalized_barcode = normalize_barcode(item.barcode)

        for product in self.products:
            score = self._similarity(normalized, self._normalized_titles.get(product.sku, ""))
            if normalized_barcode and normalize_barcode(product.barcode) == normalized_barcode:
                score = min(1.0, max(score, 0.95))
            candidates.append((product, score))

        candidates.sort(key=lambda entry: entry[1], reverse=True)
        return [Suggestion(product=product, confidence=score) for product, score in candidates[:top_n]]

    def _product_from_sku(self, sku: Optional[str]) -> Optional[CatalogProduct]:
        normalized = normalize_sku(sku)
        if not normalized:
            return None
        return self._sku_index.get(normalized)

    def _product_from_barcode(self, barcode: Optional[str]) -> Optional[CatalogProduct]:
        normalized = normalize_barcode(barcode)
        if not normalized:
            return None
        return self._barcode_index.get(normalized)

    def _best_fuzzy_match(self, item: NFEItem) -> Tuple[Optional[CatalogProduct], float]:
        normalized = normalize_text(item.description)
        best_product: Optional[CatalogProduct] = None
        best_score = 0.0
        for product in self.products:
            candidate_text = self._normalized_titles.get(product.sku, "")
            score = self._similarity(normalized, candidate_text)
            if score > best_score:
                best_score = score
                best_product = product
        return best_product, best_score

    @staticmethod
    def _similarity(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return difflib.SequenceMatcher(None, a, b).ratio()

    @staticmethod
    def _decision(item: NFEItem, product: CatalogProduct, confidence: float, source: str) -> MatchDecision:
        LOGGER.debug("Matched %s -> %s using %s (confidence %.2f)", item.sku or item.description, product.sku, source, confidence)
        return MatchDecision(item=item, product=product, confidence=confidence, match_source=source)


__all__ = ["ProductMatcher"]

