from pathlib import Path

from nfe_importer.core.matcher import ProductMatcher
from nfe_importer.core.models import NFEItem
from nfe_importer.core.parser import CatalogLoader
from nfe_importer.core.synonyms import SynonymCache


EXAMPLES = Path(__file__).resolve().parents[1] / "example_docs"


def build_matcher(tmp_path):
    loader = CatalogLoader(EXAMPLES / "MART-Ficha-tecnica-Biblioteca-Virtual-08-08-2025.xlsx")
    products = loader.to_products()
    synonyms = SynonymCache(tmp_path / "synonyms.json")
    matcher = ProductMatcher(products, synonyms)
    return matcher, synonyms, products


def make_item(**overrides) -> NFEItem:
    data = dict(
        invoice_key="TEST",
        item_number=1,
        sku="08158",
        description="BANDEJA EM METAL COM ESPELHO",
        barcode="7899525681589",
        ncm="73239900",
        cest=None,
        cfop="5102",
        unit="PC",
        quantity=1.0,
        unit_value=10.0,
        total_value=10.0,
    )
    data.update(overrides)
    return NFEItem(**data)


def test_matcher_matches_by_sku(tmp_path):
    matcher, synonyms, products = build_matcher(tmp_path)
    item = make_item()
    decision = matcher.match_item(item)
    assert decision is not None
    assert decision.product.sku == "08158"
    # Synonym must be persisted for future runs
    assert synonyms.lookup_by_cprod("08158") == "08158"


def test_matcher_matches_by_barcode(tmp_path):
    matcher, synonyms, products = build_matcher(tmp_path)
    item = make_item(sku="UNKNOWN", barcode="7899525681589")
    decision = matcher.match_item(item)
    assert decision is not None
    assert decision.product.sku == "08158"


def test_matcher_returns_suggestions_for_unknown_item(tmp_path):
    matcher, synonyms, products = build_matcher(tmp_path)
    item = make_item(sku="00000", barcode="0000000000000", description="BANDEJA EM METAL ESPELHO")
    decision = matcher.match_item(item)
    assert decision is None
    suggestions = matcher.suggest(item)
    assert suggestions
    assert any(s.product.sku == "08158" for s in suggestions)
