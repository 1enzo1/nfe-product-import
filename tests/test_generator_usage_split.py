from pathlib import Path

import pandas as pd

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings


def _make_generator(tmp_path):
    settings = build_settings(tmp_path)
    return CSVGenerator(settings)


def test_usage_from_features_kept_out_of_body(tmp_path):
    generator = _make_generator(tmp_path)
    product = CatalogProduct(
        sku="SKU-USE-ONLY",
        title="Bandeja Decorativa",
        vendor="MART",
        product_type="BANDEJAS",
        extra={"features": "RECOMENDACOES: limpar com pano seco."},
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    modo = row.get("product.metafields.custom.modo_de_uso", "")
    assert "RECOMENDACOES" in modo.upper()
    assert "RECOMENDACOES" not in (row.get("Body (HTML)") or "").upper()


def test_description_and_usage_split(tmp_path):
    generator = _make_generator(tmp_path)
    text = (
        "Bandeja retangular em acabamento cobre polido.\n\n"
        "RECOMENDACOES: limpar com pano macio e evitar produtos abrasivos."
    )
    product = CatalogProduct(
        sku="SKU-DESC-USO",
        title="Bandeja Cobre",
        vendor="MART",
        product_type="BANDEJAS",
        extra={"catalog_description": text},
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    body = row.get("Body (HTML)") or ""
    modo = row.get("product.metafields.custom.modo_de_uso", "")
    assert "BANDEJA RETANGULAR" in body.upper()
    assert "RECOMENDACOES" not in body.upper()
    assert "RECOMENDACOES" in modo.upper()


def test_polirresina_text_kept_in_body(tmp_path):
    generator = _make_generator(tmp_path)
    text = "A polirresina é resistente e permite moldagens detalhadas."
    product = CatalogProduct(
        sku="SKU-POLI",
        title="Escultura Decorativa",
        vendor="MART",
        product_type="Decor",
        metafields={"composition": "70% METAL"},
        extra={"catalog_description": text},
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    body = row.get("Body (HTML)") or ""
    assert "POLIRRESINA" in body.upper()
    # modo de uso pode estar vazio, apenas garantir que não houve falha
    assert "product.metafields.custom.modo_de_uso" in df.columns
