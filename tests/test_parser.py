from pathlib import Path

from nfe_importer.core.parser import CatalogLoader, NFEParser


EXAMPLES = Path(__file__).resolve().parents[1] / "example_docs"


def test_nfe_parser_extracts_items():
    parser = NFEParser()
    xml_path = EXAMPLES / "35250805388725000384550110003221861697090032-nfe.xml"
    invoice = parser.parse_file(xml_path)

    assert invoice.invoice_number is not None
    assert len(invoice.items) > 0
    first_item = invoice.items[0]
    assert first_item.sku == "19487"
    assert first_item.quantity == 2.0
    assert first_item.unit_value > 0


def test_catalog_loader_reads_products():
    loader = CatalogLoader(EXAMPLES / "MART-Ficha-tecnica-Biblioteca-Virtual-08-08-2025.xlsx")
    products = loader.to_products()
    assert len(products) > 1000
    product = next(prod for prod in products if prod.sku == "08158")
    assert "BANDEJA" in product.title.upper()
    assert product.vendor == "MART"
