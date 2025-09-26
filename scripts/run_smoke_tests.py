from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

from nfe_importer.config import Settings
from nfe_importer.core.pipeline import Processor

TEMPLATE_HEADERS: List[str] = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Price",
    "Variant Compare At Price",
    "Variant Inventory Qty",
    "Variant Weight",
    "Variant Weight Unit",
    "Variant Requires Shipping",
    "Image Src",
    "Variant Barcode",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "product.metafields.custom.unidade",
    "product.metafields.custom.catalogo",
    "product.metafields.custom.dimensoes_do_produto",
    "product.metafields.custom.composicao",
    "product.metafields.custom.capacidade",
    "product.metafields.custom.modo_de_uso",
    "product.metafields.custom.icms",
    "product.metafields.custom.ncm",
    "product.metafields.custom.pis",
    "product.metafields.custom.ipi",
    "product.metafields.custom.cofins",
    "product.metafields.custom.componente_de_kit",
    "product.metafields.custom.resistencia_a_agua",
    "Variant Taxable",
    "Cost per item",
    "Image Position",
    "Variant Image",
    "Product Category",
    "Type",
    "Collection",
    "Status",
]

EXAMPLE_XMLS: Tuple[Path, ...] = (
    Path("example_docs/35250805388725000384550110003221861697090032-nfe.xml"),
    Path("example_docs/procNFE29250807397758000154550010000681841767795834.xml"),
)

VERSIONS: Tuple[Tuple[str, Path], ...] = (
    ("v1", Path("pipelines/v1/config.yaml")),
    ("v2", Path("pipelines/v2/config.yaml")),
    ("enhanced", Path("pipelines/enhanced/config.yaml")),
    ("super", Path("pipelines/super/config.yaml")),
)


def run_pipeline(version_key: str, config_path: Path) -> Path:
    settings = Settings.load(str(config_path))
    processor = Processor(settings)
    xml_files = [path for path in EXAMPLE_XMLS if path.exists()]
    if not xml_files:
        raise FileNotFoundError("No example XML files found for smoke test")
    result = processor.process_files(xml_files, mode=f"smoke:{version_key}", user="smoke-test")
    return Path(result.dataframe_path)


def validate_csv(csv_path: Path) -> Dict[str, object]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers_ok = reader.fieldnames == TEMPLATE_HEADERS
        combos_per_handle: Dict[str, List[Tuple[str, str, str]]] = {}
        policies_ok = True
        weights_ok = True
        total_rows = 0
        for row in reader:
            total_rows += 1
            handle_val = (row.get("Handle") or "").strip()
            option_combo = (
                (row.get("Option1 Value") or "").strip(),
                (row.get("Option2 Value") or "").strip(),
                (row.get("Option3 Value") or "").strip(),
            )
            combos_per_handle.setdefault(handle_val, []).append(option_combo)

            fulfillment = (row.get("Variant Fulfillment Service") or "").strip().lower()
            inventory_policy = (row.get("Variant Inventory Policy") or "").strip().lower()
            inventory_tracker = (row.get("Variant Inventory Tracker") or "").strip().lower()
            requires_shipping = (row.get("Variant Requires Shipping") or "").strip().upper()
            taxable = (row.get("Variant Taxable") or "").strip().upper()

            policies_ok &= fulfillment == "manual"
            policies_ok &= inventory_policy == "deny"
            policies_ok &= inventory_tracker == "shopify"
            policies_ok &= requires_shipping == "TRUE"
            policies_ok &= taxable == "TRUE"

            weight_val_raw = (row.get("Variant Weight") or "").strip()
            weight_unit = (row.get("Variant Weight Unit") or "").strip().lower()
            if weight_val_raw:
                try:
                    weight_val = float(weight_val_raw)
                    if weight_unit == "g":
                        weights_ok &= 0 < weight_val < 1000
                    elif weight_unit == "kg":
                        weights_ok &= weight_val >= 1
                    else:
                        weights_ok = False
                except ValueError:
                    weights_ok = False

        variants_ok = True
        for combos in combos_per_handle.values():
            counts = Counter(combos)
            if any(count > 1 for count in counts.values()):
                variants_ok = False
                break

    return {
        "headers": headers_ok,
        "variants": variants_ok,
        "policies": policies_ok,
        "weights": weights_ok,
        "rows": total_rows,
    }


def main() -> None:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    scoreboard_path = reports_dir / "scoreboard.csv"

    rows: List[Tuple[object, ...]] = [
        ("version", "csv_path", "headers_ok", "variants_unique", "policies_ok", "weight_ok", "rows"),
    ]

    for version_key, config_path in VERSIONS:
        if not config_path.exists():
            raise FileNotFoundError(f"Config not found for version {version_key}: {config_path}")
        csv_path = run_pipeline(version_key, config_path)
        checks = validate_csv(csv_path)
        rows.append(
            (
                version_key,
                str(csv_path),
                checks["headers"],
                checks["variants"],
                checks["policies"],
                checks["weights"],
                checks["rows"],
            )
        )

    with scoreboard_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    print("Smoke tests completed. Scoreboard available at", scoreboard_path)


if __name__ == "__main__":
    main()
