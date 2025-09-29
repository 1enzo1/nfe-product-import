import json
from pathlib import Path

import pandas as pd

from nfe_importer.config import Settings
from nfe_importer.core.pipeline import Processor


def make_settings(tmp_path: Path) -> Settings:
    master_path = tmp_path / "master.xlsx"
    master_path.touch()
    cfg = {
        "paths": {
            "nfe_input_folder": str(tmp_path / "input"),
            "master_data_file": str(master_path),
            "output_folder": str(tmp_path / "output"),
            "log_folder": str(tmp_path / "logs"),
            "synonym_cache_file": str(tmp_path / "synonyms.json"),
        },
        "csv_output": {
            "filename_prefix": "test_",
            "columns": [
                "product.metafields.custom.unidade",
                "product.metafields.custom.ncm",
                "product.metafields.custom.capacidade",
                "product.metafields.custom.dimensoes_do_produto",
            ],
        },
        "metafields": {
            "namespace": "custom",
            "keys": {
                "unidade": "unidade",
                "ncm": "ncm",
                "capacidade": "capacidade",
                "dimensoes_do_produto": "dimensoes_do_produto",
            },
        },
    }
    settings = Settings.parse_obj(cfg)
    settings.ensure_folders()
    return settings


def test_update_metrics_writes_counts(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    processor = object.__new__(Processor)
    processor.settings = settings

    df = pd.DataFrame(
        {
            "product.metafields.custom.unidade": ["CX", ""],
            "product.metafields.custom.ncm": ["1111", "2222"],
            "product.metafields.custom.capacidade": ["", "2L"],
            "product.metafields.custom.dimensoes_do_produto": ["10x10", "20x20"],
        }
    )

    processor._update_metrics("RUN123", df)

    metrics_path = settings.paths.log_folder / "metrics.json"
    data = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert data["runs"][0]["run_id"] == "RUN123"
    fields = data["runs"][0]["fields"]
    assert fields["product.metafields.custom.unidade"]["non_empty"] == 1
    assert fields["product.metafields.custom.ncm"]["non_empty"] == 2
    assert fields["product.metafields.custom.capacidade"]["non_empty"] == 1
    assert fields["product.metafields.custom.dimensoes_do_produto"]["non_empty"] == 2
    assert fields["product.metafields.custom.ncm"]["total"] == 2
