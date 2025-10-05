from nfe_importer.core.generator import CSVGenerator

from tests.test_generator import build_settings


def test_finalize_body_strips_usage_prefix(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    body_text = "Limpe o produto com pano macio. Para limpar utilize sabao neutro.\nApos a limpeza, seque bem."
    row = {"Body (HTML)": body_text}

    result = generator._finalize_body(row)
    assert result == ""
