import pytest

from nfe_importer.core.text_splitters import split_usage_from_text, DEFAULT_USAGE_MARKERS


def test_split_usage_detects_usage_blocks():
    text = "RECOMENDACOES: limpar com pano seco.\n\nPara limpeza utilize pano macio."
    desc, uso = split_usage_from_text(text)
    assert desc == ""
    assert "pano seco" in uso


def test_split_usage_ignores_when_no_markers():
    text = "Bandeja decorativa em metal dourado."\
        " Ideal para servir ou decorar mesas."\
        " Fabricada em aço com fundo de espelho."
    desc, uso = split_usage_from_text(text)
    assert desc
    assert uso == ""


def test_split_usage_respects_custom_markers():
    text = "ATENCAO: montar somente com supervisao."
    desc, uso = split_usage_from_text(text, usage_markers=["atencao"])
    assert desc == ""
    assert "supervisao" in uso
    assert "recomenda" not in desc.lower()
