
import pandas as pd

from scripts.validate_csv_fields import collect_warnings, DEFAULT_USAGE_MARKERS


def _base_row(handle: str, sku: str, option_name: str, option_value: str) -> dict:
    return {
        'Handle': handle,
        'Variant SKU': sku,
        'Body (HTML)': 'Descricao detalhada do produto para testes automatizados.',
        'product.metafields.custom.modo_de_uso': '',
        'Option1 Name': option_name,
        'Option1 Value': option_value,
    }


def test_option_axis_mixed_warning_detected():
    df = pd.DataFrame([
        _base_row('produto-variantes', 'SKU-P', 'Tamanho', 'P'),
        _base_row('produto-variantes', 'SKU-300', 'Capacidade', '300 ml'),
    ])

    warnings = collect_warnings(df, DEFAULT_USAGE_MARKERS)
    axis_warnings = [w for w in warnings if w['warning_type'] == 'option_axis_mixed']
    assert axis_warnings, 'expected option_axis_mixed warning'
    assert axis_warnings[0]['handle'] == 'produto-variantes'


def test_option_value_incoherent_warning_detected():
    df = pd.DataFrame([
        _base_row('produto-misto', 'SKU-P', 'Tamanho', 'P'),
        _base_row('produto-misto', 'SKU-M', 'Tamanho', 'M'),
        _base_row('produto-misto', 'SKU-300', 'Tamanho', '300 ml'),
    ])

    warnings = collect_warnings(df, DEFAULT_USAGE_MARKERS)
    value_warnings = [w for w in warnings if w['warning_type'] == 'option_value_incoherent']
    assert value_warnings, 'expected option_value_incoherent warning'
    assert value_warnings[0]['handle'] == 'produto-misto'
