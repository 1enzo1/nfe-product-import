from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in __import__('sys').path:
    __import__('sys').path.append(str(ROOT_DIR))

from scripts.validate_csv_fields import DEFAULT_USAGE_MARKERS, collect_warnings


def _clean_value(value: object) -> str:
    if value is None:
        return ''
    text = str(value).strip()
    return '' if text.casefold() in {'', 'nan'} else text


def summarise_handle(group: pd.DataFrame, handle: str, warnings_by_handle: Dict[str, List[str]]) -> Dict[str, object]:
    variant_skus = sorted({_clean_value(value) for value in group.get('Variant SKU', []) if _clean_value(value)})
    option1_names = sorted({_clean_value(value) for value in group.get('Option1 Name', []) if _clean_value(value)})
    option1_values = sorted({_clean_value(value) for value in group.get('Option1 Value', []) if _clean_value(value)})

    option2_present = any(_clean_value(value) for value in group.get('Option2 Name', [])) or any(
        _clean_value(value) for value in group.get('Option2 Value', [])
    ) or any(_clean_value(value) for value in group.get('Option3 Name', [])) or any(
        _clean_value(value) for value in group.get('Option3 Value', [])
    )

    warnings = sorted(set(warnings_by_handle.get(handle, [])))

    sku_display = ', '.join(variant_skus[:6])
    if len(variant_skus) > 6:
        sku_display += '...'

    return {
        'Handle': handle,
        'SKUs': sku_display or '(vazio)',
        'Option1 Name': ', '.join(option1_names) if option1_names else '(vazio)',
        'Option1 Values': ', '.join(option1_values) if option1_values else '(vazio)',
        'Option2/3?': 'sim' if option2_present else 'nao',
        'Flags': ', '.join(warnings) if warnings else '-',
    }


def build_summary(csv_path: Path) -> List[Dict[str, object]]:
    dataframe = pd.read_csv(csv_path)
    warnings = collect_warnings(dataframe, DEFAULT_USAGE_MARKERS)

    warnings_by_handle: Dict[str, List[str]] = {}
    for warning in warnings:
        handle = (warning.get('handle') or '').strip()
        if not handle:
            continue
        warnings_by_handle.setdefault(handle, []).append(warning.get('warning_type', ''))

    summary: List[Dict[str, object]] = []
    for handle, group in dataframe.groupby('Handle', sort=False):
        if group.shape[0] <= 1:
            continue
        summary.append(summarise_handle(group, handle, warnings_by_handle))
    return summary


def render_markdown(summary: Sequence[Dict[str, object]], limit: int | None = None) -> str:
    header = "| Handle | SKUs | Option1 Name | Valores (unicos) | Tem Option2/3? | Flags |"
    separator = "| --- | --- | --- | --- | --- | --- |"
    rows_data = summary[:limit] if limit else summary
    rows = [
        "| {Handle} | {SKUs} | {Option1 Name} | {Option1 Values} | {Option2/3?} | {Flags} |".format(**item)
        for item in rows_data
    ]
    return "\n".join([header, separator, *rows])


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate per-handle option integrity report")
    parser.add_argument('paths', nargs='+', help='CSV files to analyse')
    parser.add_argument('--limit', type=int, default=20, help='Maximum number of handles to list (default: 20)')
    args = parser.parse_args(argv)

    output_dir = Path('reports') / 'validation'
    output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    report_path = output_dir / f'options_integrity_{now}.md'

    all_rows: List[Dict[str, object]] = []
    for raw in args.paths:
        path = Path(raw)
        if not path.exists():
            raise SystemExit(f'CSV not found: {path}')
        summary = build_summary(path)
        all_rows.extend(summary)

    if not all_rows:
        markdown = "Nenhum handle com variantes encontrado."
    else:
        all_rows.sort(key=lambda item: (-len(item['SKUs'].split(', ')), item['Handle']))
        markdown = render_markdown(all_rows, limit=args.limit)

    report_path.write_text(markdown, encoding='utf-8')
    print(f'Relatorio salvo em: {report_path}')
    print('\n' + markdown)


if __name__ == '__main__':
    main()
