"""Utilities to classify catalogue text into description vs. usage sections."""

from __future__ import annotations

import re
from typing import Iterable, Tuple

_RAW_USAGE_MARKERS: Tuple[str, ...] = (
    'recomendacoes',
    'recomenda\u00e7\u00f5es',
    'para limpeza',
    'para limpar',
    'nao utilizar',
    'n\u00e3o utilizar',
    'nao usar',
    'n\u00e3o usar',
    'pano',
    'espanador',
    'limpeza',
    'limpar',
    'higienizacao',
    'higieniza\u00e7\u00e3o',
    'manutencao',
    'manuten\u00e7\u00e3o',
    'uso',
)

def _decode_marker(value: str) -> str:
    return value.encode('utf-8').decode('unicode_escape')

DEFAULT_USAGE_MARKERS: Tuple[str, ...] = tuple(_decode_marker(marker) for marker in _RAW_USAGE_MARKERS)

def _normalise_markers(markers: Iterable[str]) -> Tuple[str, ...]:
    return tuple(sorted({m.casefold() for m in markers if m}))

def split_usage_from_text(text: str, usage_markers: list[str] | None = None) -> tuple[str, str]:
    """Return ``(description, usage)`` strings using a lightweight heuristic."""

    if not text:
        return '', ''

    markers = _normalise_markers(usage_markers or DEFAULT_USAGE_MARKERS)
    blocks = [block.strip() for block in re.split(r'\n+', text) if block.strip()]

    description_parts: list[str] = []
    usage_parts: list[str] = []

    for block in blocks:
        lowered = block.casefold()
        if any(marker in lowered for marker in markers):
            usage_parts.append(block.strip())
        else:
            description_parts.append(block.strip())

    if not usage_parts:
        description = description_parts or [text.strip()]
        return '\n\n'.join(description).strip(), ''

    description = '\n\n'.join(description_parts).strip()
    usage = '\n\n'.join(usage_parts).strip()
    return description, usage
