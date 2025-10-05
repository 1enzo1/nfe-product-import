"""Utilities to classify catalogue text into description vs. usage sections."""

from __future__ import annotations

import re
from typing import Iterable, Sequence, Tuple

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

USAGE_STRONG_LABELS: Tuple[str, ...] = (
    'recomenda\u00e7\u00f5es',
    'recomendacoes',
    'instru\u00e7\u00f5es',
    'instrucoes',
    'para limpeza',
)


def _decode_marker(value: str) -> str:
    return value.encode('utf-8').decode('unicode_escape')


DEFAULT_USAGE_MARKERS: Tuple[str, ...] = tuple(_decode_marker(marker) for marker in _RAW_USAGE_MARKERS)


def normalise_markers(markers: Iterable[str]) -> Tuple[str, ...]:
    return tuple(sorted({m.casefold() for m in markers if m}))


def usage_score(text: str, markers: Sequence[str]) -> int:
    lowered = text.casefold()
    return sum(lowered.count(marker) for marker in markers if marker)


def starts_with_strong_label(text: str, strong_labels: Sequence[str]) -> bool:
    lowered = text.casefold().lstrip()
    return any(lowered.startswith(label) for label in strong_labels)


def split_usage_from_text(text: str, usage_markers: list[str] | None = None) -> tuple[str, str]:
    """Return ``(description, usage)`` strings using a conservative heuristic."""

    if not text:
        return '', ''

    markers = normalise_markers(usage_markers or DEFAULT_USAGE_MARKERS)
    if usage_markers:
        strong_labels: Tuple[str, ...] = USAGE_STRONG_LABELS + normalise_markers(usage_markers)
    else:
        strong_labels = USAGE_STRONG_LABELS

    split_pattern = None
    if strong_labels:
        pattern_body = "|".join(re.escape(label) for label in strong_labels if label)
        if pattern_body:
            split_pattern = re.compile(r"\n+(?=\s*(?:" + pattern_body + r"))", flags=re.IGNORECASE)

    raw_blocks = [block.strip() for block in re.split(r"\n{2,}", text) if block.strip()]
    if not raw_blocks:
        raw_blocks = [text.strip()]

    blocks: list[str] = []
    for chunk in raw_blocks:
        if split_pattern:
            pieces = [piece.strip() for piece in split_pattern.split(chunk) if piece.strip()]
            if pieces:
                blocks.extend(pieces)
                continue
        if chunk:
            blocks.append(chunk)

    if not blocks:
        blocks = [text.strip()]

    description_parts: list[str] = []
    usage_parts: list[str] = []

    for block in blocks:
        score = usage_score(block, markers)
        strong = starts_with_strong_label(block, strong_labels)
        if score >= 2 or strong:
            usage_parts.append(block.strip())
        else:
            description_parts.append(block.strip())

    if not usage_parts:
        description = description_parts or [text.strip()]
        return "\n\n".join(description).strip(), ""

    description = "\n\n".join(description_parts).strip()
    usage = "\n\n".join(usage_parts).strip()
    return description, usage


__all__ = [
    "DEFAULT_USAGE_MARKERS",
    "USAGE_STRONG_LABELS",
    "normalise_markers",
    "split_usage_from_text",
    "starts_with_strong_label",
    "usage_score",
]
