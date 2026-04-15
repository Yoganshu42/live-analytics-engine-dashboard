from __future__ import annotations

SAMSUNG_PARTNER_SOURCES: tuple[str, ...] = (
    "samsung_vs",
    "samsung_croma",
    "samsung_reliance_digital",
)

SAMSUNG_SOURCE_VARIANTS: tuple[str, ...] = (
    *SAMSUNG_PARTNER_SOURCES,
    "samsung_vijay_sales",
    "samsung",
)

SAMSUNG_PARTNER_LABELS: dict[str, str] = {
    "samsung_vs": "Samsung Vijay Sales",
    "samsung_croma": "Samsung Croma",
    "samsung_reliance_digital": "Samsung Reliance Digital",
}

_SAMSUNG_SOURCE_ALIASES: dict[str, str] = {
    "samsung_vs": "samsung_vs",
    "samsung_vijay_sales": "samsung_vs",
    "samsung vs": "samsung_vs",
    "samsung vijay sales": "samsung_vs",
    "vijay sales": "samsung_vs",
    "samsung_croma": "samsung_croma",
    "samsung croma": "samsung_croma",
    "croma": "samsung_croma",
    "samsung protect max": "samsung_croma",
    "samsung protect max croma": "samsung_croma",
    "protect max": "samsung_croma",
    "protect max croma": "samsung_croma",
    "croma protect max": "samsung_croma",
    "samsung_reliance_digital": "samsung_reliance_digital",
    "samsung reliance digital": "samsung_reliance_digital",
    "samsungreliancedigital": "samsung_reliance_digital",
    "reliance digital": "samsung_reliance_digital",
    "reliance_digital": "samsung_reliance_digital",
    "reliance-digital": "samsung_reliance_digital",
    "reliancedigital": "samsung_reliance_digital",
}


def normalize_samsung_source(value: str | None) -> str | None:
    source_key = (value or "").strip().lower()
    if not source_key:
        return None
    if source_key == "samsung":
        return "samsung"
    return _SAMSUNG_SOURCE_ALIASES.get(source_key)


def is_samsung_source(value: str | None) -> bool:
    source_key = (value or "").strip().lower()
    normalized = normalize_samsung_source(source_key)
    return normalized == "samsung" or normalized in SAMSUNG_PARTNER_SOURCES or source_key == "samsung"
