from __future__ import annotations


def normalize_security_symbol(symbol: str | None, market: str = "") -> str | None:
    if symbol is None:
        return None
    text = str(symbol).strip().upper()
    if not text:
        return None

    if text.endswith(".XSHG") and len(text) > 5:
        return f"{text[:-5]}.SH"
    if text.endswith(".XSHE") and len(text) > 5:
        return f"{text[:-5]}.SZ"

    if text.startswith(("SH", "SZ", "BJ")) and len(text) > 2 and text[2:].isdigit():
        return f"{text[2:]}.{text[:2]}"

    if "." in text:
        code, suffix = text.split(".", 1)
        return f"{code}.{suffix}"

    market_text = str(market or "").strip().upper()
    if market_text in {"SH", "SSE", "XSHG", "1"}:
        return f"{text}.SH"
    if market_text in {"SZ", "SZSE", "XSHE", "0"}:
        return f"{text}.SZ"
    if market_text in {"BJ", "BSE"}:
        return f"{text}.BJ"

    return text


def to_jq_symbol(symbol: str | None) -> str | None:
    normalized = normalize_security_symbol(symbol)
    if not normalized:
        return None
    if "." not in normalized:
        return normalized
    code, suffix = normalized.rsplit(".", 1)
    if suffix == "SH":
        return f"{code}.XSHG"
    if suffix == "SZ":
        return f"{code}.XSHE"
    return normalized
