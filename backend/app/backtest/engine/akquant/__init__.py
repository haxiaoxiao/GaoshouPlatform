"""AKQuant engine package."""
from __future__ import annotations

# Lazy import: akquant may be missing in lightweight environments.
try:
    import akquant as aq

    AKQUANT_AVAILABLE = True
except ImportError:
    aq = None  # type: ignore
    AKQUANT_AVAILABLE = False
