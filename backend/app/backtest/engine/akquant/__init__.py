"""AKQuant 引擎子包"""
from __future__ import annotations

# 惰性导入 — akquant 可能未安装
try:
    import akquant as aq
    AKQUANT_AVAILABLE = True
except ImportError:
    aq = None  # type: ignore
    AKQUANT_AVAILABLE = False
