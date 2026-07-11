from __future__ import annotations

from typing import Any, Awaitable, Callable

from . import eastmoney, jisilu, laohu, nga, taoguba, tieba, wechat, xueqiu
from .base import AdapterRequest, AdapterResult

AdapterRunner = Callable[[Any, AdapterRequest], Awaitable[AdapterResult]]

ADAPTERS: dict[str, AdapterRunner] = {
    xueqiu.SOURCE: xueqiu.run,
    eastmoney.SOURCE: eastmoney.run,
    taoguba.SOURCE: taoguba.run,
    tieba.SOURCE: tieba.run,
    laohu.SOURCE: laohu.run,
    jisilu.SOURCE: jisilu.run,
    wechat.SOURCE: wechat.run,
    nga.SOURCE: nga.run,
}


def get_adapter(source: str) -> AdapterRunner:
    try:
        return ADAPTERS[source]
    except KeyError as exc:
        raise ValueError(f"unsupported sentiment source: {source}") from exc


__all__ = ["AdapterRequest", "AdapterResult", "get_adapter"]
