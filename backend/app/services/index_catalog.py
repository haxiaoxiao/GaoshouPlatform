from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from app.services.security_symbols import normalize_security_symbol, to_jq_symbol


@dataclass(frozen=True)
class IndexCatalogItem:
    symbol: str
    display_name: str
    provider: str
    provider_symbol: str
    market_family: str
    benchmark_enabled: bool
    pool_enabled: bool
    requires_daily_market_data: bool = True
    requires_components_when_pool: bool = False
    component_mode: str = "none"
    available_from: str | None = None
    notes: str = ""
    stock_pool_alias: str | None = None
    jq_symbol: str | None = None


def _pool_item(
    symbol: str,
    display_name: str,
    *,
    provider_symbol: str | None = None,
    available_from: str | None = None,
    notes: str = "",
    stock_pool_alias: str | None = None,
    jq_symbol: str | None = None,
    component_mode: str = "snapshot",
) -> IndexCatalogItem:
    provider_symbol = provider_symbol or symbol
    return IndexCatalogItem(
        symbol=symbol,
        display_name=display_name,
        provider="tushare.index_daily",
        provider_symbol=provider_symbol,
        market_family="cn_index",
        benchmark_enabled=True,
        pool_enabled=True,
        requires_components_when_pool=True,
        component_mode=component_mode,
        available_from=available_from,
        notes=notes,
        stock_pool_alias=stock_pool_alias,
        jq_symbol=jq_symbol,
    )


def _benchmark_item(
    symbol: str,
    display_name: str,
    *,
    provider: str = "tushare.index_daily",
    provider_symbol: str | None = None,
    market_family: str = "cn_index",
    available_from: str | None = None,
    notes: str = "",
    stock_pool_alias: str | None = None,
    jq_symbol: str | None = None,
) -> IndexCatalogItem:
    provider_symbol = provider_symbol or symbol
    return IndexCatalogItem(
        symbol=symbol,
        display_name=display_name,
        provider=provider,
        provider_symbol=provider_symbol,
        market_family=market_family,
        benchmark_enabled=True,
        pool_enabled=False,
        requires_components_when_pool=False,
        component_mode="none",
        available_from=available_from,
        notes=notes,
        stock_pool_alias=stock_pool_alias,
        jq_symbol=jq_symbol,
    )


INDEX_CATALOG: tuple[IndexCatalogItem, ...] = (
    _pool_item(
        "399101.SZ",
        "中小综指",
        available_from="2018-12-28",
        notes="Strict historical snapshots available for dynamic stock pool replay.",
        stock_pool_alias="small_cap",
        jq_symbol="399101.XSHE",
    ),
    _pool_item(
        "000300.SH",
        "沪深300",
        available_from="2018-12-28",
        stock_pool_alias="hs300",
        jq_symbol="000300.XSHG",
    ),
    _pool_item(
        "000905.SH",
        "中证500",
        available_from="2025-05-30",
        stock_pool_alias="zz500",
        jq_symbol="000905.XSHG",
    ),
    _pool_item(
        "000852.SH",
        "中证1000",
        available_from="2025-05-30",
        stock_pool_alias="zz1000",
        jq_symbol="000852.XSHG",
    ),
    _pool_item(
        "000906.SH",
        "中证800",
        available_from="2025-05-30",
        notes="Derived strictly from the point-in-time union of 000300.SH and 000905.SH snapshots.",
        stock_pool_alias="zz800",
        jq_symbol="000906.XSHG",
        component_mode="derived_union",
    ),
    _pool_item(
        "399006.SZ",
        "创业板指",
        notes="Growth board pool. Falls back to the latest available constituent snapshot when strict history is missing.",
        stock_pool_alias="chinext",
        jq_symbol="399006.XSHE",
    ),
    _pool_item(
        "399673.SZ",
        "创业板50",
        notes="Leading ChiNext names for growth/innovation factor research.",
        stock_pool_alias="chinext50",
        jq_symbol="399673.XSHE",
    ),
    _pool_item(
        "399102.SZ",
        "创业板综",
        notes="Broader ChiNext universe. Current constituents are usable when historical snapshots are incomplete.",
        stock_pool_alias="chinext_composite",
        jq_symbol="399102.XSHE",
    ),
    _pool_item(
        "000688.SH",
        "科创50",
        notes="STAR Market core pool. Falls back to the latest available constituent snapshot when strict history is missing.",
        stock_pool_alias="star50",
        jq_symbol="000688.XSHG",
    ),
    _pool_item(
        "000698.SH",
        "科创100",
        notes="STAR Market expanded core pool for technology-growth factor research.",
        stock_pool_alias="star100",
        jq_symbol="000698.XSHG",
    ),
    _benchmark_item("000001.SH", "上证指数"),
    _benchmark_item("399001.SZ", "深证成指"),
    _benchmark_item("000016.SH", "上证50"),
    _benchmark_item(
        "000985.SH",
        "中证全指",
        notes="Market-only index in this round. Strict historical constituents are not enabled.",
        stock_pool_alias="zz_quanzhi",
    ),
    _benchmark_item("932000.SH", "中证2000"),
    _benchmark_item("000922.SH", "中证红利"),
    _benchmark_item(
        "930955.SH",
        "红利低波100",
        provider_symbol="930955.CSI",
    ),
    _benchmark_item("000931.SH", "中证价值"),
    _benchmark_item("000918.SH", "中证成长"),
    _benchmark_item("801010.SI", "申万农林牧渔", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801020.SI", "申万采掘/煤炭", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801030.SI", "申万化工", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801040.SI", "申万钢铁", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801050.SI", "申万有色金属", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801080.SI", "申万电子", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801110.SI", "申万家用电器", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801120.SI", "申万食品饮料", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801130.SI", "申万纺织服装", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801150.SI", "申万医药生物", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801160.SI", "申万公用事业", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801170.SI", "申万交通运输", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801180.SI", "申万房地产", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801220.SI", "申万汽车", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801230.SI", "申万机械设备", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801240.SI", "申万电力设备", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801250.SI", "申万国防军工", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801270.SI", "申万计算机", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801280.SI", "申万传媒", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801290.SI", "申万通信", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801300.SI", "申万银行", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801310.SI", "申万非银金融", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801320.SI", "申万商贸零售", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801330.SI", "申万社会服务", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801350.SI", "申万建筑材料", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801360.SI", "申万建筑装饰", provider="tushare.sw_daily", market_family="sw_industry"),
    _benchmark_item("801370.SI", "申万轻工制造", provider="tushare.sw_daily", market_family="sw_industry"),
)


_INDEX_BY_SYMBOL: dict[str, IndexCatalogItem] = {}
_INDEX_BY_ALIAS: dict[str, IndexCatalogItem] = {}


def _register_lookup(key: str | None, item: IndexCatalogItem) -> None:
    if not key:
        return
    text = key.strip()
    if not text:
        return
    _INDEX_BY_SYMBOL[text.upper()] = item
    _INDEX_BY_ALIAS[text.lower()] = item


for _item in INDEX_CATALOG:
    _register_lookup(_item.symbol, _item)
    _register_lookup(_item.symbol.split(".", 1)[0], _item)
    _register_lookup(_item.provider_symbol, _item)
    _register_lookup(_item.provider_symbol.split(".", 1)[0], _item)
    _register_lookup(_item.stock_pool_alias, _item)
    _register_lookup(_item.jq_symbol, _item)


def get_index_item(symbol_or_alias: str | None) -> IndexCatalogItem | None:
    if not symbol_or_alias:
        return None
    normalized = normalize_security_symbol(symbol_or_alias) or str(symbol_or_alias).strip().upper()
    return _INDEX_BY_SYMBOL.get(normalized.upper()) or _INDEX_BY_ALIAS.get(symbol_or_alias.strip().lower())


def normalize_index_symbol(symbol_or_alias: str | None) -> str | None:
    item = get_index_item(symbol_or_alias)
    if item is not None:
        return item.symbol
    if not symbol_or_alias:
        return None
    return normalize_security_symbol(symbol_or_alias)


def jq_index_symbol(symbol_or_alias: str) -> str:
    item = get_index_item(symbol_or_alias)
    if item is not None and item.jq_symbol:
        return item.jq_symbol
    return to_jq_symbol(normalize_index_symbol(symbol_or_alias)) or symbol_or_alias


def list_index_catalog(*, benchmark_only: bool | None = None, pool_only: bool | None = None) -> list[dict[str, Any]]:
    items = list(INDEX_CATALOG)
    if benchmark_only is True:
        items = [item for item in items if item.benchmark_enabled]
    if pool_only is True:
        items = [item for item in items if item.pool_enabled]
    return [catalog_item_to_dict(item) for item in items]


def list_index_items(*, benchmark_only: bool | None = None, pool_only: bool | None = None) -> list[IndexCatalogItem]:
    items = list(INDEX_CATALOG)
    if benchmark_only is True:
        items = [item for item in items if item.benchmark_enabled]
    if pool_only is True:
        items = [item for item in items if item.pool_enabled]
    return items


def catalog_item_to_dict(item: IndexCatalogItem) -> dict[str, Any]:
    payload = asdict(item)
    if item.pool_enabled:
        payload["component_status"] = "available"
        payload["reason"] = None
    else:
        payload["component_status"] = "unavailable"
        payload["reason"] = "market_only_index" if item.benchmark_enabled else "strict_snapshot_missing"
    return payload
