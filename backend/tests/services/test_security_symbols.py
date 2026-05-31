from app.services.index_catalog import get_index_item, jq_index_symbol, normalize_index_symbol
from app.services.security_symbols import normalize_security_symbol, to_jq_symbol


def test_normalize_security_symbol_uses_one_rule_for_stocks_and_indexes():
    assert normalize_security_symbol("600000.xshg") == "600000.SH"
    assert normalize_security_symbol("399101.XSHE") == "399101.SZ"
    assert normalize_security_symbol("930955.csi") == "930955.CSI"
    assert normalize_security_symbol("801010.si") == "801010.SI"
    assert normalize_security_symbol("sh600519") == "600519.SH"


def test_index_catalog_accepts_canonical_code_alias_and_jq_code():
    assert normalize_index_symbol("000300") == "000300.SH"
    assert normalize_index_symbol("000300.XSHG") == "000300.SH"
    assert jq_index_symbol("000300.SH") == "000300.XSHG"
    assert to_jq_symbol("399101.SZ") == "399101.XSHE"

    item = get_index_item("930955.CSI")
    assert item is not None
    assert item.symbol == "930955.SH"


def test_index_catalog_exposes_growth_board_stock_pools():
    expected = {
        "chinext": "399006.SZ",
        "chinext50": "399673.SZ",
        "chinext_composite": "399102.SZ",
        "star50": "000688.SH",
        "star100": "000698.SH",
    }
    for alias, symbol in expected.items():
        item = get_index_item(alias)
        assert item is not None
        assert item.symbol == symbol
        assert item.pool_enabled is True
