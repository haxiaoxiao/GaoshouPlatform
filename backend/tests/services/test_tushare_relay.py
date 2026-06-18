from app.services.tushare_relay import parse_relay_rows
from app.services.tushare_relay_sync import (
    ANALYST_RELAY_DATASETS,
    FINANCIAL_STATEMENT_RELAY_DATASETS,
    INSTITUTION_RELAY_DATASETS,
    STRUCTURED_RELAY_DATASETS,
    build_sync_catalog,
    _normalize_dataset_rows,
)


def test_parse_native_tushare_envelope() -> None:
    payload = {
        "code": 0,
        "data": {
            "fields": ["ts_code", "trade_date", "adj_factor"],
            "items": [["000001.SZ", "20240506", 0.8672]],
        },
    }

    assert parse_relay_rows(payload) == [
        {"ts_code": "000001.SZ", "trade_date": "20240506", "adj_factor": 0.8672}
    ]


def test_sync_catalog_exposes_relay_guardrails() -> None:
    catalog = build_sync_catalog()
    dataset_names = {item["name"] for item in catalog["datasets"]}
    presets = {item["name"]: item for item in catalog["presets"]}

    assert set(STRUCTURED_RELAY_DATASETS).issubset(dataset_names)
    assert set(ANALYST_RELAY_DATASETS).issubset(dataset_names)
    assert set(INSTITUTION_RELAY_DATASETS).issubset(dataset_names)
    assert set(FINANCIAL_STATEMENT_RELAY_DATASETS).issubset(dataset_names)
    assert "ths_concept" in dataset_names
    assert "dividend" not in dataset_names
    assert presets["relay_structured"]["relay_datasets"] == list(STRUCTURED_RELAY_DATASETS)
    assert presets["relay_analyst"]["relay_datasets"] == [*ANALYST_RELAY_DATASETS, "stock_research_report_em"]
    assert presets["relay_institution"]["relay_datasets"] == list(INSTITUTION_RELAY_DATASETS)
    assert presets["relay_financial_statement"]["relay_datasets"] == list(FINANCIAL_STATEMENT_RELAY_DATASETS)
    assert presets["relay_text"]["include_by_default"] is False
    assert catalog["guardrails"]["news_default_daily_limit"] == 200


def test_normalize_analyst_rank_rows() -> None:
    frame = _normalize_dataset_rows(
        "analyst_rank",
        [
            {
                "\u5206\u6790\u5e08\u540d\u79f0": "\u4efb\u5fd7\u5f3a",
                "\u5206\u6790\u5e08\u5355\u4f4d": "\u534e\u798f\u8bc1\u5238",
                "\u5e74\u5ea6\u6307\u6570": 6424.01,
                "12\u4e2a\u6708\u6536\u76ca\u7387": 135.17,
                "\u5206\u6790\u5e08ID": "11000213851",
                "\u884c\u4e1a": "\u7535\u5b50",
                "\u66f4\u65b0\u65e5\u671f": "2024-12-31",
                "\u5e74\u5ea6": "2024",
            }
        ],
        {},
    )

    assert frame.iloc[0]["analyst_id"] == "11000213851"
    assert frame.iloc[0]["analyst_name"] == "\u4efb\u5fd7\u5f3a"
    assert str(frame.iloc[0]["update_date"].date()) == "2024-12-31"


def test_normalize_stock_research_report_rows() -> None:
    frame = _normalize_dataset_rows(
        "stock_research_report_em",
        [
            {
                "\u80a1\u7968\u4ee3\u7801": "000001",
                "\u80a1\u7968\u7b80\u79f0": "\u5e73\u5b89\u94f6\u884c",
                "\u62a5\u544a\u540d\u79f0": "2025\u5e74\u62a5\u70b9\u8bc4",
                "\u4e1c\u8d22\u8bc4\u7ea7": "\u4e2d\u6027",
                "\u673a\u6784": "\u56fd\u4fe1\u8bc1\u5238",
                "\u65e5\u671f": "2026-04-26",
                "\u62a5\u544aPDF\u94fe\u63a5": "https://example.test/report.pdf",
            }
        ],
        {},
    )

    assert frame.iloc[0]["symbol"] == "000001.SZ"
    assert frame.iloc[0]["title"] == "2025\u5e74\u62a5\u70b9\u8bc4"
    assert str(frame.iloc[0]["report_date"].date()) == "2026-04-26"
    assert frame.iloc[0]["title_hash"]


def test_normalize_hsgt_holding_rows() -> None:
    frame = _normalize_dataset_rows(
        "hk_hold",
        [
            {
                "trade_date": "20240614",
                "ts_code": "000001.SZ",
                "name": "\u5e73\u5b89\u94f6\u884c",
                "vol": "123456",
                "ratio": "2.34",
                "exchange": "sz",
            }
        ],
        {},
    )

    assert frame.iloc[0]["symbol"] == "000001.SZ"
    assert frame.iloc[0]["holding_volume"] == 123456
    assert frame.iloc[0]["holding_ratio"] == 2.34
    assert frame.iloc[0]["exchange"] == "SZ"


def test_normalize_fund_portfolio_rows() -> None:
    frame = _normalize_dataset_rows(
        "fund_portfolio",
        [
            {
                "ts_code": "000001.OF",
                "ann_date": "20240420",
                "end_date": "20240331",
                "symbol": "600519.SH",
                "mkv": "1024.5",
                "stk_mkv_ratio": "8.6",
            }
        ],
        {},
    )

    assert frame.iloc[0]["fund_code"] == "000001.OF"
    assert frame.iloc[0]["symbol"] == "600519.SH"
    assert str(frame.iloc[0]["end_date"].date()) == "2024-03-31"


def test_normalize_financial_statement_aliases() -> None:
    balancesheet = _normalize_dataset_rows(
        "balancesheet",
        [
            {
                "ts_code": "600519.SH",
                "f_ann_date": "20240403",
                "end_date": "20231231",
                "report_type": "1",
                "comp_type": "1",
                "intan_assets": "100.5",
                "goodwill": "20.0",
                "total_hldr_eqy_exc_min_int": "5000",
            }
        ],
        {},
    )
    cashflow = _normalize_dataset_rows(
        "cashflow",
        [
            {
                "ts_code": "600519.SH",
                "f_ann_date": "20240403",
                "end_date": "20231231",
                "n_cashflow_act": "300.0",
                "c_pay_acq_const_fiolta": "25.5",
            }
        ],
        {},
    )

    assert balancesheet.iloc[0]["symbol"] == "600519.SH"
    assert balancesheet.iloc[0]["intangible_assets"] == 100.5
    assert balancesheet.iloc[0]["total_equity"] == 5000
    assert cashflow.iloc[0]["net_operate_cash_flow"] == 300.0
    assert cashflow.iloc[0]["capex"] == 25.5
