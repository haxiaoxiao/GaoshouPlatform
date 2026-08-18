from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from app.services import tushare_relay_sync
from app.services.dataset_manifest import DatasetManifest, write_dataset_manifest
from app.services.tushare_relay import TushareRelayMeta, TushareRelayResult, parse_relay_rows
from app.services.tushare_relay_sync import (
    ANALYST_RELAY_DATASETS,
    FINANCIAL_STATEMENT_RELAY_DATASETS,
    INSTITUTION_RELAY_DATASETS,
    STRUCTURED_RELAY_DATASETS,
    _estimate_total,
    _normalize_dataset_rows,
    build_sync_catalog,
)


def test_dataset_coverage_prefers_valid_manifest(tmp_path, monkeypatch):
    dataset_root = tmp_path / "klines_daily"
    write_dataset_manifest(
        dataset_root,
        DatasetManifest(
            dataset="klines_daily",
            generated_at=pd.Timestamp("2026-07-10 10:00:00").to_pydatetime(),
            file_count=2,
            byte_size=2048,
            row_count=321,
            partition_count=1,
            min_date="2026-07-01",
            max_date="2026-07-09",
            schema_hash="schema",
        ),
    )
    monkeypatch.setattr(tushare_relay_sync.settings, "parquet_data_dir", str(tmp_path))
    tushare_relay_sync._COVERAGE_CACHE.clear()

    coverage = tushare_relay_sync.dataset_coverage("klines_daily", "trade_date")

    assert coverage == {
        "row_count": 321,
        "min_date": "2026-07-01",
        "max_date": "2026-07-09",
        "estimated": False,
        "source": "manifest",
        "file_count": 2,
        "partition_count": 1,
        "validation_status": "valid",
    }


def test_dataset_coverage_uses_exact_partition_boundaries_with_cache(monkeypatch, tmp_path) -> None:
    part = tmp_path / "klines_daily" / "year=2026" / "month=06"
    part.mkdir(parents=True)
    pd.DataFrame(
        [
            {"symbol": "000001.SZ", "trade_date": date(2026, 6, 17), "close": 10.0},
            {"symbol": "000002.SZ", "trade_date": date(2026, 6, 18), "close": 20.0},
        ]
    ).to_parquet(part / "part-0.parquet", index=False)

    tushare_relay_sync._COVERAGE_CACHE.clear()
    monkeypatch.setattr(tushare_relay_sync.settings, "parquet_data_dir", str(tmp_path))

    result = tushare_relay_sync.dataset_coverage("klines_daily", "trade_date")
    assert result["estimated"] is True
    assert result["row_count"] is None
    assert result["min_date"] == "2026-06-17"
    assert result["max_date"] == "2026-06-18"

    class ExplodingDuckDB:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("cached coverage should not query DuckDB again")

    monkeypatch.setattr(tushare_relay_sync, "get_duckdb", lambda: ExplodingDuckDB())
    cached = tushare_relay_sync.dataset_coverage("klines_daily", "trade_date")
    assert cached == result


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

    assert "datasync" in dataset_names
    assert presets["daily"]["sync_types"] == ["datasync"]
    assert presets["daily"]["display_item_count"] == 4
    assert "日频市值" in presets["daily"]["description"]
    assert "涨跌停" in presets["daily"]["description"]
    assert "复权" in presets["daily"]["description"]
    assert "完整分钟线" in presets["daily"]["description"]
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


def test_sync_catalog_exposes_market_radar_relay_datasets() -> None:
    catalog = build_sync_catalog(refresh=True)
    datasets = {item["name"]: item for item in catalog["datasets"]}
    presets = {item["name"]: item for item in catalog["presets"]}

    expected = {
        "tushare_limit_list_d": ("tushare_limit_list_d", "trade_date"),
        "tushare_limit_step": ("tushare_limit_step", "trade_date"),
        "tushare_margin": ("tushare_margin", "trade_date"),
    }
    for name, (storage_dataset, date_col) in expected.items():
        assert name in datasets
        assert datasets[name]["category"] == "relay_market_radar"
        assert datasets[name]["storage_dataset"] == storage_dataset
        assert datasets[name]["date_col"] == date_col

    assert presets["market_radar"]["display_name"] == "市场雷达数据"
    assert presets["market_radar"]["relay_datasets"] == list(expected)


def test_normalize_market_radar_rows_adds_trade_date_dt_and_numeric_fields() -> None:
    limit_detail = _normalize_dataset_rows(
        "tushare_limit_list_d",
        [{"ts_code": "000001.SZ", "trade_date": "20260817", "limit": "U", "limit_times": "2"}],
        {},
    )
    limit_step = _normalize_dataset_rows(
        "tushare_limit_step",
        [{"ts_code": "000001.SZ", "trade_date": "20260817", "nums": "3"}],
        {},
    )
    margin = _normalize_dataset_rows(
        "tushare_margin",
        [{"trade_date": "20260817", "exchange_id": "SSE", "rzye": "123.4"}],
        {},
    )

    assert limit_detail.iloc[0]["symbol"] == "000001.SZ"
    assert str(limit_detail.iloc[0]["trade_date"].date()) == "2026-08-17"
    assert str(limit_detail.iloc[0]["trade_date_dt"].date()) == "2026-08-17"
    assert limit_detail.iloc[0]["limit_times"] == 2
    assert limit_step.iloc[0]["nums"] == 3
    assert margin.iloc[0]["exchange_id"] == "SSE"
    assert margin.iloc[0]["rzye"] == 123.4
    assert str(margin.iloc[0]["trade_date_dt"].date()) == "2026-08-17"


def test_market_radar_estimate_counts_one_unit_per_date() -> None:
    dates = [date(2026, 8, 17), date(2026, 8, 18)]

    for name in ("tushare_limit_list_d", "tushare_limit_step", "tushare_margin"):
        assert _estimate_total([name], dates, [], {}) == len(dates)


@pytest.mark.asyncio
async def test_market_radar_date_handler_requests_each_date_and_advances_progress() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class FakeClient:
        def request(self, api_name: str, params: dict[str, object]) -> TushareRelayResult:
            calls.append((api_name, params))
            return TushareRelayResult(
                rows=[{"ts_code": "000001.SZ"}],
                payload={},
                meta=TushareRelayMeta(
                    api_name=api_name,
                    base_url="https://relay.test",
                    status_code=200,
                    elapsed_ms=1,
                ),
            )

    progress = SimpleNamespace(current=0, total=2, details={})
    spec = SimpleNamespace(name="tushare_limit_list_d", api_name="limit_list_d")
    rows, metas = await tushare_relay_sync._sync_market_radar_dataset(
        FakeClient(),
        spec,
        dates=[date(2026, 8, 17), date(2026, 8, 18)],
        options={},
        progress=progress,
    )

    assert calls == [
        ("limit_list_d", {"trade_date": "20260817"}),
        ("limit_list_d", {"trade_date": "20260818"}),
    ]
    assert [row["trade_date"] for row in rows] == ["20260817", "20260818"]
    assert len(metas) == 2
    assert progress.current == 2


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
