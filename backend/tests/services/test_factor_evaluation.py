"""因子评估服务测试"""
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from app.models.factor import BoardQuery, BoardRow
from app.services.factor_evaluation import FactorEvaluationService


def _make_fake_ohlcv(symbols, n_days=100):
    """生成模拟 OHLCV 数据，与 ClickHouse execute() 返回格式一致"""
    np.random.seed(42)
    start = date(2024, 1, 2)
    rows = []
    for sym in symbols:
        for i in range(n_days):
            rows.append((
                sym,
                start,
                10.0 + 0.01 * i,    # open
                11.0 + 0.01 * i,    # high
                9.0 + 0.01 * i,     # low
                10.5 + 0.01 * i,    # close
                1_000_000 + i * 100,  # volume
                10_500_000.0 + i * 1000,  # amount
                0.01 + 0.001 * i,   # turnover_rate
            ))
    return rows


def _make_fake_close_data(symbols, n_days=100):
    """生成模拟收盘价数据"""
    np.random.seed(42)
    start = date(2024, 1, 2)
    rows = []
    for sym in symbols:
        for i in range(n_days):
            rows.append((sym, start, 10.5 + 0.01 * i))
    return rows


@pytest.fixture
def mock_ch_client():
    """Mock ClickHouse 客户端 — 返回固定数据"""
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    ohlcv_rows = _make_fake_ohlcv(symbols, 100)
    close_rows = _make_fake_close_data(symbols, 100)

    mock_client = MagicMock()
    # Return ohlcv for the factor matrix query, close for return matrix query
    mock_client.execute.side_effect = [ohlcv_rows, close_rows]
    return mock_client


@pytest.fixture
def mock_evaluate_expression():
    """Mock evaluate_expression 返回已知因子值"""
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    dates = pd.date_range("2024-01-02", periods=100, freq="B")[:97]

    result = {}
    for sym in symbols:
        np.random.seed(42)
        values = np.random.randn(len(dates))
        result[sym] = pd.Series(values, index=dates)

    with patch("app.services.factor_evaluation.evaluate_expression", return_value=result):
        yield


@pytest.mark.asyncio
async def test_run_ic_analysis_structure(mock_ch_client, mock_evaluate_expression):
    """IC 分析返回结构完整性"""
    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_ch_client):
        service = FactorEvaluationService()
        result = await service.run_ic_analysis(
            expression="Mean($close, 5)",
            symbols=["000001.SZ", "000002.SZ", "000003.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
        )

    # IC 序列
    assert "ic_series" in result
    assert isinstance(result["ic_series"], list)

    # IC 统计量
    assert "ic_stats" in result
    stats = result["ic_stats"]
    assert "mean" in stats
    assert "std" in stats
    assert "icir" in stats
    assert "positive_rate" in stats
    assert isinstance(stats["mean"], float)
    assert isinstance(stats["icir"], float)

    # IC 衰减
    assert "ic_decay" in result
    decay = result["ic_decay"]
    assert len(decay) == 5  # lags [1, 3, 5, 10, 20]
    for entry in decay:
        assert "lag" in entry
        assert "ic_mean" in entry
        assert isinstance(entry["lag"], int)
        assert isinstance(entry["ic_mean"], float)


@pytest.mark.asyncio
async def test_run_ic_analysis_empty_data(mock_ch_client):
    """空数据时 IC 分析返回默认值"""
    mock_empty = MagicMock()
    mock_empty.execute.return_value = []

    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_empty):
        service = FactorEvaluationService()
        result = await service.run_ic_analysis(
            expression="Mean($close, 5)",
            symbols=["000001.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
        )

    assert result["ic_series"] == []
    assert result["ic_stats"]["mean"] == 0.0
    assert result["ic_stats"]["std"] == 0.0
    assert result["ic_stats"]["icir"] == 0.0
    assert result["ic_stats"]["positive_rate"] == 0.0
    assert len(result["ic_decay"]) == 5


@pytest.mark.asyncio
async def test_run_quantile_backtest_delegates(mock_ch_client):
    """分层回测委托给 BacktestRunner"""
    from app.backtest.config import BacktestResult

    fake_result = BacktestResult(
        total_return=0.15,
        annual_return=0.08,
        sharpe_ratio=1.2,
        max_drawdown=-0.05,
        n_trading_days=100,
    )

    mock_runner = AsyncMock()
    mock_runner.run.return_value = fake_result

    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_ch_client), \
         patch("app.services.factor_evaluation.get_backtest_runner", return_value=mock_runner):
        service = FactorEvaluationService()
        result = await service.run_quantile_backtest(
            expression="Mean($close, 5)",
            symbols=["000001.SZ", "000002.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
            n_groups=5,
            rebalance_freq="monthly",
        )

    # Verify delegation — runner.run should be called with a BacktestConfig
    assert mock_runner.run.called
    call_args = mock_runner.run.call_args
    config = call_args[0][0]
    assert config.mode == "vectorized"
    assert config.n_groups == 5
    assert config.rebalance_freq == "monthly"
    assert config.factor_expression == "Mean($close, 5)"

    # Result shape
    assert "total_return" in result
    assert result["total_return"] == 0.15
    assert "sharpe_ratio" in result
    assert result["sharpe_ratio"] == 1.2


@pytest.mark.asyncio
async def test_run_full_report_merges(mock_ch_client):
    """完整报告合并 IC 分析和分层回测结果"""
    from app.backtest.config import BacktestResult

    fake_result = BacktestResult(
        total_return=0.15,
        annual_return=0.08,
        sharpe_ratio=1.2,
        max_drawdown=-0.05,
        n_trading_days=100,
    )
    mock_runner = AsyncMock()
    mock_runner.run.return_value = fake_result

    with patch("app.services.factor_evaluation.get_ch_client", return_value=mock_ch_client), \
         patch("app.services.factor_evaluation.get_backtest_runner", return_value=mock_runner), \
         patch("app.services.factor_evaluation.evaluate_expression") as mock_eval_expr:
        # Mock evaluate_expression to return known data
        symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
        dates = pd.date_range("2024-01-02", periods=97, freq="B")
        result_dict = {}
        for sym in symbols:
            result_dict[sym] = pd.Series(np.random.randn(len(dates)), index=dates)
        mock_eval_expr.return_value = result_dict

        service = FactorEvaluationService()
        result = await service.run_full_report(
            expression="Mean($close, 5)",
            symbols=["000001.SZ", "000002.SZ", "000003.SZ"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 4, 30),
            n_groups=5,
            rebalance_freq="monthly",
        )

    # Top-level fields
    assert "expression" in result
    assert result["expression"] == "Mean($close, 5)"
    assert "parameters" in result
    assert "ic_analysis" in result
    assert "quantile_backtest" in result
    assert "summary" in result

    # Summary merges IC and backtest metrics
    summary = result["summary"]
    assert "ic_mean" in summary
    assert "icir" in summary
    assert "long_short_annual_return" in summary
    assert "long_short_sharpe" in summary
    assert "max_drawdown" in summary


def test_sort_and_page_rows_handles_none_metrics() -> None:
    service = FactorEvaluationService()
    query = BoardQuery(sort_by="latest_ic_mean", sort_order="desc", page=1, page_size=10)
    rows = [
        BoardRow(
            factor_name="alpha101_001",
            category="alpha101",
            latest_ic_mean=None,
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_002",
            category="alpha101",
            latest_ic_mean=0.12,
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.12,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_003",
            category="alpha101",
            latest_ic_mean=-0.05,
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=-0.05,
            ir=0.0,
        ),
    ]

    ordered = service._sort_and_page_rows(rows, query)

    assert [row.factor_name for row in ordered] == ["alpha101_002", "alpha101_003", "alpha101_001"]


def test_filter_board_rows_matches_factor_cache_metadata() -> None:
    service = FactorEvaluationService()
    rows = [
        BoardRow(
            factor_name="alpha101_001",
            display_name="Alpha101 #001",
            source="catalog.alpha101",
            factor_group="alpha101",
            factor_group_display_name="Alpha101",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="paper_pb_roe_residual",
            display_name="PB-ROE 残差",
            source="catalog.cn_paper",
            factor_group="cn_paper_fundamental",
            factor_group_display_name="研报基本面因子",
            category="cn_paper_fundamental",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
    ]

    filtered = service._filter_board_rows(rows, BoardQuery(factor_keyword="pb_roe"))

    assert [row.factor_name for row in filtered] == ["paper_pb_roe_residual"]


def test_board_research_payload_preserves_matching_settings() -> None:
    service = FactorEvaluationService()
    query = BoardQuery(
        stock_pool="zz800",
        start_date=date(2020, 1, 1),
        end_date=date(2026, 5, 20),
        fee_rate=0.003,
        stamp_tax_rate=0.001,
        transfer_fee_rate=0.0,
        slippage=0.001,
        filter_limit_up=True,
        filter_limit_down=True,
        group_count=5,
        direction="desc",
        pool_membership_mode="static_latest",
        factor_value_params_hashes={"paper_pb_roe_residual": "bf21a9e8fbc5a384"},
        outlier_handling="winsorize",
        industry_neutralization=True,
        standardize=True,
    )

    payload = service._board_research_payload("paper_pb_roe_residual", query)

    assert payload["stock_pool_value"] == "zz800"
    assert payload["factor_value_params_hash"] == "bf21a9e8fbc5a384"
    assert payload["filter_limit_down"] is True
    assert payload["group_count"] == 5
    assert payload["direction"] == "desc"
    assert payload["outlier_handling"] == "winsorize"
    assert payload["industry_neutralization"] is True
    assert payload["standardize"] is True


@pytest.mark.asyncio
async def test_board_query_sorts_after_latest_metrics_attached(monkeypatch) -> None:
    service = FactorEvaluationService()
    query = BoardQuery(
        factor_groups=["alpha101"],
        sort_by="latest_ic_mean",
        sort_order="desc",
        page=1,
        page_size=2,
    )
    rows = [
        BoardRow(
            factor_name="alpha101_001",
            category="alpha101",
            latest_ic_mean=None,
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_002",
            category="alpha101",
            latest_ic_mean=None,
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_003",
            category="alpha101",
            latest_ic_mean=None,
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
    ]

    async def fake_saved_factor_board_rows(_query):
        return list(rows)

    def fake_attach_board_coverage(_rows, _query):
        return None

    async def fake_attach_latest_research_runs(target_rows, _query):
        metrics = {
            "alpha101_001": -0.01,
            "alpha101_002": 0.15,
            "alpha101_003": 0.03,
        }
        for row in target_rows:
            row.latest_ic_mean = metrics[row.factor_name]
            row.ic_mean = metrics[row.factor_name]

    monkeypatch.setattr(service, "_saved_factor_board_rows", fake_saved_factor_board_rows)
    monkeypatch.setattr(service, "_attach_board_coverage", fake_attach_board_coverage)
    monkeypatch.setattr(service, "_attach_latest_research_runs", fake_attach_latest_research_runs)

    result = await service.board_query(query)

    assert [row.factor_name for row in result.rows] == ["alpha101_002", "alpha101_003"]


@pytest.mark.asyncio
async def test_board_query_attaches_paged_coverage_for_latest_sort(monkeypatch) -> None:
    service = FactorEvaluationService()
    query = BoardQuery(
        factor_groups=["alpha101"],
        sort_by="latest_ic_mean",
        sort_order="desc",
        page=1,
        page_size=2,
    )
    rows = [
        BoardRow(
            factor_name="alpha101_001",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_002",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_003",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
    ]
    coverage_calls: list[list[str]] = []

    async def fake_saved_factor_board_rows(_query):
        return list(rows)

    async def fake_attach_latest_research_runs(target_rows, _query):
        metrics = {
            "alpha101_001": -0.01,
            "alpha101_002": 0.15,
            "alpha101_003": 0.03,
        }
        for row in target_rows:
            row.latest_ic_mean = metrics[row.factor_name]
            row.ic_mean = metrics[row.factor_name]

    def fake_attach_board_coverage(target_rows, _query):
        coverage_calls.append([row.factor_name for row in target_rows])
        for row in target_rows:
            row.coverage_status = "covered"

    monkeypatch.setattr(service, "_saved_factor_board_rows", fake_saved_factor_board_rows)
    monkeypatch.setattr(service, "_attach_latest_research_runs", fake_attach_latest_research_runs)
    monkeypatch.setattr(service, "_attach_board_coverage", fake_attach_board_coverage)

    result = await service.board_query(query)

    assert [row.factor_name for row in result.rows] == ["alpha101_002", "alpha101_003"]
    assert coverage_calls == []


def test_attach_research_snapshot_coverage_from_summary() -> None:
    service = FactorEvaluationService()
    row = BoardRow(
        factor_name="alpha101_001",
        category="alpha101",
        min_quantile_excess_return=0.0,
        max_quantile_excess_return=0.0,
        min_quantile_turnover=0.0,
        max_quantile_turnover=0.0,
        ic_mean=0.0,
        ir=0.0,
    )

    service._attach_research_snapshot_coverage(
        row,
        {
            "active_symbol_count": 960,
            "coverage_ratio": 0.9982,
            "effective_start_date": "2025-05-26",
            "effective_end_date": "2026-05-19",
            "effective_trading_days": 238,
        },
    )

    assert row.coverage_symbol_count == 960
    assert row.coverage_date_count == 238
    assert row.coverage_total_rows == 228069
    assert row.coverage_min_date == "2025-05-26"
    assert row.coverage_max_date == "2026-05-19"
    assert row.coverage_status == "covered"


@pytest.mark.asyncio
async def test_board_query_attaches_coverage_before_sort_for_coverage_sort(monkeypatch) -> None:
    service = FactorEvaluationService()
    query = BoardQuery(
        factor_groups=["alpha101"],
        sort_by="coverage_total_rows",
        sort_order="desc",
        page=1,
        page_size=2,
    )
    rows = [
        BoardRow(
            factor_name="alpha101_001",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_002",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
        BoardRow(
            factor_name="alpha101_003",
            category="alpha101",
            min_quantile_excess_return=0.0,
            max_quantile_excess_return=0.0,
            min_quantile_turnover=0.0,
            max_quantile_turnover=0.0,
            ic_mean=0.0,
            ir=0.0,
        ),
    ]
    coverage_calls: list[list[str]] = []

    async def fake_saved_factor_board_rows(_query):
        return list(rows)

    async def fake_attach_latest_research_runs(_target_rows, _query):
        return None

    def fake_attach_board_coverage(target_rows, _query):
        coverage_calls.append([row.factor_name for row in target_rows])
        values = {
            "alpha101_001": 5,
            "alpha101_002": 20,
            "alpha101_003": 10,
        }
        for row in target_rows:
            row.coverage_total_rows = values[row.factor_name]

    monkeypatch.setattr(service, "_saved_factor_board_rows", fake_saved_factor_board_rows)
    monkeypatch.setattr(service, "_attach_latest_research_runs", fake_attach_latest_research_runs)
    monkeypatch.setattr(service, "_attach_board_coverage", fake_attach_board_coverage)

    result = await service.board_query(query)

    assert coverage_calls == [["alpha101_001", "alpha101_002", "alpha101_003"]]
    assert [row.factor_name for row in result.rows] == ["alpha101_002", "alpha101_003"]
