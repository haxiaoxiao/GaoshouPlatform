import pandas as pd
import pytest

from app.api import data_explorer, parquet_explorer


def _make_partitioned_dataset(root, dataset: str) -> None:
    part = root / dataset / "year=2026" / "month=05"
    part.mkdir(parents=True)
    (part / "part-0.parquet").write_text("placeholder", encoding="utf-8")


def test_data_explorer_tables_use_partition_metadata(monkeypatch, tmp_path):
    _make_partitioned_dataset(tmp_path, "klines_daily")

    class ExplodingDuckDB:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("list_tables should not run DuckDB counts")

    monkeypatch.setattr(data_explorer.settings, "market_data_backend", "parquet")
    monkeypatch.setattr(data_explorer.settings, "parquet_data_dir", str(tmp_path))
    monkeypatch.setattr(data_explorer, "get_duckdb", lambda: ExplodingDuckDB())

    result = data_explorer.list_tables()

    assert result["code"] == 0
    assert result["data"] == [
        {
            "name": "klines_daily",
            "label": "A 股日线",
            "category": "market",
            "symbol_column": "symbol",
            "description": "A 股 OHLCV 日频行情。",
            "factor_ready": False,
            "exact_summary": False,
            "row_count": None,
            "rows": None,
            "count": None,
            "min_date": "2026-05-01",
            "max_date": "2026-05-31",
            "date_column": "trade_date",
            "estimated": True,
            "partition_count": 1,
        }
    ]


def test_data_explorer_jq_moneyflow_uses_trade_date_1(monkeypatch, tmp_path):
    _make_partitioned_dataset(tmp_path, "jq_money_flow_daily")

    class FakeDuckDB:
        last_sql = ""

        def execute(self, sql, *_args, **_kwargs):
            self.last_sql = str(sql)
            return self

        def df(self):
            return pd.DataFrame(columns=["symbol", "trade_date", "trade_date_1", "net_amount_main"])

        def fetchall(self):
            return [("000001.SZ", None, "2026-04-17", 1.0)]

    fake = FakeDuckDB()
    monkeypatch.setattr(data_explorer.settings, "market_data_backend", "parquet")
    monkeypatch.setattr(data_explorer.settings, "parquet_data_dir", str(tmp_path))
    monkeypatch.setattr(data_explorer, "get_duckdb", lambda: fake)

    result = data_explorer.search_table(
        "jq_money_flow_daily",
        data_explorer.ExplorerSearchRequest(
            quick_search={"symbol": "000001.SZ", "start_date": "2026-04-01", "end_date": "2026-04-17"},
        ),
    )

    assert result["code"] == 0
    assert '"trade_date_1" BETWEEN' in result["data"]["generated_sql"]
    assert '"trade_date" BETWEEN' not in result["data"]["generated_sql"]


def test_data_explorer_preview_skips_count_by_default(monkeypatch, tmp_path):
    _make_partitioned_dataset(tmp_path, "klines_daily")

    class FakeDuckDB:
        def execute(self, sql, *_args, **_kwargs):
            assert "count(" not in str(sql).lower()
            return self

        def df(self):
            return pd.DataFrame(columns=["symbol", "trade_date"])

        def fetchall(self):
            return [("000001.SZ", "2026-05-01")]

    monkeypatch.setattr(data_explorer.settings, "market_data_backend", "parquet")
    monkeypatch.setattr(data_explorer.settings, "parquet_data_dir", str(tmp_path))
    monkeypatch.setattr(data_explorer, "get_duckdb", lambda: FakeDuckDB())

    result = data_explorer.preview_table(
        "klines_daily",
        page=1,
        page_size=50,
        order_dir="ASC",
        include_total=False,
    )

    assert result["code"] == 0
    assert result["data"]["total_estimated"] is True
    assert result["data"]["total"] == 1
    assert result["data"]["rows"] == [{"symbol": "000001.SZ", "trade_date": "2026-05-01"}]


@pytest.mark.asyncio
async def test_parquet_explorer_datasets_use_partition_metadata(monkeypatch, tmp_path):
    _make_partitioned_dataset(tmp_path, "klines_daily")

    monkeypatch.setattr(parquet_explorer.settings, "parquet_data_dir", str(tmp_path))

    result = await parquet_explorer.list_datasets()

    assert result["code"] == 0
    assert result["data"][0]["name"] == "klines_daily"
    assert result["data"][0]["row_count"] is None
    assert result["data"][0]["estimated"] is True
    assert result["data"][0]["partition_count"] == 1


@pytest.mark.asyncio
async def test_parquet_explorer_preview_skips_count_by_default(monkeypatch, tmp_path):
    _make_partitioned_dataset(tmp_path, "klines_daily")

    class FakeDuckDB:
        def execute(self, sql, *_args, **_kwargs):
            assert "count(" not in str(sql).lower()
            return self

        def df(self):
            return pd.DataFrame([{"symbol": "000001.SZ", "trade_date": "2026-05-01"}])

    monkeypatch.setattr(parquet_explorer.settings, "parquet_data_dir", str(tmp_path))
    monkeypatch.setattr(parquet_explorer, "get_duckdb", lambda: FakeDuckDB())

    result = await parquet_explorer.dataset_preview(
        "klines_daily",
        limit=20,
        offset=0,
        include_total=False,
    )

    assert result["code"] == 0
    assert result["data"]["total_estimated"] is True
    assert result["data"]["total_rows"] == 1
