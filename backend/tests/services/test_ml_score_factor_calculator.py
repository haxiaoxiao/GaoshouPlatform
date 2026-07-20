"""ML score factor calculator tests."""

from datetime import date

from app.services import ml_score_factor_calculator as calculator


def test_trading_dates_use_configured_parquet_directory(monkeypatch, tmp_path):
    parquet_root = tmp_path / "configured parquet"
    captured: dict[str, str] = {}

    class FakeResult:
        @staticmethod
        def fetchall():
            return [("2025-01-02",), ("2025-01-03",)]

    class FakeDuckDb:
        @staticmethod
        def execute(sql):
            captured["sql"] = sql
            return FakeResult()

    monkeypatch.setattr(calculator.settings, "parquet_data_dir", str(parquet_root))
    monkeypatch.setattr(calculator, "get_duckdb", lambda: FakeDuckDb())

    result = calculator._trading_dates(date(2025, 1, 1), date(2025, 1, 31))

    expected_glob = str(parquet_root / "klines_daily" / "**" / "*.parquet").replace(
        "\\", "/"
    )
    assert expected_glob in captured["sql"]
    assert "E:/Projects/Data" not in captured["sql"]
    assert result == [date(2025, 1, 2), date(2025, 1, 3)]
