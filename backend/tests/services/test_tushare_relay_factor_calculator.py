from __future__ import annotations

from datetime import date

import pandas as pd

from app.services import tushare_relay_factor_calculator as calculator


def test_jq_moneyflow_direct_factor_uses_trade_date_1(monkeypatch, tmp_path):
    part = tmp_path / "jq_money_flow_daily" / "year=2026" / "month=04"
    part.mkdir(parents=True)
    (part / "part-0.parquet").write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(calculator.settings, "parquet_data_dir", str(tmp_path))
    monkeypatch.setattr(
        calculator,
        "_dataset_columns",
        lambda _pattern: {"symbol", "trade_date", "trade_date_1", "net_amount_main"},
    )

    captured: dict[str, str] = {}

    class FakeDuckDB:
        def execute(self, sql, *_args, **_kwargs):
            captured["sql"] = str(sql)
            return self

        def df(self):
            return pd.DataFrame(
                [
                    {
                        "symbol": "000001.SZ",
                        "trade_date": date(2026, 4, 17),
                        "value": 123.4,
                    }
                ]
            )

    monkeypatch.setattr(calculator, "get_duckdb", lambda: FakeDuckDB())

    rows = calculator._load_direct_factor(
        "jq_moneyflow_net_amount_main",
        date(2026, 4, 1),
        date(2026, 4, 17),
        ["000001.SZ"],
    )

    assert rows[0]["factor_name"] == "jq_moneyflow_net_amount_main"
    assert rows[0]["trade_date"] == date(2026, 4, 17)
    assert 'CAST("trade_date_1" AS DATE)' in captured["sql"]
    assert 'CAST("trade_date" AS DATE)' not in captured["sql"]
    assert '"net_amount_main" AS value' in captured["sql"]
