from datetime import date

import pandas as pd

from app.scripts import sync_tushare_daily_to_parquet


def test_fetch_adj_factors_normalizes_market_wide_response() -> None:
    class FakePro:
        def adj_factor(self, **kwargs):
            assert kwargs["trade_date"] == "20260710"
            return pd.DataFrame(
                [{"ts_code": "000001.SZ", "trade_date": "20260710", "adj_factor": "123.45"}]
            )

    assert hasattr(sync_tushare_daily_to_parquet, "fetch_adj_factors")
    result = sync_tushare_daily_to_parquet.fetch_adj_factors(FakePro(), date(2026, 7, 10))

    assert result.to_dict("records") == [
        {"symbol": "000001.SZ", "trade_date": date(2026, 7, 10), "adj_factor": 123.45}
    ]
