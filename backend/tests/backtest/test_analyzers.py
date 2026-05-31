from __future__ import annotations

import pandas as pd

from app.backtest.analyzers import compute_ic_series


def test_compute_ic_series_does_not_require_scipy() -> None:
    dates = pd.to_datetime(["2024-01-02", "2024-01-03"])
    symbols = [f"0000{idx:02d}.SZ" for idx in range(12)]
    factor = pd.DataFrame(
        [list(range(12)), list(range(11, -1, -1))],
        index=dates,
        columns=symbols,
        dtype=float,
    )
    returns = pd.DataFrame(
        [list(range(12)), list(range(11, -1, -1))],
        index=dates,
        columns=symbols,
        dtype=float,
    )

    result = compute_ic_series(factor, returns)

    assert result.to_dict() == {"2024-01-02": 1.0, "2024-01-03": 1.0}
