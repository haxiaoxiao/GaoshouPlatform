from datetime import date

import pandas as pd

from app.services.factor_value_store import FactorValueStore


def test_factor_value_store_metadata_cache_invalidates_after_append(tmp_path):
    store = FactorValueStore(str(tmp_path))

    assert store.exists() is False

    written = store.append(pd.DataFrame([
        {
            "symbol": "000001.SZ",
            "trade_date": date(2026, 5, 1),
            "factor_name": "demo_factor",
            "value": 1.0,
        }
    ]))

    assert written == 1
    assert store.exists() is True
    assert "factor_name" in store._schema_columns()
