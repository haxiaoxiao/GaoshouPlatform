from datetime import datetime

from app.api.backtest import _strategy_list_item


def test_strategy_list_item_omits_code_by_default():
    strategy = type(
        "StrategyRow",
        (),
        {
            "id": 1,
            "name": "Large strategy",
            "code": "x" * 500_000,
            "parameters": {"a": 1},
            "description": "summary",
            "created_at": datetime(2026, 7, 10, 10, 0),
            "updated_at": datetime(2026, 7, 10, 10, 1),
        },
    )()

    summary = _strategy_list_item(strategy)
    detail = _strategy_list_item(strategy, include_code=True)

    assert "code" not in summary
    assert summary["code_size"] == 500_000
    assert detail["code"] == strategy.code
