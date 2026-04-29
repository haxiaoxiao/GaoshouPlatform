"""算子单元测试"""
import sys
import numpy as np
import pandas as pd
import pytest
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry


@pytest.fixture(autouse=True)
def _setup_registry():
    """每个测试前清空并重新注册算子，避免与 test_registry.py 交叉污染"""
    OperatorRegistry.clear()
    # 先 pop 已缓存的模块，再重新 import 触发注册
    for mod_name in list(sys.modules):
        if mod_name.startswith("app.compute.operators.") and mod_name not in (
            "app.compute.operators.base",
            "app.compute.operators.registry",
        ):
            sys.modules.pop(mod_name, None)
    import app.compute.operators.raw_fields  # noqa
    import app.compute.operators.math_ops   # noqa


class TestRawFields:
    def test_close_field(self):
        op = OperatorRegistry.get("close")
        df = pd.DataFrame({"close": [10.0, 11.0, 12.0]})
        result = op.evaluate(df)
        pd.testing.assert_series_equal(result, pd.Series([10.0, 11.0, 12.0], name="close", dtype=float))

    def test_missing_column_raises(self):
        op = OperatorRegistry.get("close")
        df = pd.DataFrame({"open": [10.0]})
        with pytest.raises(KeyError):
            op.evaluate(df)


class TestDelay:
    def test_delay_1(self):
        op = OperatorRegistry.get("Delay")
        series = pd.Series([1.0, 2.0, 3.0, 4.0])
        result = op.evaluate(pd.DataFrame(), series=series, period=1)
        expected = pd.Series([np.nan, 1.0, 2.0, 3.0])
        pd.testing.assert_series_equal(result, expected)

    def test_delay_2(self):
        op = OperatorRegistry.get("Delay")
        series = pd.Series([1.0, 2.0, 3.0, 4.0])
        result = op.evaluate(pd.DataFrame(), series=series, period=2)
        expected = pd.Series([np.nan, np.nan, 1.0, 2.0])
        pd.testing.assert_series_equal(result, expected)


class TestRank:
    def test_rank_pct(self):
        op = OperatorRegistry.get("Rank")
        series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = op.evaluate(pd.DataFrame(), series=series)
        assert result.min() == 0.2
        assert result.max() == 1.0
