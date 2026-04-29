"""算子单元测试"""
import numpy as np
import pandas as pd
import pytest
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry

# 注册所有算子
OperatorRegistry.clear()
from app.compute.operators import raw_fields  # noqa
from app.compute.operators import math_ops    # noqa


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
