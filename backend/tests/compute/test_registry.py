"""算子注册表测试"""
import pytest
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry


class TestOperatorRegistry:
    def setup_method(self):
        OperatorRegistry.clear()

    def test_register_and_get(self):
        op = RawFieldOperator("close", "close", "收盘价")
        OperatorRegistry.register(op)
        assert OperatorRegistry.get("close") is op

    def test_all(self):
        o1 = RawFieldOperator("close", "close")
        o2 = RawFieldOperator("open", "open")
        OperatorRegistry.register(o1)
        OperatorRegistry.register(o2)
        assert len(OperatorRegistry.all()) == 2

    def test_by_level(self):
        OperatorRegistry.register(RawFieldOperator("close", "close"))
        assert len(OperatorRegistry.by_level(0)) == 1

    def test_to_api_list(self):
        OperatorRegistry.register(RawFieldOperator("close", "close", "收盘价"))
        api_list = OperatorRegistry.to_api_list()
        assert len(api_list) == 1
        assert api_list[0]["name"] == "close"
        assert api_list[0]["level"] == 0

    def test_register_overwrite(self):
        o1 = RawFieldOperator("close", "close")
        o2 = RawFieldOperator("close", "new_close")
        OperatorRegistry.register(o1)
        OperatorRegistry.register(o2)
        assert OperatorRegistry.get("close") is o2
