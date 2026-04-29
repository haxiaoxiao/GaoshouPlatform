"""表达式引擎测试"""
import sys

import numpy as np
import pandas as pd
import pytest

from app.compute.operators.registry import OperatorRegistry


@pytest.fixture(autouse=True)
def _setup_operators():
    """每个测试前确保算子已注册（防止跨测试文件的注册表污染）"""
    OperatorRegistry.clear()
    for mod_name in list(sys.modules):
        if mod_name.startswith("app.compute.operators.") and mod_name not in (
            "app.compute.operators.base",
            "app.compute.operators.registry",
        ):
            sys.modules.pop(mod_name, None)
    import app.compute.operators.raw_fields  # noqa
    import app.compute.operators.math_ops   # noqa
    import app.compute.operators.rolling_ops  # noqa
    import app.compute.operators.ta_ops  # noqa


from app.compute.expression import (
    Tokenizer, TokenType, Parser, Evaluator,
    VariableNode, LiteralNode, FunctionCallNode, BinaryOpNode,
    evaluate_expression, validate_expression,
)


def make_test_data(n: int = 100) -> dict:
    """创建测试用的 OHLCV 数据"""
    np.random.seed(42)
    close = np.cumsum(np.random.randn(n) * 0.01) + 10.0
    open_ = close - np.random.randn(n) * 0.005
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.005)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.005)
    volume = np.abs(np.random.randn(n) * 1e6 + 5e6).astype(int)
    amount = volume * close * 0.8

    df = pd.DataFrame({
        "trade_date": pd.date_range("2025-01-01", periods=n, freq="B"),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "amount": amount,
        "turnover_rate": volume / 1e8,
    })
    df.set_index("trade_date", inplace=True)
    return {"test": df}


class TestTokenizer:
    def test_simple_variable(self):
        tokens = Tokenizer("$close").tokenize()
        assert tokens[0].type == TokenType.VARIABLE
        assert tokens[0].value == "$close"

    def test_arithmetic(self):
        tokens = Tokenizer("$close + $open * 2").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert types == [
            TokenType.VARIABLE, TokenType.PLUS,
            TokenType.VARIABLE, TokenType.STAR, TokenType.NUMBER,
        ]

    def test_function_call(self):
        tokens = Tokenizer("RSI($close, 14)").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert types == [
            TokenType.FUNCTION, TokenType.LPAREN,
            TokenType.VARIABLE, TokenType.COMMA, TokenType.NUMBER,
            TokenType.RPAREN,
        ]

    def test_comparison(self):
        tokens = Tokenizer("RSI($close, 14) < 30").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert TokenType.LT in types

    def test_nested_function(self):
        tokens = Tokenizer("Mean($close, 5)").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert TokenType.FUNCTION in types

    def test_logical(self):
        tokens = Tokenizer("a < 30 AND b > 70").tokenize()
        types = [t.type for t in tokens if t.type != TokenType.EOF]
        assert TokenType.AND in types


class TestParser:
    def test_variable(self):
        tokens = Tokenizer("$close").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, VariableNode)
        assert ast.name == "close"

    def test_addition(self):
        tokens = Tokenizer("$close + 1").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, BinaryOpNode)
        assert ast.op == "+"

    def test_precedence(self):
        # a + b * c => a + (b * c)
        tokens = Tokenizer("$close + $open * 2").tokenize()
        ast = Parser(tokens).parse()
        assert ast.op == "+"
        assert ast.right.op == "*"

    def test_function_call(self):
        tokens = Tokenizer("Mean($close, 20)").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, FunctionCallNode)
        assert ast.name == "Mean"
        assert len(ast.args) == 2

    def test_nested_function(self):
        tokens = Tokenizer("RSI(Mean($close, 5), 14)").tokenize()
        ast = Parser(tokens).parse()
        assert isinstance(ast, FunctionCallNode)
        assert ast.name == "RSI"
        inner = ast.args[0]
        assert isinstance(inner, FunctionCallNode)
        assert inner.name == "Mean"


class TestEvaluator:
    def test_raw_field(self):
        data = make_test_data(20)
        result = evaluate_expression("$close", data)
        assert len(result) == 20

    def test_arithmetic(self):
        data = make_test_data(20)
        result = evaluate_expression("$close - $open", data)
        assert len(result) == 20
        assert not result.isna().all()

    def test_mean(self):
        data = make_test_data(50)
        result = evaluate_expression("Mean($close, 10)", data)
        assert len(result) == 50
        assert not result.iloc[9:].isna().any()  # 第10个值开始有效

    def test_std(self):
        data = make_test_data(50)
        result = evaluate_expression("Std($close, 10)", data)
        assert len(result) == 50

    def test_rsi(self):
        data = make_test_data(50)
        result = evaluate_expression("RSI($close, 14)", data)
        assert len(result) == 50
        # RSI 应在 0-100 之间
        valid = result.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_composite_factor(self):
        data = make_test_data(50)
        result = evaluate_expression("(Mean($close, 5) - Mean($close, 20)) / Std($close, 20)", data)
        assert len(result) == 50

    def test_nested_rsi_delay(self):
        data = make_test_data(50)
        result = evaluate_expression("RSI($close, 14) - Delay(RSI($close, 14), 1)", data)
        assert len(result) == 50


class TestValidateExpression:
    def test_valid(self):
        ok, err = validate_expression("Mean($close, 20)")
        assert ok
        assert err == ""

    def test_invalid_syntax(self):
        ok, err = validate_expression("Mean($close, 20")
        assert not ok

    def test_invalid_char(self):
        ok, err = validate_expression("$close @ $open")
        assert not ok
