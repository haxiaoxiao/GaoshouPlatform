"""表达式引擎 — Tokenizer + Parser + Evaluator"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

import numpy as np
import pandas as pd


class TokenType(Enum):
    VARIABLE = auto()    # $close, $open ...
    NUMBER = auto()      # 14, 20, 5.5
    FUNCTION = auto()    # RSI, Mean, Std ...
    LPAREN = auto()      # (
    RPAREN = auto()      # )
    COMMA = auto()       # ,
    PLUS = auto()        # +
    MINUS = auto()       # -
    STAR = auto()        # *
    SLASH = auto()       # /
    LT = auto()          # <
    GT = auto()          # >
    LE = auto()          # <=
    GE = auto()          # >=
    EQ = auto()          # ==
    NE = auto()          # !=
    AND = auto()         # AND
    OR = auto()          # OR
    EOF = auto()         # end of input


@dataclass
class Token:
    type: TokenType
    value: str
    pos: int = 0  # position in source for error reporting


TOKEN_PATTERNS = [
    (r"\$[a-zA-Z_]\w*", TokenType.VARIABLE),
    (r"\d+\.?\d*", TokenType.NUMBER),
    (r"and\b", TokenType.AND),
    (r"or\b", TokenType.OR),
    (r"<=", TokenType.LE),
    (r">=", TokenType.GE),
    (r"==", TokenType.EQ),
    (r"!=", TokenType.NE),
    (r"<", TokenType.LT),
    (r">", TokenType.GT),
    (r"\+", TokenType.PLUS),
    (r"-", TokenType.MINUS),
    (r"\*", TokenType.STAR),
    (r"/", TokenType.SLASH),
    (r"\(", TokenType.LPAREN),
    (r"\)", TokenType.RPAREN),
    (r",", TokenType.COMMA),
]


class Tokenizer:
    """将表达式字符串分解为 Token 流"""

    def __init__(self, source: str):
        self.source = source
        self.pos = 0
        self.tokens: list[Token] = []

    def tokenize(self) -> list[Token]:
        tokens = []
        while self.pos < len(self.source):
            if self.source[self.pos].isspace():
                self.pos += 1
                continue
            # 尝试匹配所有 pattern
            matched = False
            for pattern, ttype in TOKEN_PATTERNS:
                m = re.match(pattern, self.source[self.pos:], re.IGNORECASE)
                if m:
                    val = m.group()
                    tokens.append(Token(ttype, val, self.pos))
                    self.pos += len(val)
                    matched = True
                    break
            if not matched:
                # 尝试匹配函数名（大写字母开头或字母+可选数字）
                m = re.match(r"[a-zA-Z_]\w*", self.source[self.pos:])
                if m:
                    val = m.group()
                    tokens.append(Token(TokenType.FUNCTION, val, self.pos))
                    self.pos += len(val)
                else:
                    raise SyntaxError(
                        f"Unexpected character '{self.source[self.pos]}' at position {self.pos}"
                    )
        tokens.append(Token(TokenType.EOF, "", self.pos))
        return tokens


# AST Nodes
class ASTNode:
    pass


@dataclass
class LiteralNode(ASTNode):
    value: float


@dataclass
class VariableNode(ASTNode):
    name: str  # e.g. "close" (strip $ prefix)


@dataclass
class FunctionCallNode(ASTNode):
    name: str
    args: list[ASTNode]


@dataclass
class BinaryOpNode(ASTNode):
    op: str  # "+", "-", "*", "/", "<", ">", "<=", ">=", "==", "!=", "AND", "OR"
    left: ASTNode
    right: ASTNode


@dataclass
class UnaryOpNode(ASTNode):
    op: str  # "-" for negation
    operand: ASTNode


class Parser:
    """递归下降解析器

    Grammar:
      comparison -> expression (("<"|">"|"<="|">="|"=="|"!=") expression)*
      expression -> term (("+"|"-") term)*
      term       -> factor (("*"|"/") factor)*
      factor     -> ("-") factor | atom
      atom       -> NUMBER | VARIABLE | FUNCTION "(" args ")" | "(" comparison ")"
      args       -> comparison ("," comparison)*
      and_or     -> comparison (("AND"|"OR") comparison)*
    """

    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> Token:
        return self.tokens[self.pos]

    def consume(self) -> Token:
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def expect(self, ttype: TokenType, msg: str = "") -> Token:
        t = self.peek()
        if t.type != ttype:
            raise SyntaxError(
                f"{msg or f'Expected {ttype}, got {t.type}'} at position {t.pos}: '{t.value}'"
            )
        return self.consume()

    def parse(self) -> ASTNode:
        node = self._parse_and_or()
        if self.peek().type != TokenType.EOF:
            raise SyntaxError(
                f"Unexpected token '{self.peek().value}' at position {self.peek().pos}"
            )
        return node

    def _parse_and_or(self) -> ASTNode:
        left = self._parse_comparison()
        while self.peek().type in (TokenType.AND, TokenType.OR):
            op = self.consume().value.upper()
            right = self._parse_comparison()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_comparison(self) -> ASTNode:
        left = self._parse_expression()
        while self.peek().type in (TokenType.LT, TokenType.GT, TokenType.LE,
                                     TokenType.GE, TokenType.EQ, TokenType.NE):
            op = self.consume().value
            right = self._parse_expression()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_expression(self) -> ASTNode:
        left = self._parse_term()
        while self.peek().type in (TokenType.PLUS, TokenType.MINUS):
            op = self.consume().value
            right = self._parse_term()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_term(self) -> ASTNode:
        left = self._parse_factor()
        while self.peek().type in (TokenType.STAR, TokenType.SLASH):
            op = self.consume().value
            right = self._parse_factor()
            left = BinaryOpNode(op, left, right)
        return left

    def _parse_factor(self) -> ASTNode:
        if self.peek().type == TokenType.MINUS:
            self.consume()
            operand = self._parse_factor()
            return UnaryOpNode("-", operand)
        return self._parse_atom()

    def _parse_atom(self) -> ASTNode:
        t = self.peek()
        if t.type == TokenType.NUMBER:
            self.consume()
            return LiteralNode(float(t.value))
        if t.type == TokenType.VARIABLE:
            self.consume()
            return VariableNode(t.value[1:])  # strip $
        if t.type == TokenType.FUNCTION:
            name = self.consume().value
            self.expect(TokenType.LPAREN, f"Expected '(' after function '{name}'")
            args = self._parse_args()
            self.expect(TokenType.RPAREN, f"Expected ')' after function args for '{name}'")
            return FunctionCallNode(name, args)
        if t.type == TokenType.LPAREN:
            self.consume()
            # 对于比较运算符，递归回 and_or（完整表达式）
            node = self._parse_and_or()
            self.expect(TokenType.RPAREN, "Expected ')'")
            return node
        raise SyntaxError(f"Unexpected token '{t.value}' at position {t.pos}")

    def _parse_args(self) -> list[ASTNode]:
        args = []
        if self.peek().type == TokenType.RPAREN:
            return args
        args.append(self._parse_and_or())
        while self.peek().type == TokenType.COMMA:
            self.consume()
            args.append(self._parse_and_or())
        return args


class Evaluator:
    """AST 求值器 — 递归遍历语法树执行计算"""

    # 二元运算符映射
    _BINOPS = {
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a / b.replace(0, np.nan),
        "<": lambda a, b: a < b,
        ">": lambda a, b: a > b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "AND": lambda a, b: a & b,
        "OR": lambda a, b: a | b,
    }

    def __init__(self, data: dict[str, pd.Series]):
        """
        Args:
            data: {symbol -> DataFrame(index=trade_date, columns=open,high,low,close,volume,amount,turnover)}
                  or single symbol case: 直接在 context 中传入 Series
        """
        self.data = data
        self._cache: dict[str, Any] = {}  # 当前求值过程的内存缓存

    def evaluate(self, node: ASTNode) -> pd.Series | pd.DataFrame:
        """求值入口"""

        if isinstance(node, LiteralNode):
            return self._eval_literal(node)
        if isinstance(node, VariableNode):
            return self._eval_variable(node)
        if isinstance(node, FunctionCallNode):
            return self._eval_function(node)
        if isinstance(node, BinaryOpNode):
            return self._eval_binary(node)
        if isinstance(node, UnaryOpNode):
            return self._eval_unary(node)
        raise TypeError(f"Unknown AST node: {type(node)}")

    def _eval_literal(self, node: LiteralNode):
        return node.value

    def _eval_variable(self, node: VariableNode):
        """L0 原始字段 — 从数据中提取列"""
        field = node.name
        from app.compute.operators.registry import OperatorRegistry

        op = OperatorRegistry.get(field)
        if op is not None and op.level == 0:
            # 对每个 symbol 的 DataFrame 提取列
            results = {}
            for key, df in self.data.items():
                results[key] = op.evaluate(df)
            if len(results) == 1:
                return list(results.values())[0]
            return results
        raise NameError(f"Unknown field '${field}'")

    def _eval_function(self, node: FunctionCallNode):
        from app.compute.operators.registry import OperatorRegistry

        op = OperatorRegistry.get(node.name)
        if op is None:
            raise NameError(f"Unknown function '{node.name}'")

        # 求值所有参数
        args = [self.evaluate(arg) for arg in node.args]

        # 对于 std、rolling 等需要原生 Python 数据类型的参数,
        # 第 2 个之后的位置参数通常是数值
        kwargs = {}
        if len(args) >= 1:
            kwargs["series"] = args[0]
        if len(args) >= 2:
            kwargs["period"] = int(args[1]) if isinstance(args[1], (int, float)) else args[1]
        if node.name in ("Corr", "Cov"):
            kwargs["series_a"] = args[0]
            if len(args) >= 2:
                kwargs["series_b"] = args[1]
            if len(args) >= 3:
                kwargs["period"] = int(args[2]) if isinstance(args[2], (int, float)) else args[2]
        if node.name == "ATR":
            kwargs["high_series"] = args[0]
            kwargs["low_series"] = args[1]
            if len(args) >= 3:
                kwargs["close_series"] = args[2]
            if len(args) >= 4:
                kwargs["period"] = int(args[3]) if isinstance(args[3], (int, float)) else args[3]
        if node.name in ("CCI", "WILLR"):
            kwargs["high"] = args[0]
            kwargs["low"] = args[1]
            kwargs["close"] = args[2]
            if len(args) >= 4:
                kwargs["period"] = int(args[3]) if isinstance(args[3], (int, float)) else args[3]
        if node.name == "OBV":
            kwargs["close"] = args[0]
            kwargs["volume"] = args[1]

        # 生成缓存 key
        cache_key = self._make_key(node)
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = op.evaluate(None, **kwargs)
        self._cache[cache_key] = result
        return result

    def _eval_binary(self, node: BinaryOpNode):
        left = self.evaluate(node.left)
        right = self.evaluate(node.right)
        op_fn = self._BINOPS.get(node.op)
        if op_fn is None:
            raise ValueError(f"Unknown operator '{node.op}'")
        # 处理 literal + series 的情况
        if isinstance(left, (int, float)) and isinstance(right, pd.Series):
            left = pd.Series(left, index=right.index)
        if isinstance(right, (int, float)) and isinstance(left, pd.Series):
            right = pd.Series(right, index=left.index)
        return op_fn(left, right)

    def _eval_unary(self, node: UnaryOpNode):
        operand = self.evaluate(node.operand)
        if node.op == "-":
            return -operand
        raise ValueError(f"Unknown unary operator '{node.op}'")

    def _make_key(self, node: FunctionCallNode) -> str:
        """生成函数调用的缓存 key"""
        parts = [node.name]
        for arg in node.args:
            if isinstance(arg, VariableNode):
                parts.append(arg.name)
            elif isinstance(arg, LiteralNode):
                parts.append(str(arg.value))
            elif isinstance(arg, FunctionCallNode):
                parts.append(self._make_key(arg))
            else:
                parts.append("?")
        return ":".join(parts)


def evaluate_expression(
    expression: str,
    data: dict[str, pd.DataFrame],
) -> pd.Series | dict[str, pd.Series]:
    """便捷函数 — 解析并求值表达式

    Args:
        expression: 因子表达式，如 "Mean($close, 5) / Std($close, 20)"
        data: {symbol -> DataFrame} or single symbol

    Returns:
        单 symbol 返回 pd.Series，多 symbol 返回 dict[str, pd.Series]
    """
    tokens = Tokenizer(expression).tokenize()
    ast = Parser(tokens).parse()
    evaluator = Evaluator(data)
    return evaluator.evaluate(ast)


def validate_expression(expression: str) -> tuple[bool, str]:
    """校验表达式语法是否正确

    Returns:
        (is_valid, error_message)
    """
    try:
        tokens = Tokenizer(expression).tokenize()
        Parser(tokens).parse()
        return True, ""
    except SyntaxError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {e}"
