"""Tests for factor validator service."""
import pytest
from app.services.factor_validator import FactorValidator


class TestFactorValidator:
    def setup_method(self):
        self.validator = FactorValidator()

    def test_valid_simple_expression(self):
        result = self.validator.validate("$close")
        assert result["valid"] is True
        assert result["error"] is None

    def test_valid_complex_expression(self):
        result = self.validator.validate("(1/$close) + Mean($close, 20)")
        assert result["valid"] is True

    def test_invalid_syntax_unmatched_paren(self):
        result = self.validator.validate("($close + $open")
        assert result["valid"] is False
        assert result["error"] is not None

    def test_invalid_empty_expression(self):
        result = self.validator.validate("")
        assert result["valid"] is False
        assert "empty" in result["error"].lower()

    def test_invalid_unknown_operator(self):
        result = self.validator.validate("$close @ $open")
        assert result["valid"] is False

    def test_whitespace_only(self):
        result = self.validator.validate("   ")
        assert result["valid"] is False
