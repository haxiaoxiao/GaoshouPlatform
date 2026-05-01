"""Factor expression validator — syntax check and field reference validation.

Delegates to the expression engine's `validate_expression` for syntax checks
and provides additional semantic checks (empty expressions, field references).
"""

from app.compute.expression import validate_expression


class FactorValidator:
    """Validates factor expressions before compute/backtest."""

    def validate(self, expression: str) -> dict:
        if not expression or not expression.strip():
            return {"valid": False, "error": "Expression is empty"}

        is_valid, error = validate_expression(expression.strip())
        if not is_valid:
            return {"valid": False, "error": error or "Syntax validation failed"}

        return {"valid": True, "error": None}
