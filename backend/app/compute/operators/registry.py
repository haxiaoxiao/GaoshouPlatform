"""算子注册表 — 全局单例，管理所有算子"""
from __future__ import annotations

from typing import Any

from app.compute.operators.base import Operator


class OperatorRegistry:
    """算子注册表"""

    _operators: dict[str, Operator] = {}

    @classmethod
    def register(cls, op: Operator) -> Operator:
        """注册一个算子（同名则覆盖）"""
        cls._operators[op.name] = op
        return op

    @classmethod
    def get(cls, name: str) -> Operator | None:
        """按名称获取算子"""
        return cls._operators.get(name)

    @classmethod
    def all(cls) -> list[Operator]:
        """返回所有算子"""
        return list(cls._operators.values())

    @classmethod
    def by_level(cls, level: int) -> list[Operator]:
        """按级别筛选举子"""
        return [op for op in cls._operators.values() if op.level == level]

    @classmethod
    def by_category(cls, category: str) -> list[Operator]:
        """按类别筛选举子"""
        return [op for op in cls._operators.values() if op.category == category]

    @classmethod
    def names(cls) -> list[str]:
        """返回所有算子名称"""
        return list(cls._operators.keys())

    @classmethod
    def to_api_list(cls) -> list[dict[str, Any]]:
        """返回为前端 API 准备的数据格式"""
        return [
            {
                "name": op.name,
                "signature": op.signature,
                "description": op.description,
                "level": op.level,
                "category": op.category,
            }
            for op in cls._operators.values()
        ]

    @classmethod
    def clear(cls) -> None:
        """清空注册表（测试用）"""
        cls._operators.clear()
