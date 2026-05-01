"""Factor creation templates — 4 preset types with predefined expressions and params."""

from app.models.factor import FactorTemplate, TemplateType, StockPool

TEMPLATES: list[dict] = [
    {
        "id": "financial",
        "type": TemplateType.FINANCIAL,
        "name": "财务因子",
        "description": "基于财务报表数据构建复合价值/质量因子",
        "preset_expression": "(1/PE_TTM) + (1/PB) + ROE",
        "preset_params": {"stock_pool": StockPool.HS300.value, "direction": "desc"},
        "category": "质量类因子",
    },
    {
        "id": "technical",
        "type": TemplateType.TECHNICAL,
        "name": "技术指标因子",
        "description": "基于量价数据构建技术分析因子",
        "preset_expression": "RSI($close, 14)",
        "preset_params": {"stock_pool": StockPool.ZZ500.value, "direction": "asc"},
        "category": "技术指标因子",
    },
    {
        "id": "custom_operator",
        "type": TemplateType.CUSTOM_OPERATOR,
        "name": "自定义算子",
        "description": "组合系统提供的算子构建个性化因子",
        "preset_expression": "ZScore(Mean($turnover_rate, 20), 252)",
        "preset_params": {"stock_pool": StockPool.ZZ800.value, "direction": "asc"},
        "category": "情绪类因子",
    },
    {
        "id": "custom_base",
        "type": TemplateType.CUSTOM_BASE,
        "name": "自定义基础因子",
        "description": "从原始字段开始自由编写表达式",
        "preset_expression": "$close",
        "preset_params": {"stock_pool": StockPool.HS300.value, "direction": "desc"},
        "category": "基础科目及衍生类因子",
    },
]


class FactorTemplatesService:
    """Provides factor creation templates and parameter merging."""

    def list_templates(self, template_type: TemplateType | None = None) -> list[FactorTemplate]:
        templates = []
        for t in TEMPLATES:
            if template_type and t["type"] != template_type:
                continue
            templates.append(FactorTemplate(
                id=t["id"],
                type=t["type"],
                name=t["name"],
                description=t["description"],
                preset_expression=t["preset_expression"],
                preset_params=t["preset_params"],
                category=t["category"],
            ))
        return templates

    def merge_params(self, template_type: TemplateType | str, overrides: dict) -> dict:
        template = next((t for t in TEMPLATES if t["type"] == template_type or t["id"] == template_type), None)
        if not template:
            raise ValueError(f"Unknown template type: {template_type}")
        merged = {**template["preset_params"], **overrides}
        return merged
