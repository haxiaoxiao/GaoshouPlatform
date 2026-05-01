"""Tests for factor templates service."""
import pytest
from app.services.factor_templates import FactorTemplatesService, TEMPLATES
from app.models.factor import TemplateType


class TestFactorTemplatesService:
    def setup_method(self):
        self.service = FactorTemplatesService()

    def test_list_all_templates(self):
        templates = self.service.list_templates()
        assert len(templates) == 4
        types = {t.type for t in templates}
        assert types == {
            TemplateType.FINANCIAL,
            TemplateType.TECHNICAL,
            TemplateType.CUSTOM_OPERATOR,
            TemplateType.CUSTOM_BASE,
        }

    def test_get_by_type(self):
        templates = self.service.list_templates(TemplateType.FINANCIAL)
        assert len(templates) == 1
        assert templates[0].type == TemplateType.FINANCIAL

    def test_merge_params_fills_defaults(self):
        result = self.service.merge_params(
            TemplateType.FINANCIAL,
            {"stock_pool": "zz500"},
        )
        assert result["stock_pool"] == "zz500"

    def test_merge_params_unknown_template_raises(self):
        with pytest.raises(ValueError, match="Unknown template type"):
            self.service.merge_params("nonexistent", {})

    def test_all_templates_have_required_fields(self):
        for t in self.service.list_templates():
            assert t.id
            assert t.name
            assert t.description
            assert t.preset_expression
            assert t.category


class TestTemplates:
    def test_template_count(self):
        assert len(TEMPLATES) == 4

    def test_template_ids_unique(self):
        ids = [t["id"] for t in TEMPLATES]
        assert len(ids) == len(set(ids))
