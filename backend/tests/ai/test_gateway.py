from __future__ import annotations

from app.ai.gateway import LLMGateway
from app.ai.schemas import AIChatMessage
from app.core.config import settings


def test_llm_gateway_uses_litellm_completion(monkeypatch):
    captured = {}

    def fake_completion(**kwargs):
        captured.update(kwargs)
        return {"choices": [{"message": {"content": "ok"}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(settings, "ai_model", "deepseek/deepseek-chat")
    monkeypatch.setattr(settings, "ai_api_key_env", "DEEPSEEK_API_KEY")
    monkeypatch.setattr("litellm.completion", fake_completion)

    gateway = LLMGateway()
    text = gateway.chat(
        system="system prompt",
        messages=[AIChatMessage(role="user", content="hello")],
        temperature=0.1,
        max_tokens=32,
    )

    assert text == "ok"
    assert captured["model"] == gateway.model
    assert captured["temperature"] == 0.1
    assert captured["max_tokens"] == 32
    assert captured["messages"] == [
        {"role": "system", "content": "system prompt"},
        {"role": "user", "content": "hello"},
    ]
