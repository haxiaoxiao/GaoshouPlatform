# LLM JSON Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let operators paste a JSON document for each LLM endpoint, safely extract credentials and runtime parameters, and execute supported Chat Completions or Responses API calls without exposing secrets.

**Architecture:** A pure parser converts user JSON into normalized routing fields, a sanitized preserved document, and a separately handled credential. Existing endpoint columns remain authoritative and gain a small set of optional runtime columns, preserving old rows. The gateway consumes only typed allowlisted parameters and keeps the existing priority, failover, cooldown, and health contracts.

**Tech Stack:** FastAPI, Pydantic, SQLAlchemy async, Alembic, Fernet, LiteLLM, Vue 3, TypeScript, Element Plus, CodeMirror, pytest, Vitest.

---

## File Map

- Create `backend/app/services/llm_config.py`: pure JSON parsing, normalization, sanitization, legacy synthesis, and preserved-field reporting.
- Create `backend/migrations/versions/20260714_0001_llm_json_config.py`: additive endpoint runtime/config columns.
- Modify `backend/app/db/models/llm_endpoint.py`: normalized JSON-derived fields.
- Modify `backend/app/services/llm_endpoints.py`: atomic create/update using parsed JSON and the existing encryption boundary.
- Modify `backend/app/api/llm_endpoints.py`: compatible JSON request/response contract and connection testing.
- Modify `backend/app/ai/gateway.py`: typed Chat/Responses dispatch and review-model selection.
- Modify `backend/tests/services/test_llm_endpoints.py`: persistence, compatibility, and secret-boundary tests.
- Create `backend/tests/services/test_llm_config.py`: pure parser tests.
- Modify `backend/tests/services/test_ai_native.py`: gateway parameter and response normalization tests.
- Modify `backend/tests/api/test_llm_endpoint_routes.py`: JSON API contract tests.
- Modify `frontend/src/api/system.ts`: JSON configuration DTOs and payloads.
- Modify `frontend/src/components/LLMEndpointManager.vue`: JSON editor, preview, validation, and warnings.
- Modify `frontend/src/views/llmEndpointOps.test.ts`: JSON editor and secret-handling tests.

### Task 1: Pure JSON Parser

**Files:**
- Create: `backend/app/services/llm_config.py`
- Create: `backend/tests/services/test_llm_config.py`

- [ ] **Step 1: Write failing parser tests**

Cover provider selection, nested and root credentials, aliases, unknown fields, local-only fields, malformed JSON objects, and sanitized output:

```python
def test_parse_json_extracts_runtime_fields_and_removes_key():
    parsed = parse_llm_config({
        "model_provider": "OpenAI",
        "model": "gpt-5.5",
        "review_model": "gpt-5.5",
        "model_reasoning_effort": "xhigh",
        "disable_response_storage": True,
        "network_access": "enabled",
        "model_providers": {
            "OpenAI": {
                "name": "OpenAI",
                "base_url": "https://api.example.com",
                "wire_api": "responses",
                "requires_openai_auth": True,
            }
        },
        "env": {"OPENAI_API_KEY": "secret-key"},
        "future_option": {"mode": "keep"},
    })
    assert parsed.provider == "OpenAI"
    assert parsed.api_key == "secret-key"
    assert parsed.wire_api == "responses"
    assert parsed.reasoning_effort == "xhigh"
    assert parsed.disable_response_storage is True
    assert parsed.sanitized_config["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert "network_access" in parsed.preserved_fields
    assert "future_option" in parsed.preserved_fields
    assert "secret-key" not in json.dumps(parsed.sanitized_config)
```

- [ ] **Step 2: Run parser tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\services\test_llm_config.py -q`

Expected: import failure because `app.services.llm_config` does not exist.

- [ ] **Step 3: Implement the typed parser**

Define immutable output and explicit allowlists:

```python
API_KEY_PLACEHOLDER = "__GAOSHOU_STORED_SECRET__"
WireApi = Literal["chat_completions", "responses"]
ReasoningEffort = Literal["none", "minimal", "low", "medium", "high", "xhigh"]

@dataclass(frozen=True)
class ParsedLlmConfig:
    provider: str
    name: str
    api_base: str
    api_key: str | None
    model: str
    review_model: str | None
    wire_api: WireApi
    reasoning_effort: ReasoningEffort | None
    disable_response_storage: bool
    requires_openai_auth: bool
    sanitized_config: dict[str, Any]
    preserved_fields: tuple[str, ...]

```

Implement `parse_llm_config(config: Mapping[str, Any], *, allow_placeholder: bool = False) -> ParsedLlmConfig` and `synthesize_legacy_config(endpoint: LlmEndpoint) -> dict[str, Any]`. Deep-copy input through JSON serialization, extract `env.OPENAI_API_KEY` or root `OPENAI_API_KEY`, replace every recognized credential location with the placeholder, and validate all scalar types. Reuse URL validation through a small exported helper rather than duplicating rules.

- [ ] **Step 4: Run parser tests and lint**

Run:

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest tests\services\test_llm_config.py -q
.\.venv\Scripts\ruff.exe check app\services\llm_config.py tests\services\test_llm_config.py
```

Expected: all parser tests and Ruff pass.

- [ ] **Step 5: Commit the parser**

```powershell
git add backend/app/services/llm_config.py backend/tests/services/test_llm_config.py
git commit -m "Add LLM JSON configuration parser"
```

### Task 2: Persist Normalized JSON Configuration

**Files:**
- Create: `backend/migrations/versions/20260714_0001_llm_json_config.py`
- Modify: `backend/app/db/models/llm_endpoint.py`
- Modify: `backend/app/services/llm_endpoints.py`
- Modify: `backend/tests/services/test_llm_endpoints.py`

- [ ] **Step 1: Write failing persistence and compatibility tests**

Add tests proving JSON create/update, no plaintext storage, placeholder preservation, provider/base URL rebinding, unknown-field retention, and legacy synthesis:

```python
@pytest.mark.asyncio
async def test_json_config_is_normalized_and_secret_is_encrypted(session, tmp_path):
    endpoint = await LlmEndpointService(session, data_dir=tmp_path).create_from_config(CONFIG)
    assert endpoint.provider == "OpenAI"
    assert endpoint.wire_api == "responses"
    assert endpoint.review_model == "gpt-5.5"
    assert endpoint.reasoning_effort == "xhigh"
    assert "secret-key" not in endpoint.config_json
    assert "secret-key" not in endpoint.api_key_encrypted

@pytest.mark.asyncio
async def test_placeholder_update_preserves_key_only_for_same_destination(session, tmp_path):
    service = LlmEndpointService(session, data_dir=tmp_path)
    endpoint = await service.create_from_config(CONFIG)
    sanitized = service.serialize(endpoint)["config"]
    await service.update_from_config(endpoint.id, sanitized)
    assert await service.decrypt_api_key(endpoint.id) == "secret-key"
```

- [ ] **Step 2: Run persistence tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\services\test_llm_endpoints.py -q`

Expected: failures for missing model columns and JSON service methods.

- [ ] **Step 3: Add model columns and migration**

Add nullable/default-compatible columns:

```python
provider: Mapped[str | None] = mapped_column(String(100))
review_model: Mapped[str | None] = mapped_column(String(200))
wire_api: Mapped[str] = mapped_column(String(32), nullable=False, default="chat_completions")
reasoning_effort: Mapped[str | None] = mapped_column(String(16))
disable_response_storage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
requires_openai_auth: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
config_json: Mapped[str | None] = mapped_column(Text)
```

Migration revision `20260714_0001` uses `down_revision = "20260713_0001"`, server defaults for non-null columns, and removes those defaults after SQLite-compatible batch alteration only when supported.

- [ ] **Step 4: Implement atomic JSON create/update**

Add `create_from_config(config, enabled=True)` and `update_from_config(endpoint_id, config, enabled=None)`. Parse only after acquiring the existing immediate write transaction for updates, re-read the endpoint, and enforce:

```python
destination_changed = (
    parsed.provider != (endpoint.provider or endpoint.name)
    or self._normalized_destination(parsed.api_base) != self._normalized_destination(endpoint.api_base)
)
if destination_changed and parsed.api_key in {None, API_KEY_PLACEHOLDER}:
    raise ValueError("A replacement OPENAI_API_KEY is required when provider or destination changes")
```

Persist `json.dumps(parsed.sanitized_config, ensure_ascii=False, sort_keys=True)` and keep existing discrete create/update methods as wrappers for compatibility.

- [ ] **Step 5: Run persistence tests and migration round-trip**

Run:

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest tests\services\test_llm_config.py tests\services\test_llm_endpoints.py -q
$env:DATABASE_URL='sqlite+aiosqlite:///C:/Users/Albert/AppData/Local/Temp/gaoshou-llm-json-migration.db'
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\alembic.exe downgrade 20260713_0001
.\.venv\Scripts\alembic.exe upgrade head
```

Expected: tests pass and the migration upgrades, downgrades, and upgrades cleanly.

- [ ] **Step 6: Commit persistence**

```powershell
git add backend/app/db/models/llm_endpoint.py backend/app/services/llm_endpoints.py backend/migrations/versions/20260714_0001_llm_json_config.py backend/tests/services/test_llm_endpoints.py
git commit -m "Persist normalized LLM JSON configuration"
```

### Task 3: Map JSON Parameters Into Gateway Calls

**Files:**
- Modify: `backend/app/ai/gateway.py`
- Modify: `backend/tests/services/test_ai_native.py`

- [ ] **Step 1: Write failing Chat and Responses tests**

Test candidate loading, primary/review selection, allowlisted parameters, local-only omission, Responses normalization, and unchanged failover:

```python
def test_responses_candidate_maps_reasoning_and_storage(monkeypatch):
    captured = {}
    monkeypatch.setattr(gateway, "_responses", lambda **kwargs: captured.update(kwargs) or fake_response())
    candidate = GatewayCandidate(
        endpoint_id="one", name="OpenAI", api_base="https://api.example.com",
        api_key="secret", model="gpt-5.5", review_model="gpt-5.5-review",
        wire_api="responses", reasoning_effort="xhigh",
        disable_response_storage=True, source="database",
    )
    gateway.complete_candidate_sync(candidate, [{"role": "user", "content": "hello"}], model_role="review")
    assert captured["model"] == "gpt-5.5-review"
    assert captured["reasoning"] == {"effort": "xhigh"}
    assert captured["store"] is False
    assert "network_access" not in captured
```

- [ ] **Step 2: Run gateway tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\services\test_ai_native.py -q`

Expected: candidate and dispatch assertions fail.

- [ ] **Step 3: Extend candidate and dispatch without breaking callers**

Add optional defaulted fields and a keyword-only role:

```python
ModelRole = Literal["primary", "review"]

@dataclass(frozen=True)
class GatewayCandidate:
    # existing fields unchanged
    provider: str | None = None
    review_model: str | None = None
    wire_api: Literal["chat_completions", "responses"] = "chat_completions"
    reasoning_effort: str | None = None
    disable_response_storage: bool = False

def complete_candidate_sync(
    candidate: GatewayCandidate,
    messages: list[dict[str, Any]],
    *,
    system: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.2,
    max_tokens: int | None = None,
    model_role: ModelRole = "primary",
) -> LLMResult:
    model = candidate.review_model if model_role == "review" and candidate.review_model else candidate.model
    if candidate.wire_api == "responses":
        return _complete_responses(candidate, model, messages, system, tools, temperature, max_tokens)
    return _complete_chat(candidate, model, messages, system, tools, temperature, max_tokens)
```

Use `litellm.responses` with `input`, `instructions`, `max_output_tokens`, optional `reasoning`, and optional `store` for Responses, and existing `litellm.completion` for Chat. Add `_normalize_responses_response` that reads `output_text`, function calls, model, and usage into `LLMResult`. Never pass preserved JSON wholesale.

- [ ] **Step 4: Run gateway and API compatibility tests**

Run:

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest tests\services\test_ai_native.py tests\api\test_ai_native_routes.py tests\api\test_llm_endpoint_routes.py -q
.\.venv\Scripts\ruff.exe check app\ai\gateway.py tests\services\test_ai_native.py
```

Expected: all tests and Ruff pass.

- [ ] **Step 5: Commit gateway mapping**

```powershell
git add backend/app/ai/gateway.py backend/tests/services/test_ai_native.py
git commit -m "Map LLM JSON parameters into gateway calls"
```

### Task 4: Expose Compatible JSON Operations API

**Files:**
- Modify: `backend/app/api/llm_endpoints.py`
- Modify: `backend/tests/api/test_llm_endpoint_routes.py`

- [ ] **Step 1: Write failing JSON route tests**

Cover create, list, sanitized edit response, placeholder update, invalid JSON fields, preserved fields, legacy payloads, and connection-test wire selection:

```python
@pytest.mark.asyncio
async def test_json_route_extracts_key_and_never_returns_it(endpoint_api):
    response = await client.post("/api/system/llm-endpoints", json={"config": CONFIG, "enabled": True})
    assert response.status_code == 201
    payload = response.json()
    assert payload["provider"] == "OpenAI"
    assert payload["config"]["env"]["OPENAI_API_KEY"] == API_KEY_PLACEHOLDER
    assert "secret-key" not in response.text
    assert "network_access" in payload["preserved_fields"]
```

- [ ] **Step 2: Run route tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\api\test_llm_endpoint_routes.py -q`

Expected: request validation rejects `config` and response fields are absent.

- [ ] **Step 3: Add compatible discriminated request handling**

Keep existing fields optional and require either `config` or the complete legacy set in a model validator. Responses add normalized previews, sanitized `config`, and `preserved_fields`. The connection test builds a candidate from normalized endpoint columns and continues using the one-token limit.

- [ ] **Step 4: Run route, service, and OpenAPI tests**

Run:

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest tests\api\test_llm_endpoint_routes.py tests\services\test_llm_config.py tests\services\test_llm_endpoints.py -q
.\.venv\Scripts\ruff.exe check app\api\llm_endpoints.py tests\api\test_llm_endpoint_routes.py
```

Expected: all tests and Ruff pass; plaintext keys are absent from response captures.

- [ ] **Step 5: Commit API contract**

```powershell
git add backend/app/api/llm_endpoints.py backend/tests/api/test_llm_endpoint_routes.py
git commit -m "Expose LLM JSON configuration API"
```

### Task 5: Build The JSON Endpoint Editor

**Files:**
- Modify: `frontend/src/api/system.ts`
- Modify: `frontend/src/components/LLMEndpointManager.vue`
- Modify: `frontend/src/views/llmEndpointOps.test.ts`

- [ ] **Step 1: Write failing frontend contract and behavior tests**

Assert JSON DTOs, template, formatting, invalid JSON, sanitized edit loading, preview fields, warnings, placeholder preservation, and absence of key fields in read DTOs:

```typescript
it('builds an update from sanitized JSON without replacing the stored key', () => {
  const config = JSON.parse(JSON.stringify(LLM_CONFIG_TEMPLATE))
  config.env.OPENAI_API_KEY = LLM_API_KEY_PLACEHOLDER
  expect(buildLlmJsonUpdate(config)).toEqual({ config })
})
```

- [ ] **Step 2: Run targeted Vitest and verify RED**

Run: `cd frontend; npm test -- --run src/views/llmEndpointOps.test.ts`

Expected: missing JSON helpers and editor assertions fail.

- [ ] **Step 3: Add typed JSON API helpers**

Define `LlmJsonConfig`, normalized preview fields, `preserved_fields`, `LLM_API_KEY_PLACEHOLDER`, and safe template/format helpers. Create/update payloads use `{config, enabled}` while legacy helper exports remain only where existing tests require them.

- [ ] **Step 4: Replace discrete provider inputs with CodeMirror**

Reuse the existing CodeMirror dependency and local editor conventions. The form retains the enabled switch and adds:

- JSON editor with the sanitized document.
- Format, reset-to-template, and validate buttons.
- Extracted provider/model/base URL/wire/reasoning preview.
- Preserved-field warning list.
- Inline parse/backend errors.
- Notice that credentials are encrypted and never shown again.

Keep all current list controls, stale read-only behavior, accessibility labels, and mobile breakpoint behavior.

- [ ] **Step 5: Run frontend tests, typecheck, and build**

Run:

```powershell
cd frontend
npm test -- --run
npx vue-tsc -b
npm run build
```

Expected: all tests, typecheck, and production build pass without a new chunk warning attributable to the editor.

- [ ] **Step 6: Commit frontend editor**

```powershell
git add frontend/src/api/system.ts frontend/src/components/LLMEndpointManager.vue frontend/src/views/llmEndpointOps.test.ts
git commit -m "Add JSON editor for LLM endpoints"
```

### Task 6: End-To-End Verification And Integration

**Files:**
- Verify all changed files.
- Update desktop scripts only if service startup requirements changed; no change is expected.

- [ ] **Step 1: Run full backend and frontend suites**

Run:

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m pytest -q
cd ..\frontend
npm test -- --run
npm run build
```

Expected: all backend and frontend tests pass.

- [ ] **Step 2: Run migration round-trip on a temporary SQLite database**

Run upgrade, downgrade to `20260713_0001`, and upgrade to head with a temporary `DATABASE_URL`. Verify `alembic current` reports `20260714_0001 (head)`.

- [ ] **Step 3: Perform security checks**

Search test captures, serialized responses, database `config_json`, logs, and rendered DOM for the test credential. Assert only encrypted material and masked hints persist. Do not use the credential supplied in chat.

- [ ] **Step 4: Perform rendered browser QA**

At `http://127.0.0.1:3511/monitor`, verify desktop and `390x844` mobile flows: open manager, add template, paste fake JSON, format, validate, inspect preview, save against a fake/test backend, edit sanitized JSON, and confirm no overflow, overlay, console error, or secret DOM text.

- [ ] **Step 5: Request final code review**

Review the complete branch against `docs/superpowers/specs/2026-07-14-llm-json-config-design.md`, fixing all P0-P2 findings and rerunning affected tests.

- [ ] **Step 6: Merge and restart affected services**

Merge `codex/llm-json-config` into PROD `main`, run the production migration, restart API and frontend only, then verify ports `8800`, `8810`, and `3511`, `/api/system/health`, `/api/ai/status`, endpoint list, and the rendered manager.
