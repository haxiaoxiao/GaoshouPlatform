# LLM Provider Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add encrypted multi-endpoint LLM configuration to System Monitor and make the existing LiteLLM gateway fail over in fixed priority order.

**Architecture:** A focused `LLMEndpointService` owns encrypted persistence and health state. The existing gateway reads endpoint candidates through that service and retains environment settings as a compatibility fallback. A separate system router and a separate Vue manager component keep the existing large system modules from growing further.

**Tech Stack:** FastAPI, SQLAlchemy async, SQLite, Alembic, Fernet (`cryptography`), LiteLLM, Vue 3, TypeScript, Element Plus, Vitest, pytest.

---

## File Map

- Create `backend/app/db/models/llm_endpoint.py`: persistent endpoint and provider-health model.
- Create `backend/migrations/versions/20260713_0001_llm_endpoints.py`: additive table migration.
- Create `backend/app/services/llm_endpoints.py`: encryption, validation, CRUD, ordering, masking, and health state.
- Create `backend/app/api/llm_endpoints.py`: operations CRUD/test endpoints.
- Modify `backend/app/ai/gateway.py`: fixed-priority candidate selection and failover.
- Modify `backend/app/api/router.py`: mount endpoint operations router.
- Modify `backend/app/db/models/__init__.py`: export model.
- Modify `backend/pyproject.toml` and `backend/requirements.txt`: direct `cryptography` dependency.
- Create `backend/tests/services/test_llm_endpoints.py`: encryption/store tests.
- Create `backend/tests/api/test_llm_endpoint_routes.py`: masked API contract tests.
- Modify `backend/tests/services/test_ai_native.py`: gateway priority/failover/cooldown tests.
- Modify `frontend/src/api/system.ts`: endpoint DTOs and methods.
- Create `frontend/src/components/LLMEndpointManager.vue`: management dialog.
- Modify `frontend/src/views/SystemMonitor/index.vue`: gateway status group and dialog mount.
- Create `frontend/src/views/llmEndpointOps.test.ts`: UI/API source contract tests.

### Task 1: Encrypted Endpoint Store

**Files:**
- Create: `backend/app/db/models/llm_endpoint.py`
- Create: `backend/app/services/llm_endpoints.py`
- Create: `backend/migrations/versions/20260713_0001_llm_endpoints.py`
- Modify: `backend/app/db/models/__init__.py`
- Modify: `backend/pyproject.toml`
- Modify: `backend/requirements.txt`
- Test: `backend/tests/services/test_llm_endpoints.py`

- [ ] **Step 1: Write failing encryption and CRUD tests**

Cover key generation below `settings.gaoshou_data_dir`, encrypted-at-rest values, masked serialization, blank-key update preservation, validation, ordering, and decryption failure:

```python
@pytest.mark.asyncio
async def test_endpoint_key_is_encrypted_and_masked(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "gaoshou_data_dir", str(tmp_path))
    async with async_session_factory() as session:
        row = await LLMEndpointService(session).create({
            "name": "primary",
            "api_base": "https://api.example.com/v1",
            "api_key": "secret-1234",
            "model": "openai/model-a",
            "enabled": True,
        })
        assert row.api_key_encrypted != "secret-1234"
        payload = LLMEndpointService.serialize(row)
        assert payload["api_key_masked"] == "********1234"
        assert "secret-1234" not in str(payload)
```

- [ ] **Step 2: Run the store tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\services\test_llm_endpoints.py -q`

Expected: import failure because the model/service do not exist.

- [ ] **Step 3: Implement the model and encryption boundary**

The model must define UUID ID, required endpoint fields, priority/enabled, failure/cooldown fields, and timestamps. The service must expose:

```python
class LLMEndpointService:
    async def list(self, *, enabled_only: bool = False) -> list[LLMEndpoint]: ...
    async def create(self, payload: Mapping[str, Any]) -> LLMEndpoint: ...
    async def update(self, endpoint_id: str, payload: Mapping[str, Any]) -> LLMEndpoint: ...
    async def delete(self, endpoint_id: str) -> None: ...
    async def reorder(self, endpoint_ids: list[str]) -> list[LLMEndpoint]: ...
    async def decrypt_api_key(self, row: LLMEndpoint) -> str: ...
    async def mark_success(self, endpoint_id: str) -> None: ...
    async def mark_failure(self, endpoint_id: str, error: str) -> None: ...
    @staticmethod
    def serialize(row: LLMEndpoint) -> dict[str, Any]: ...
```

Use `Fernet.generate_key()` only when the key file is missing. Create `.secrets` first, write only the generated key, and never log plaintext credentials. Validate URLs with `urllib.parse.urlparse`, accepting only `http` and `https` with a hostname.

- [ ] **Step 4: Add migration and direct dependency**

Create revision `20260713_0001` with `down_revision = "20260712_0001"`. Add indexes on `(enabled, priority)` and `cooldown_until`. Add `cryptography>=44.0.0` to both dependency files.

- [ ] **Step 5: Run tests and migration round-trip**

Run:

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest tests\services\test_llm_endpoints.py -q
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\alembic.exe current
```

Expected: tests pass and Alembic reports `20260713_0001 (head)`.

- [ ] **Step 6: Commit the store**

```powershell
git add backend/app/db/models backend/app/services/llm_endpoints.py backend/migrations backend/pyproject.toml backend/requirements.txt backend/tests/services/test_llm_endpoints.py
git commit -m "Add encrypted LLM endpoint store"
```

### Task 2: Fixed-Priority Gateway Failover

**Files:**
- Modify: `backend/app/ai/gateway.py`
- Test: `backend/tests/services/test_ai_native.py`

- [ ] **Step 1: Write failing gateway tests**

Use a fake LiteLLM completion function and a fake candidate loader. Verify the first endpoint wins, failure moves to the second, three failures create a 60-second cooldown, success clears failures, and environment configuration is used only when no enabled rows exist:

```python
def test_gateway_fails_over_in_fixed_priority(monkeypatch):
    attempts = []
    monkeypatch.setattr(gateway, "load_candidates_sync", lambda: [primary, secondary])
    monkeypatch.setattr(gateway, "_completion", fake_completion_that_fails_primary(attempts))
    result = gateway.complete_sync([{"role": "user", "content": "hello"}])
    assert attempts == ["primary", "secondary"]
    assert result.model == "secondary-model"
```

- [ ] **Step 2: Run gateway tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\services\test_ai_native.py -q`

Expected: failure because candidate loading and failover do not exist.

- [ ] **Step 3: Refactor the existing gateway without changing callers**

Keep `complete_sync(...)` and `complete(...)` signatures unchanged. Add an internal candidate dataclass and helpers:

```python
@dataclass(frozen=True)
class GatewayCandidate:
    id: str | None
    name: str
    api_base: str
    api_key: str
    model: str
    source: Literal["database", "environment"]
```

Build LiteLLM kwargs per candidate. Use `num_retries=0` so one provider cannot consume all retry time before failover. Sanitize exception text by replacing API keys and URL userinfo before recording or aggregating errors.

- [ ] **Step 4: Make status database-aware**

`gateway_status()` must report `configured`, `state`, `configuration_mode`, `enabled_endpoint_count`, `primary_endpoint`, and masked/sanitized reason. It must preserve existing `model` and `api_base` fields for frontend compatibility.

- [ ] **Step 5: Run gateway and AI tests**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\services\test_ai_native.py tests\api\test_ai_native_routes.py -q`

Expected: all pass.

- [ ] **Step 6: Commit failover**

```powershell
git add backend/app/ai/gateway.py backend/tests/services/test_ai_native.py
git commit -m "Add fixed-priority LLM failover"
```

### Task 3: Operations API

**Files:**
- Create: `backend/app/api/llm_endpoints.py`
- Modify: `backend/app/api/router.py`
- Test: `backend/tests/api/test_llm_endpoint_routes.py`

- [ ] **Step 1: Write failing masked CRUD API tests**

Test create/list/update-with-blank-key/reorder/delete/test-connection. Assert every serialized response excludes `api_key`, `api_key_encrypted`, and the plaintext value. Assert invalid URL and incomplete reorder return 422/409 rather than 500.

- [ ] **Step 2: Run API tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\api\test_llm_endpoint_routes.py -q`

Expected: 404 for the missing router.

- [ ] **Step 3: Implement request models and routes**

Use explicit Pydantic request models:

```python
class LLMEndpointCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    api_base: str = Field(min_length=8, max_length=500)
    api_key: str = Field(min_length=1, max_length=2000)
    model: str = Field(min_length=1, max_length=200)
    enabled: bool = True

class LLMEndpointUpdate(BaseModel):
    name: str | None = None
    api_base: str | None = None
    api_key: str | None = None
    model: str | None = None
    enabled: bool | None = None
```

Mount the router at `/api/system/llm-endpoints`. The test route must decrypt only its selected endpoint, call a one-token/minimal prompt through the same candidate call helper, update health, and return sanitized latency/status.

- [ ] **Step 4: Run API and secret-leak tests**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests\api\test_llm_endpoint_routes.py tests\services\test_llm_endpoints.py -q`

Expected: all pass and plaintext key is absent from captured responses/log payloads.

- [ ] **Step 5: Commit operations API**

```powershell
git add backend/app/api/llm_endpoints.py backend/app/api/router.py backend/tests/api/test_llm_endpoint_routes.py
git commit -m "Expose LLM endpoint operations API"
```

### Task 4: System Monitor Management UI

**Files:**
- Modify: `frontend/src/api/system.ts`
- Create: `frontend/src/components/LLMEndpointManager.vue`
- Modify: `frontend/src/views/SystemMonitor/index.vue`
- Create: `frontend/src/views/llmEndpointOps.test.ts`

- [ ] **Step 1: Write failing frontend source-contract tests**

Assert the API module exposes list/create/update/delete/reorder/test methods, System Monitor mounts `<LLMEndpointManager>`, and the component includes password input, enable switch, move controls, test command, and delete confirmation.

- [ ] **Step 2: Run Vitest and verify RED**

Run: `cd frontend; npm test -- --run src/views/llmEndpointOps.test.ts`

Expected: failure because the component and API contracts do not exist.

- [ ] **Step 3: Add typed frontend API methods**

Define `LLMEndpoint`, `LLMEndpointDraft`, and `LLMEndpointTestResult`. API keys appear only in create/update payload types, never in read DTOs.

- [ ] **Step 4: Implement the management dialog**

Use an Element Plus dialog with a compact ordered table and an inline edit form. Use `ArrowUp`, `ArrowDown`, `CirclePlus`, `Connection`, `Delete`, and `Edit` icons. Disable move buttons at boundaries. Keep the key input blank in edit mode with `show-password`; blank preserves the key. Confirm deletion with `ElMessageBox.confirm`.

- [ ] **Step 5: Integrate the control-dock summary**

Load endpoints in `loadOps()`. Show readiness, enabled/total, first enabled provider, and most recent sanitized error. Open the manager from one command button and refresh the summary after mutations.

- [ ] **Step 6: Run frontend tests and build**

Run:

```powershell
cd frontend
npm test -- --run
npm run build
```

Expected: Vitest and Vue TypeScript build pass.

- [ ] **Step 7: Commit UI**

```powershell
git add frontend/src/api/system.ts frontend/src/components/LLMEndpointManager.vue frontend/src/views/SystemMonitor/index.vue frontend/src/views/llmEndpointOps.test.ts
git commit -m "Add LLM endpoint operations UI"
```

### Task 5: Integrated Verification And Rollout

**Files:**
- Verify all changed files.

- [ ] **Step 1: Run backend verification**

```powershell
cd backend
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\ruff.exe check app tests
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\alembic.exe current
```

Expected: all tests pass, changed files are lint-clean, dependencies are consistent, and migration head is `20260713_0001`.

- [ ] **Step 2: Run frontend verification**

```powershell
cd frontend
npm test -- --run
npm run build
```

Expected: all tests and production build pass.

- [ ] **Step 3: Perform local browser smoke test**

Restart backend `8800` and frontend `3511`. Verify desktop and mobile System Monitor layouts, masked keys, create/edit/reorder/test/delete interactions, immediate gateway status refresh, no overlap, and no console errors.

- [ ] **Step 4: Verify compatibility fallback**

With no enabled database endpoints, call `/api/ai/status` and confirm environment configuration behavior is unchanged. With two fake/test endpoints, verify fixed-priority fallback and sanitized errors without committing real credentials.

- [ ] **Step 5: Final review and merge**

Run `git diff --check`, request code review, fix P0/P1 findings, merge the feature branch into PROD `main`, restart affected services, and verify `/health`, `/api/ai/status`, `/api/system/llm-endpoints`, and `/system` UI.
