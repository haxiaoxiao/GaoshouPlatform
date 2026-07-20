# LLM Provider Operations Design

## Goal

Allow operators to configure multiple OpenAI-compatible LLM endpoints from the existing System Monitor page. Each endpoint has its own name, API URL, API key, model, enabled state, and priority. The AI gateway always prefers the first healthy endpoint and fails over in priority order.

## Scope

- Add encrypted persistent endpoint configuration.
- Add masked CRUD, ordering, enable/disable, and connection-test APIs.
- Add an AI Gateway section and management dialog to the existing operations page.
- Update the existing LiteLLM gateway to use fixed-priority failover.
- Preserve the current environment-variable endpoint as a compatibility fallback when no database endpoint is enabled.

This change does not add provider billing, quotas, load balancing, per-user credentials, or live-trading access.

## Data Model

Create `llm_endpoints` with:

- `id`: UUID primary key.
- `name`: operator-facing name.
- `api_base`: OpenAI-compatible base URL.
- `api_key_encrypted`: encrypted API key; never returned directly.
- `api_key_hint`: last four characters for masked display.
- `model`: provider-specific model name.
- `priority`: ascending fixed-priority order.
- `enabled`: whether the endpoint is eligible.
- `consecutive_failures`: operational health counter.
- `cooldown_until`: temporary failover cooldown.
- `last_success_at`, `last_failure_at`, `last_error`: health details.
- standard `created_at`, `updated_at` timestamps.

API keys are encrypted with Fernet. The application generates a key on first write at `GAOSHOU_DATA_DIR/.secrets/llm-config.key`. The key file and encrypted database value are both required to decrypt credentials. The key path is outside the repository and is never included in API responses or logs.

## Backend Components

### Endpoint Store

`LLMEndpointService` owns validation, encryption, CRUD, reordering, masked serialization, connection testing, and health updates. URL validation accepts only `http` or `https`. Names, models, and URLs are required. Updating an endpoint without a new API key preserves the existing encrypted key.

### Gateway Selection

The existing `app.ai.gateway` remains the only model-call entry point.

For each request:

1. Load enabled database endpoints ordered by `priority`, then `created_at`.
2. Skip endpoints whose `cooldown_until` is in the future.
3. Attempt each endpoint in order and return the first successful response.
4. On provider/network/authentication/rate-limit/server failure, record the failure and try the next endpoint.
5. After three consecutive failures, cool the endpoint down for 60 seconds.
6. On success, clear failure state and cooldown.

If no enabled database endpoint exists, use the current `LLM_API_BASE`, `LLM_API_KEY`, and `LLM_DEFAULT_MODEL` settings as a virtual fallback endpoint. If database endpoints exist but all fail, return an aggregated error containing endpoint names and sanitized error summaries, never keys or credential-bearing URLs.

### Operations API

Add under `/api/system/llm-endpoints`:

- `GET /`: list masked endpoint configurations and health.
- `POST /`: create an endpoint.
- `PUT /{id}`: update metadata or replace its key.
- `DELETE /{id}`: delete after explicit frontend confirmation.
- `POST /reorder`: persist an ordered list of endpoint IDs.
- `POST /{id}/test`: issue a minimal model request and update health.

The existing `/api/ai/status` reports the selected configuration mode, enabled endpoint count, and whether at least one usable endpoint exists, without exposing credentials.

## Frontend

Extend the existing System Monitor control dock with an `AI Gateway` group showing:

- readiness state;
- enabled/total endpoint count;
- current first-priority endpoint;
- most recent provider error;
- a `Manage` command.

The management dialog uses a compact ordered table. Operators can add or edit an endpoint, enable or disable it, move it up or down, test it, and delete it. API keys are password inputs and are never prefilled; edit mode explains that leaving the key blank preserves the current key. Connection testing is an explicit action because it may incur provider usage.

The dialog remains usable at mobile widths by switching each row to a stacked layout. Existing Element Plus controls and page styling are reused.

## Error Handling And Security

- API keys are write-only and masked in every read response.
- Exceptions and task metadata must not contain API keys.
- Invalid URLs, empty models, duplicate priorities, and malformed reorder payloads return 4xx errors.
- Decryption failure marks an endpoint blocked and does not fall back to exposing stored data.
- A failed connection test does not disable an endpoint automatically; normal gateway cooldown still applies.
- Deleting the local encryption key makes stored credentials unrecoverable. Operators must enter new keys.

## Testing

- Encryption round-trip, masked serialization, key preservation, and missing-key behavior.
- CRUD validation, reorder behavior, delete, and connection-test API contracts.
- Fixed-priority selection, failover, success reset, three-failure cooldown, and environment fallback.
- Verification that errors/status payloads never contain raw keys.
- Frontend API typing and component behavior for create/edit/reorder/test/delete.
- Full backend suite, frontend Vitest, TypeScript build, production build, migration upgrade/downgrade, and local browser smoke test.

## Rollout

The migration only adds `llm_endpoints`. Existing AI behavior continues through environment fallback until at least one database endpoint is enabled. After deployment, operators can add providers from System Monitor without restarting the API service.
