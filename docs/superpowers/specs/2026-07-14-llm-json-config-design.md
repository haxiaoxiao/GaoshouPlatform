# LLM JSON Configuration Design

## Goal

Extend the existing encrypted multi-endpoint LLM configuration so an operator can paste one JSON document per endpoint. The system extracts useful routing and request parameters, encrypts credentials separately, preserves unsupported configuration for future use, and never returns plaintext credentials.

## User Experience

The endpoint editor uses one JSON editor as the primary configuration surface. A template action inserts a valid example. A parse preview shows the extracted provider, base URL, primary model, review model, wire API, reasoning effort, and which fields will remain local-only.

The accepted shape is intentionally compatible with common OpenAI/Codex-style configuration:

```json
{
  "model_provider": "OpenAI",
  "model": "gpt-5.5",
  "review_model": "gpt-5.5",
  "model_reasoning_effort": "xhigh",
  "disable_response_storage": true,
  "network_access": "enabled",
  "windows_wsl_setup_acknowledged": true,
  "model_providers": {
    "OpenAI": {
      "name": "OpenAI",
      "base_url": "https://api.example.com",
      "wire_api": "responses",
      "requires_openai_auth": true
    }
  },
  "env": {
    "OPENAI_API_KEY": "sk-example"
  }
}
```

For compatibility, `OPENAI_API_KEY` may also appear at the document root. The parser accepts exactly one selected provider: `model_provider` identifies the entry under `model_providers`.

On edit, the server returns sanitized JSON. Credential values are replaced by a stable placeholder. Leaving the placeholder unchanged preserves the encrypted key. Supplying a new credential replaces it. Changing the selected provider or base URL requires a replacement key, preserving the existing destination-to-key security binding.

Existing list ordering, enabled toggles, connection tests, cooldown state, and health information remain outside the JSON editor because they are platform operational state rather than provider configuration.

## Storage

Add nullable columns to `llm_endpoints` for normalized runtime fields:

- `provider`: selected provider identifier.
- `review_model`: optional review model.
- `wire_api`: `responses` or `chat_completions`.
- `reasoning_effort`: optional `none`, `minimal`, `low`, `medium`, `high`, or `xhigh` value.
- `config_json`: sanitized JSON containing all non-secret input, including unknown fields.

The existing `name`, `api_base`, `model`, `api_key_encrypted`, `api_key_hint`, `enabled`, and `priority` columns remain authoritative for routing and list queries. This avoids parsing JSON during candidate selection and preserves compatibility with existing endpoints.

API keys continue to use the existing Fernet boundary. Plaintext keys are removed before `config_json` is persisted. Key placeholders are presentation-only and are never encrypted as new credentials.

Existing rows require no user action. The read serializer synthesizes an equivalent JSON document from normalized legacy fields when `config_json` is absent.

## Parsing And Validation

A focused backend parser owns JSON normalization. It returns a typed normalized configuration and sanitized JSON. It does not access the database or call LiteLLM.

Validation rules:

- The root must be a JSON object.
- `model_provider`, `model`, and the selected provider's `base_url` are required.
- The selected provider must exist in `model_providers`.
- `base_url` uses the existing URL validation, including scheme, hostname, credentials, query, fragment, and port rules.
- `wire_api` accepts `responses` and `chat_completions`; aliases such as `chat` normalize to `chat_completions`.
- Boolean fields must be JSON booleans, not truthy strings.
- `model_reasoning_effort` is optional and validated against the supported values.
- Create requires a non-placeholder API key. Update preserves the current key only when provider and normalized destination are unchanged.
- Unknown fields are retained in sanitized JSON and reported as preserved, not rejected.

Malformed JSON and invalid extracted fields return field-specific 422 responses. Secrets are redacted before any error is logged or returned.

## Runtime Mapping

The gateway candidate adds `provider`, `review_model`, `wire_api`, `reasoning_effort`, and a small typed request-options structure.

The runtime mapping is allowlisted:

| JSON field | Runtime behavior |
|---|---|
| `model` | Primary model for normal calls |
| `review_model` | Used only when a caller requests the review role; otherwise stored and exposed |
| `wire_api=responses` | Use LiteLLM Responses API and normalize its output into `LLMResult` |
| `wire_api=chat_completions` | Use the existing LiteLLM completion path |
| `model_reasoning_effort` | Send as `reasoning_effort` when set |
| `disable_response_storage=true` | Send `store=false` where the selected wire API supports it |
| `requires_openai_auth` | Provider metadata; explicit encrypted key remains required by this platform |
| `network_access` | Local-only preserved configuration; never sent to the provider |
| `windows_wsl_setup_acknowledged` | Local-only preserved configuration; never sent to the provider |

Unknown fields are never passed through blindly. This prevents local settings and future vendor-specific secrets from leaking into provider requests. New runtime parameters must be added to the allowlist with tests.

The existing fixed-priority failover, cooldown, health updates, error sanitization, environment fallback, synchronous `complete_sync`, asynchronous `complete`, and response normalization contracts remain intact.

## API

Existing routes remain stable. Create and update accept a new `config` JSON object as the preferred input. The current discrete fields remain accepted temporarily for backward compatibility.

Read responses add:

- `config`: sanitized JSON object.
- `provider`, `review_model`, `wire_api`, and `reasoning_effort` preview fields.
- `preserved_fields`: paths retained but not used at runtime.

No response includes plaintext credentials or encrypted credential material.

Connection testing uses the normalized stored configuration and the selected wire API. It continues to request the smallest possible response and records sanitized health state.

## Frontend

The current endpoint manager remains the owning component. The add/edit form replaces the discrete provider fields with a CodeMirror JSON editor, while retaining the endpoint enabled switch.

The editor provides:

- Format and validate commands.
- A safe example template with a fake key.
- Inline JSON syntax and backend validation errors.
- An extracted-configuration preview.
- A warning list for preserved local-only or unknown fields.
- A clear notice that saved keys will not be shown again.

The list continues to show provider name, model, base URL, masked key hint, health, and priority controls. Mobile layout keeps the editor and preview in a single column without horizontal page overflow.

## Testing

Backend tests cover parsing, legacy synthesis, credential extraction and removal, placeholder preservation, destination rebinding, unknown-field preservation, migration round-trip, both wire APIs, reasoning and storage request mapping, review-role model selection, failover, cooldown, and complete secret redaction.

Frontend tests cover JSON template insertion, formatting, invalid JSON, sanitized edit loading, placeholder preservation, extracted preview, warning rendering, create/update payloads, and mobile layout contracts.

Rendered QA covers desktop and mobile add/edit flows, validation failures, successful save with a fake provider in tests, and absence of secrets in DOM snapshots, network responses, logs, and screenshots.

## Non-Goals

- Arbitrary passthrough of unknown JSON fields to LiteLLM.
- Storing or exposing plaintext credentials.
- Replacing endpoint priority, health, or enabled controls with JSON fields.
- Implementing WSL setup or provider-side network access controls.
- Adding live provider credentials to automated tests.
