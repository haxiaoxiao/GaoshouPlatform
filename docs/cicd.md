# CI/CD

GaoshouPlatform uses GitHub Actions with one Windows self-hosted runner for
local development and production deployments, plus an optional Mac self-hosted
runner for manual development compatibility checks.

## Branches and environments

| Branch | GitHub environment | Target | Ports |
|---|---|---|---|
| `develop` | `development` | `E:\Projects\GaoshouPlatform-dev` | `18800`, `18810`, `13500` |
| `main` | `production` | `E:\Projects\GaoshouPlatform-prod` | `8800`, `8810`, `3500` |

Configure the `production` GitHub environment with required reviewers. This is
the production approval gate; do not put production secrets in workflow files.

## Runner labels

Windows runner labels:

```text
self-hosted
Windows
gaoshou-windows
```

Mac runner labels, only for the manual compatibility workflow:

```text
self-hosted
macOS
gaoshou-mac-dev
```

## Local deployment directories

Create two clean checkouts on the Windows PC:

```powershell
git clone https://github.com/haxiaoxiao/GaoshouPlatform.git E:\Projects\GaoshouPlatform-prod
git clone https://github.com/haxiaoxiao/GaoshouPlatform.git E:\Projects\GaoshouPlatform-dev
cd E:\Projects\GaoshouPlatform-dev
git switch -c develop origin/develop
```

Each checkout needs its own `.env.local`. Keep data paths, tokens, QMT settings,
and machine-specific config in `.env.local`; these files are ignored by git.

Minimum development overrides:

```text
SYNC_SERVICE_URL=http://127.0.0.1:18810
SYNC_SERVICE_PORT=18810
```

The deployment script supplies the backend, sync, and frontend ports to the
launcher through environment variables.

## Manual deploy commands

Development:

```powershell
powershell -ExecutionPolicy Bypass -File tools\deploy-windows.ps1 -Environment development
```

Production:

```powershell
powershell -ExecutionPolicy Bypass -File tools\deploy-windows.ps1 -Environment production
```

The deploy script refuses to run on a dirty target checkout unless `-AllowDirty`
is provided. It fetches and fast-forwards the configured branch, installs
backend/frontend dependencies, builds the frontend, restarts the target instance,
and checks health endpoints.

The deploy script defaults pip to `https://pypi.org/simple` to avoid inherited
machine-level mirror errors. Set `GAOSHOU_PIP_INDEX_URL` or pass `-PipIndexUrl`
if a local mirror is required.

## Validation policy

The current repository has pre-existing Ruff findings, so CI treats Ruff as a
non-blocking report. The hard gates are backend tests and frontend build. Tighten
Ruff to a blocking step after a dedicated lint cleanup.
