# CI/CD

Last updated: 2026-07-17

GaoshouPlatform uses GitHub Actions with one Windows self-hosted runner for
local development and production deployments, plus an optional Mac self-hosted
runner for manual development compatibility checks.

## Branches and environments

| Branch | GitHub environment | Target | Ports |
|---|---|---|---|
| `develop` | `development` | `E:\Projects\GaoshouPlatform-dev` | `18800`, `18810`, `13500` |
| `main` | `production` | `E:\Projects\GaoshouPlatform-prod` | `8800`, `8810`, `3511` |

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

The frontend normally binds the environment-specific port shown above. If that
port is occupied, the launcher chooses the next available port and records it in
`.runtime/frontend-port.txt`. Deployment health checks read that runtime file;
they do not assume the preferred port stayed available.

Deployment notes:

- The script stops the target services before `npm ci` so Windows file locks
  under `frontend/node_modules` do not break deploys.
- Backend editable install now bootstraps `packaging`, `hatchling`, and
  `editables`, then uses `--no-build-isolation` for a more stable local build.
- When `.env.local` configures miniQMT (`QMT_ACCOUNT_ID` or `QMT_TRADER_PATH`),
  the script auto-installs `xtquant==250516.1.1` unless it is already present in
  the target venv. Override the package spec with `GAOSHOU_XTQUANT_SPEC` if the
  machine needs a different local QMT build.
- Local runtime output such as `factor_eval_runs/` is git-ignored so generated
  research artifacts do not block automatic deploys.

## Validation policy

Ruff, backend tests, frontend unit tests, and the frontend production build are
blocking CI gates. Dependency declarations live in `backend/pyproject.toml`;
`backend/requirements.txt` installs the editable project with its development
extras instead of maintaining a second dependency list.
