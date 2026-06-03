# AKQuant Update Workflow

This workflow keeps the platform's AKQuant dependency current without silently
changing backtest semantics.

## Check For Updates

```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.\.venv\Scripts\python.exe -m app.scripts.update_akquant
```

The script checks the backend virtualenv against the latest PyPI release and
prints the installed, latest, and target versions. It does not modify files in
check-only mode.

## Upgrade And Validate

```powershell
cd E:\Projects\GaoshouPlatform-dev\backend
.\.venv\Scripts\python.exe -m app.scripts.update_akquant --upgrade
```

The upgrade mode:

- Installs the exact latest AKQuant release into `backend/.venv`.
- Updates `backend/pyproject.toml` and `backend/requirements.txt` to the same
  lower-bound version.
- Runs the AKQuant integration and Parquet-provider regression tests.

## Useful Options

```powershell
# Try a specific release instead of latest.
.\.venv\Scripts\python.exe -m app.scripts.update_akquant --upgrade --version 0.2.40

# CI/monitor mode: fail with exit code 2 when a new version is available.
.\.venv\Scripts\python.exe -m app.scripts.update_akquant --fail-if-outdated

# Preview install/test actions without changing the environment or files.
.\.venv\Scripts\python.exe -m app.scripts.update_akquant --upgrade --dry-run

# Add extra regression tests after upgrade.
.\.venv\Scripts\python.exe -m app.scripts.update_akquant --upgrade --test tests/backtest
```

If a new AKQuant release changes public APIs, fix the platform adapter under
`backend/app/backtest/engine/akquant/` and rerun the script. Avoid vendoring the
upstream source tree into this repository unless we intentionally need a local
patch that cannot be carried as an adapter.
