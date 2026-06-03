# Local Python Environments

This workstation has multiple Python environments. Use the right one to avoid
surprise "package not found" failures.

## Project Environments

Use these for project-specific runtime and tests:

```powershell
# Gaoshou dev backend
E:\Projects\GaoshouPlatform-dev\backend\.venv\Scripts\python.exe

# Gaoshou prod backend
E:\Projects\GaoshouPlatform-prod\backend\.venv\Scripts\python.exe

# QuantaAlpha
E:\Projects\QuantaAlpha\.venv\Scripts\python.exe
```

## Shared Research Environment

For ad hoc local analysis outside a project venv, use the E-drive shared env:

```powershell
E:\PythonEnvs\quant-tools\Scripts\python.exe
```

Installed common packages include `duckdb`, `pandas`, `pyarrow`, `numpy`,
`scipy`, `scikit-learn`, `statsmodels`, `fastapi`, `sqlalchemy`,
`pydantic-settings`, `pytest`, `requests`, `httpx`, `loguru`, and `openpyxl`.

Activate it in an interactive PowerShell session with:

```powershell
E:\PythonEnvs\quant-tools\Scripts\Activate.ps1
```

## Pip Cache

Pip is configured to use an E-drive cache:

```powershell
E:\pip-cache
```

Check it with:

```powershell
E:\PythonEnvs\quant-tools\Scripts\python.exe -m pip cache dir
```

## Practical Rule

Use the Gaoshou backend venv for Gaoshou runtime or tests. Use
`E:\PythonEnvs\quant-tools\Scripts\python.exe` for quick parquet inspection,
one-off notebooks, or cross-project data checks.
