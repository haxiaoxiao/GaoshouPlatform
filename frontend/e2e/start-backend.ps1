$ErrorActionPreference = "Stop"
$runtime = Join-Path $PSScriptRoot "..\.e2e-runtime"
$data = Join-Path $runtime "data"
$parquet = Join-Path $data "parquet"
New-Item -ItemType Directory -Force -Path $parquet | Out-Null

$database = (Join-Path $runtime "e2e.db").Replace("\", "/")
$env:DATABASE_URL = "sqlite+aiosqlite:///$database"
$env:GAOSHOU_DATA_DIR = $data
$env:PARQUET_DATA_DIR = $parquet
$env:MARKET_DATA_BACKEND = "parquet"
$env:LIVE_TRADING_ENABLE_ORDER_SUBMIT = "false"
$env:LIVE_TRADING_AUTO_EXECUTE_ENABLED = "false"
$env:LIVE_TRADING_CONTROL_SECRET = ""

$backend = Resolve-Path (Join-Path $PSScriptRoot "..\..\backend")
$venvPython = Join-Path $backend ".venv\Scripts\python.exe"
$python = if ($env:GAOSHOU_E2E_PYTHON) {
    $env:GAOSHOU_E2E_PYTHON
} elseif (Test-Path -LiteralPath $venvPython) {
    $venvPython
} else {
    "python"
}
& $python -m uvicorn app.main:app --host 127.0.0.1 --port 18801 --app-dir $backend
