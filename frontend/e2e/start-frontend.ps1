$ErrorActionPreference = "Stop"
$env:VITE_API_PROXY_TARGET = "http://127.0.0.1:18801"
$frontend = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $frontend
npm run build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
npm run preview -- --host 127.0.0.1 --port 3512 --strictPort
