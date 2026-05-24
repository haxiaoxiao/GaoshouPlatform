@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title GaoshouPlatform Shutdown

set "ROOT=E:\Projects\GaoshouPlatform"
set "ENV_FILE=%ROOT%\.env.local"
set "BACKEND_PORT=8800"
set "SYNC_PORT=8810"
set "FRONTEND_PORT=3500"
set "STOP_REDIS=0"
set "STOP_CLICKHOUSE=0"
set "NO_PAUSE=0"

if /i "%~1"=="--no-pause" set "NO_PAUSE=1"
if "%GAOSHOU_SKIP_PAUSE%"=="1" set "NO_PAUSE=1"
if /i "%~1"=="--stop-redis" set "STOP_REDIS=1"
if /i "%~1"=="--stop-clickhouse" set "STOP_CLICKHOUSE=1"
if /i "%~2"=="--no-pause" set "NO_PAUSE=1"
if /i "%~2"=="--stop-redis" set "STOP_REDIS=1"
if /i "%~2"=="--stop-clickhouse" set "STOP_CLICKHOUSE=1"

set "MARKET_DATA_BACKEND=parquet"
set "CLICKHOUSE_ENABLED=false"
if exist "%ENV_FILE%" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    set "K=%%a"
    set "V=%%b"
    if /i "!K!"=="MARKET_DATA_BACKEND" set "MARKET_DATA_BACKEND=!V!"
    if /i "!K!"=="CLICKHOUSE_ENABLED" set "CLICKHOUSE_ENABLED=!V!"
    if /i "!K!"=="SYNC_SERVICE_PORT" set "SYNC_PORT=!V!"
  )
)
if /i "%MARKET_DATA_BACKEND%"=="clickhouse" set "STOP_CLICKHOUSE=1"
if /i "%CLICKHOUSE_ENABLED%"=="true" set "STOP_CLICKHOUSE=1"

echo ========================================
echo   GaoshouPlatform Shutdown
echo ========================================
echo.

echo [1/3] Stopping backend/sync/frontend processes...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ports=@(%BACKEND_PORT%,%SYNC_PORT%,%FRONTEND_PORT%,3501,5173); $ids=@(); Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort } | ForEach-Object { $p=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $_.OwningProcess) -ErrorAction SilentlyContinue; if ($p -and $p.CommandLine -and ($p.CommandLine -match 'uvicorn app\.(main|sync_main):app|npm run dev|node_modules.*vite|vite\.js')) { $ids += [int]$p.ProcessId } }; Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -match 'uvicorn app\.(main|sync_main):app|node_modules.*vite|vite\.js' } | ForEach-Object { $ids += [int]$_.ProcessId }; $ids | Select-Object -Unique | ForEach-Object { Write-Host ('      Killing PID ' + $_); Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }"
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 1"
echo       OK

echo [2/3] Docker service handling...
where docker >nul 2>&1
if errorlevel 1 (
  echo       Docker not found. Skip Docker containers.
) else (
  if "%STOP_REDIS%"=="1" (
    docker stop redis-server >nul 2>&1
    if errorlevel 1 (echo       Redis was not running) else (echo       Redis stopped)
  ) else (
    echo       Redis left running. Use --stop-redis to stop it.
  )
  if "%STOP_CLICKHOUSE%"=="1" (
    docker stop clickhouse-server >nul 2>&1
    if errorlevel 1 (echo       ClickHouse was not running) else (echo       ClickHouse stopped)
  ) else (
    echo       ClickHouse left running. Use --stop-clickhouse to stop it.
  )
)

echo [3/3] Verifying ports...
for %%p in (%BACKEND_PORT% %SYNC_PORT% %FRONTEND_PORT% 3501 5173) do (
  netstat -ano 2>nul | findstr ":%%p " | findstr "LISTENING" >nul 2>&1
  if not errorlevel 1 (
    echo       WARN: port %%p is still listening
  )
)
echo       Done
echo       miniQMT client is external and was left running.

echo.
echo ========================================
echo   Shutdown complete
echo ========================================
echo.
if "%NO_PAUSE%"=="0" pause
exit /b 0
