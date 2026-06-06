@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title GaoshouPlatform Launcher

for %%I in ("%~dp0..") do set "SCRIPT_ROOT=%%~fI"
if defined GAOSHOU_ROOT (set "ROOT=%GAOSHOU_ROOT%") else (set "ROOT=%SCRIPT_ROOT%")
set "BACKEND_DIR=%ROOT%\backend"
set "FRONTEND_DIR=%ROOT%\frontend"
if defined GAOSHOU_ENV_FILE (set "ENV_FILE=%GAOSHOU_ENV_FILE%") else (set "ENV_FILE=%ROOT%\.env.local")
if defined GAOSHOU_PYTHON (set "PYTHON=%GAOSHOU_PYTHON%") else (set "PYTHON=%BACKEND_DIR%\.venv\Scripts\python.exe")

if defined GAOSHOU_BACKEND_HOST (set "BACKEND_HOST=%GAOSHOU_BACKEND_HOST%") else (set "BACKEND_HOST=127.0.0.1")
if defined GAOSHOU_BACKEND_PORT (set "BACKEND_PORT=%GAOSHOU_BACKEND_PORT%") else (set "BACKEND_PORT=18800")
if defined GAOSHOU_SYNC_HOST (set "SYNC_HOST=%GAOSHOU_SYNC_HOST%") else (set "SYNC_HOST=127.0.0.1")
if defined GAOSHOU_SYNC_PORT (set "SYNC_PORT=%GAOSHOU_SYNC_PORT%") else (set "SYNC_PORT=18810")
if defined GAOSHOU_FRONTEND_HOST (set "FRONTEND_HOST=%GAOSHOU_FRONTEND_HOST%") else (set "FRONTEND_HOST=127.0.0.1")
if defined GAOSHOU_FRONTEND_PORT (set "FRONTEND_PORT=%GAOSHOU_FRONTEND_PORT%") else (set "FRONTEND_PORT=13500")

set "NO_PAUSE=0"
if /i "%~1"=="--no-pause" set "NO_PAUSE=1"
if "%GAOSHOU_SKIP_PAUSE%"=="1" set "NO_PAUSE=1"

set "MARKET_DATA_BACKEND=parquet"
set "CLICKHOUSE_ENABLED=false"
set "REDIS_PORT=16379"
set "QMT_ACCOUNT_ID="
set "QMT_TRADER_PATH="
set "GRID_TRADING_ENABLE_ORDER_SUBMIT=false"
if exist "%ENV_FILE%" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    set "K=%%a"
    set "V=%%b"
    if /i "!K!"=="MARKET_DATA_BACKEND" set "MARKET_DATA_BACKEND=!V!"
    if /i "!K!"=="CLICKHOUSE_ENABLED" set "CLICKHOUSE_ENABLED=!V!"
    if /i "!K!"=="REDIS_PORT" set "REDIS_PORT=!V!"
    if /i "!K!"=="QMT_ACCOUNT_ID" set "QMT_ACCOUNT_ID=!V!"
    if /i "!K!"=="QMT_TRADER_PATH" set "QMT_TRADER_PATH=!V!"
    if /i "!K!"=="GRID_TRADING_ENABLE_ORDER_SUBMIT" set "GRID_TRADING_ENABLE_ORDER_SUBMIT=!V!"
    if /i "!K!"=="BACKEND_PORT" if not defined GAOSHOU_BACKEND_PORT set "BACKEND_PORT=!V!"
    if /i "!K!"=="SYNC_SERVICE_PORT" if not defined GAOSHOU_SYNC_PORT set "SYNC_PORT=!V!"
    if /i "!K!"=="SYNC_PORT" if not defined GAOSHOU_SYNC_PORT set "SYNC_PORT=!V!"
    if /i "!K!"=="FRONTEND_PORT" if not defined GAOSHOU_FRONTEND_PORT set "FRONTEND_PORT=!V!"
  )
)

set "BACKEND_URL=http://%BACKEND_HOST%:%BACKEND_PORT%/health"
set "SYNC_URL=http://%SYNC_HOST%:%SYNC_PORT%/health"
set "FRONTEND_URL=http://%FRONTEND_HOST%:%FRONTEND_PORT%"
set "SYNC_SERVICE_URL=http://%SYNC_HOST%:%SYNC_PORT%"
set "SYNC_SERVICE_PORT=%SYNC_PORT%"
set "QMT_ACCOUNT_STATUS=not configured"
if defined QMT_ACCOUNT_ID set "QMT_ACCOUNT_STATUS=configured"

echo ========================================
echo   GaoshouPlatform Startup
echo ========================================
echo Root:      %ROOT%
echo Env file:  %ENV_FILE%
echo Backend:   http://%BACKEND_HOST%:%BACKEND_PORT%
echo Sync:      http://%SYNC_HOST%:%SYNC_PORT%
echo Frontend:  %FRONTEND_URL%
echo Data mode: %MARKET_DATA_BACKEND%  ClickHouse=%CLICKHOUSE_ENABLED%
echo miniQMT:   account %QMT_ACCOUNT_STATUS%  order_submit=%GRID_TRADING_ENABLE_ORDER_SUBMIT%
echo.

if not exist "%ROOT%" (
  echo [ERROR] Project root not found: %ROOT%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
if not exist "%PYTHON%" (
  echo [ERROR] Backend Python not found: %PYTHON%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
if not exist "%FRONTEND_DIR%\package.json" (
  echo [ERROR] Frontend package.json not found: %FRONTEND_DIR%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)

echo [1/7] Stopping stale project processes on configured ports...
call :stop_project_processes
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 1"
call :assert_ports_free
if errorlevel 1 (
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo [2/7] Starting Redis on port %REDIS_PORT% if Docker is available...
where docker >nul 2>&1
if errorlevel 1 (
  echo       WARN: Docker not found. Continue without Redis.
) else (
  docker start redis-server >nul 2>&1
  if errorlevel 1 (
    docker run -d --name redis-server -p %REDIS_PORT%:6379 redis:7-alpine >nul 2>&1
  )
  docker ps --format "{{.Names}}" 2>nul | findstr /x "redis-server" >nul 2>&1
  if errorlevel 1 (
    echo       WARN: Redis is not running. Continue without Redis.
  ) else (
    echo       OK
  )
)

echo [3/7] ClickHouse handling...
set "NEED_CLICKHOUSE=0"
if /i "%MARKET_DATA_BACKEND%"=="clickhouse" set "NEED_CLICKHOUSE=1"
if /i "%CLICKHOUSE_ENABLED%"=="true" set "NEED_CLICKHOUSE=1"
if "%NEED_CLICKHOUSE%"=="1" (
  where docker >nul 2>&1
  if errorlevel 1 (
    echo       WARN: Docker not found. ClickHouse cannot be started.
  ) else (
    docker start clickhouse-server >nul 2>&1
    docker ps --format "{{.Names}}" 2>nul | findstr /x "clickhouse-server" >nul 2>&1
    if errorlevel 1 (
      echo       WARN: ClickHouse is not running.
    ) else (
      echo       OK
    )
  )
) else (
  echo       SKIP: Parquet/DuckDB mode does not require ClickHouse.
)

echo [4/7] Starting sync service on %SYNC_HOST%:%SYNC_PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%PYTHON%' -ArgumentList @('-m','uvicorn','app.sync_main:app','--host','%SYNC_HOST%','--port','%SYNC_PORT%') -WorkingDirectory '%BACKEND_DIR%' -WindowStyle Hidden"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$url='%SYNC_URL%'; $ok=$false; for($i=0; $i -lt 60; $i++){ try { Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null; $ok=$true; break } catch { Start-Sleep -Seconds 1 } }; if(-not $ok){ exit 1 }"
if errorlevel 1 (
  echo       ERROR: Sync service health check failed: %SYNC_URL%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo [5/7] Starting backend on %BACKEND_HOST%:%BACKEND_PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%PYTHON%' -ArgumentList @('-m','uvicorn','app.main:app','--host','%BACKEND_HOST%','--port','%BACKEND_PORT%') -WorkingDirectory '%BACKEND_DIR%' -WindowStyle Hidden"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$url='%BACKEND_URL%'; $ok=$false; for($i=0; $i -lt 60; $i++){ try { Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null; $ok=$true; break } catch { Start-Sleep -Seconds 1 } }; if(-not $ok){ exit 1 }"
if errorlevel 1 (
  echo       ERROR: Backend health check failed: %BACKEND_URL%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo [6/7] Checking miniQMT live-trading bridge...
if not defined QMT_ACCOUNT_ID (
  echo       SKIP: miniQMT account is optional and QMT_ACCOUNT_ID is not configured.
) else if not defined QMT_TRADER_PATH (
  echo       SKIP: miniQMT account is optional and QMT_TRADER_PATH is not configured.
) else (
  echo       OPTIONAL: miniQMT account config found. Open the miniQMT client before using /live.
  echo       OPTIONAL: status can be checked at http://%BACKEND_HOST%:%BACKEND_PORT%/api/grid-trading/status
)

echo [7/7] Starting frontend on %FRONTEND_HOST%:%FRONTEND_PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath 'cmd.exe' -ArgumentList @('/c','set VITE_API_PROXY_TARGET=http://%BACKEND_HOST%:%BACKEND_PORT%&& npm run dev -- --host %FRONTEND_HOST% --port %FRONTEND_PORT% --strictPort') -WorkingDirectory '%FRONTEND_DIR%' -WindowStyle Hidden"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$url='%FRONTEND_URL%'; $ok=$false; for($i=0; $i -lt 60; $i++){ try { Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null; $ok=$true; break } catch { Start-Sleep -Seconds 1 } }; if(-not $ok){ exit 1 }"
if errorlevel 1 (
  echo       ERROR: Frontend did not bind to %FRONTEND_URL%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo.
echo ========================================
echo   Startup complete
echo ========================================
echo Backend docs:  http://%BACKEND_HOST%:%BACKEND_PORT%/docs
echo Backend API:   http://%BACKEND_HOST%:%BACKEND_PORT%/api/system/status
echo Sync health:   http://%SYNC_HOST%:%SYNC_PORT%/health
echo Live trading:  %FRONTEND_URL%/live
echo Frontend:      %FRONTEND_URL%
echo.
if "%NO_PAUSE%"=="0" pause
exit /b 0

:stop_project_processes
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ports=@([int]'%BACKEND_PORT%',[int]'%SYNC_PORT%',[int]'%FRONTEND_PORT%'); $ids=@(); Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort } | ForEach-Object { $p=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $_.OwningProcess) -ErrorAction SilentlyContinue; if($p -and $p.CommandLine -and ($p.CommandLine -match 'uvicorn app\.(main|sync_main):app|npm run dev|node_modules.*vite|vite\.js')) { $ids += [int]$p.ProcessId } }; $ids | Select-Object -Unique | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }"
exit /b 0

:assert_ports_free
set "PORTS_BUSY=0"
for %%p in (%BACKEND_PORT% %SYNC_PORT% %FRONTEND_PORT%) do (
  netstat -ano 2>nul | findstr ":%%p " | findstr "LISTENING" >nul 2>&1
  if not errorlevel 1 (
    echo       ERROR: port %%p is still listening
    set "PORTS_BUSY=1"
  )
)
if "%PORTS_BUSY%"=="1" (
  echo       Unable to start because one or more configured ports are still occupied.
  exit /b 1
)
exit /b 0
