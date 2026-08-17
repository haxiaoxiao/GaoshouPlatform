@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title GaoshouPlatform Launcher

for %%I in ("%~dp0..") do set "SCRIPT_ROOT=%%~fI"
if defined GAOSHOU_ROOT (set "ROOT=%GAOSHOU_ROOT%") else (set "ROOT=%SCRIPT_ROOT%")
set "BACKEND_DIR=%ROOT%\backend"
set "FRONTEND_DIR=%ROOT%\frontend"
if defined GAOSHOU_ENV_FILE (
  set "ENV_FILE=%GAOSHOU_ENV_FILE%"
) else if exist "%ROOT%\.env.local" (
  set "ENV_FILE=%ROOT%\.env.local"
) else (
  set "ENV_FILE=%ROOT%\.env"
)
if defined GAOSHOU_PYTHON (set "PYTHON=%GAOSHOU_PYTHON%") else (set "PYTHON=%BACKEND_DIR%\.venv\Scripts\python.exe")

if defined GAOSHOU_BACKEND_HOST (set "BACKEND_HOST=%GAOSHOU_BACKEND_HOST%") else (set "BACKEND_HOST=127.0.0.1")
if defined GAOSHOU_BACKEND_PORT (set "BACKEND_PORT=%GAOSHOU_BACKEND_PORT%") else (set "BACKEND_PORT=8800")
if defined GAOSHOU_SYNC_HOST (set "SYNC_HOST=%GAOSHOU_SYNC_HOST%") else (set "SYNC_HOST=127.0.0.1")
if defined GAOSHOU_SYNC_PORT (set "SYNC_PORT=%GAOSHOU_SYNC_PORT%") else (set "SYNC_PORT=8810")
if defined GAOSHOU_FRONTEND_HOST (set "FRONTEND_HOST=%GAOSHOU_FRONTEND_HOST%") else (set "FRONTEND_HOST=127.0.0.1")
if defined GAOSHOU_FRONTEND_PORT (set "FRONTEND_PORT=%GAOSHOU_FRONTEND_PORT%") else (set "FRONTEND_PORT=3511")

set "NO_PAUSE=0"
if /i "%~1"=="--no-pause" set "NO_PAUSE=1"
if "%GAOSHOU_SKIP_PAUSE%"=="1" set "NO_PAUSE=1"
set "SKIP_OPTIONAL_CHECKS=0"
if "%GAOSHOU_SKIP_OPTIONAL_CHECKS%"=="1" set "SKIP_OPTIONAL_CHECKS=1"
if "%GAOSHOU_SKIP_DOCKER%"=="1" set "SKIP_OPTIONAL_CHECKS=1"
set "OPEN_BROWSER=1"
if "%GAOSHOU_OPEN_BROWSER%"=="0" set "OPEN_BROWSER=0"

set "MARKET_DATA_BACKEND=parquet"
set "GAOSHOU_DATA_DIR="
set "PARQUET_DATA_DIR="
set "FACTOR_VALUE_STORE_DIR="
set "DATABASE_URL="
set "REDIS_PORT=16379"
set "QMT_ACCOUNT_ID="
set "QMT_TRADER_PATH="
set "TUSHARE_TOKEN="
set "TS_TOKEN="
set "LIVE_TRADING_ENABLE_ORDER_SUBMIT=false"
set "LIVE_TRADING_AUTO_EXECUTE_ENABLED=false"
if exist "%ENV_FILE%" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    set "K=%%a"
    set "V=%%b"
    if /i "!K!"=="GAOSHOU_DATA_DIR" set "GAOSHOU_DATA_DIR=!V!"
    if /i "!K!"=="PARQUET_DATA_DIR" set "PARQUET_DATA_DIR=!V!"
    if /i "!K!"=="FACTOR_VALUE_STORE_DIR" set "FACTOR_VALUE_STORE_DIR=!V!"
    if /i "!K!"=="DATABASE_URL" set "DATABASE_URL=!V!"
    if /i "!K!"=="MARKET_DATA_BACKEND" set "MARKET_DATA_BACKEND=!V!"
    if /i "!K!"=="REDIS_PORT" set "REDIS_PORT=!V!"
    if /i "!K!"=="QMT_ACCOUNT_ID" set "QMT_ACCOUNT_ID=!V!"
    if /i "!K!"=="QMT_TRADER_PATH" set "QMT_TRADER_PATH=!V!"
    if /i "!K!"=="TUSHARE_TOKEN" set "TUSHARE_TOKEN=!V!"
    if /i "!K!"=="TS_TOKEN" set "TS_TOKEN=!V!"
    if /i "!K!"=="LIVE_TRADING_ENABLE_ORDER_SUBMIT" set "LIVE_TRADING_ENABLE_ORDER_SUBMIT=!V!"
    if /i "!K!"=="LIVE_TRADING_AUTO_EXECUTE_ENABLED" set "LIVE_TRADING_AUTO_EXECUTE_ENABLED=!V!"
    if /i "!K!"=="BACKEND_PORT" if not defined GAOSHOU_BACKEND_PORT set "BACKEND_PORT=!V!"
    if /i "!K!"=="SYNC_SERVICE_PORT" if not defined GAOSHOU_SYNC_PORT set "SYNC_PORT=!V!"
    if /i "!K!"=="SYNC_PORT" if not defined GAOSHOU_SYNC_PORT set "SYNC_PORT=!V!"
    if /i "!K!"=="FRONTEND_PORT" if not defined GAOSHOU_FRONTEND_PORT set "FRONTEND_PORT=!V!"
  )
)
if not defined TS_TOKEN if defined TUSHARE_TOKEN set "TS_TOKEN=%TUSHARE_TOKEN%"
if not defined TUSHARE_TOKEN if defined TS_TOKEN set "TUSHARE_TOKEN=%TS_TOKEN%"

set "BACKEND_URL=http://%BACKEND_HOST%:%BACKEND_PORT%/health"
set "RADAR_URL=http://%BACKEND_HOST%:%BACKEND_PORT%/api/market-radar/overview"
set "SYNC_URL=http://%SYNC_HOST%:%SYNC_PORT%/health"
set "SYNC_SERVICE_URL=http://%SYNC_HOST%:%SYNC_PORT%"
set "SYNC_SERVICE_PORT=%SYNC_PORT%"
set "QMT_ACCOUNT_MASK=not configured"
if defined QMT_ACCOUNT_ID set "QMT_ACCOUNT_MASK=!QMT_ACCOUNT_ID:~0,2!***!QMT_ACCOUNT_ID:~-2!"

echo ========================================
echo   GaoshouPlatform Startup
echo ========================================
echo Root:      %ROOT%
echo Env file:  %ENV_FILE%
echo Backend:   http://%BACKEND_HOST%:%BACKEND_PORT%
echo Sync:      http://%SYNC_HOST%:%SYNC_PORT%
echo Frontend:  preferred port %FRONTEND_PORT% (dynamic fallback enabled)
echo Data mode: %MARKET_DATA_BACKEND%  storage=Parquet/DuckDB
if defined GAOSHOU_DATA_DIR echo Data root: %GAOSHOU_DATA_DIR%
if defined PARQUET_DATA_DIR echo Parquet:   %PARQUET_DATA_DIR%
if defined FACTOR_VALUE_STORE_DIR echo Factors:   %FACTOR_VALUE_STORE_DIR%
if defined DATABASE_URL echo SQLite:   %DATABASE_URL%
echo miniQMT:   account %QMT_ACCOUNT_MASK%  order_submit=%LIVE_TRADING_ENABLE_ORDER_SUBMIT%  auto_execute=%LIVE_TRADING_AUTO_EXECUTE_ENABLED%
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

echo [1/8] Stopping stale project processes on configured ports...
call :stop_project_processes
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 1"
call :resolve_frontend_port
if errorlevel 1 (
  echo [ERROR] No usable frontend port was found.
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
set "FRONTEND_URL=http://%FRONTEND_HOST%:%FRONTEND_PORT%"
call :assert_ports_free
if errorlevel 1 (
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo [2/8] Optional Redis handling...
if "%SKIP_OPTIONAL_CHECKS%"=="1" (
  echo       SKIP: optional Redis/Docker checks disabled for this startup.
) else (
  echo       Starting Redis on port %REDIS_PORT% if Docker is available...
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
)

echo [3/8] Market data storage...
if /i not "%MARKET_DATA_BACKEND%"=="parquet" (
  echo       WARN: MARKET_DATA_BACKEND=%MARKET_DATA_BACKEND% is ignored; Parquet/DuckDB is the only supported backend.
)
if not defined GAOSHOU_DATA_DIR (
  echo       WARN: GAOSHOU_DATA_DIR is not configured in %ENV_FILE%.
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "if(-not (Test-Path -LiteralPath '%GAOSHOU_DATA_DIR%')){ exit 1 }" >nul 2>&1
  if errorlevel 1 echo       WARN: GAOSHOU_DATA_DIR does not exist or is not reachable from this shell: %GAOSHOU_DATA_DIR%
)
if not defined PARQUET_DATA_DIR (
  echo       WARN: PARQUET_DATA_DIR is not configured in %ENV_FILE%.
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "if(-not (Test-Path -LiteralPath '%PARQUET_DATA_DIR%')){ exit 1 }" >nul 2>&1
  if errorlevel 1 echo       WARN: PARQUET_DATA_DIR does not exist or is not reachable from this shell: %PARQUET_DATA_DIR%
)
if defined FACTOR_VALUE_STORE_DIR (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "if(-not (Test-Path -LiteralPath '%FACTOR_VALUE_STORE_DIR%\_manifest.json')){ exit 1 }" >nul 2>&1
  if errorlevel 1 (
    echo       ERROR: FACTOR_VALUE_STORE_DIR has no validated manifest: %FACTOR_VALUE_STORE_DIR%
    if "%NO_PAUSE%"=="0" pause
    exit /b 1
  )
)
echo       OK: Parquet/DuckDB mode

echo [4/8] Applying database migrations...
pushd "%BACKEND_DIR%"
"%PYTHON%" -m alembic -c alembic.ini upgrade head
if errorlevel 1 (
  popd
  echo       ERROR: Database migration failed. Backend services were not started.
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
popd
echo       OK

echo [5/8] Starting sync service on %SYNC_HOST%:%SYNC_PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%PYTHON%' -ArgumentList @('-m','app.service_runner','app.sync_main:app','--host','%SYNC_HOST%','--port','%SYNC_PORT%','--pid-file','%ROOT%\.runtime\sync-service.pid') -WorkingDirectory '%BACKEND_DIR%' -WindowStyle Hidden"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$url='%SYNC_URL%'; $ok=$false; for($i=0; $i -lt 60; $i++){ try { Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null; $ok=$true; break } catch { Start-Sleep -Seconds 1 } }; if(-not $ok){ exit 1 }"
if errorlevel 1 (
  echo       ERROR: Sync service health check failed: %SYNC_URL%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo [6/8] Starting backend on %BACKEND_HOST%:%BACKEND_PORT%...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%PYTHON%' -ArgumentList @('-m','app.service_runner','app.main:app','--host','%BACKEND_HOST%','--port','%BACKEND_PORT%','--pid-file','%ROOT%\.runtime\backend-api.pid') -WorkingDirectory '%BACKEND_DIR%' -WindowStyle Hidden"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$url='%BACKEND_URL%'; $ok=$false; for($i=0; $i -lt 60; $i++){ try { Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 | Out-Null; $ok=$true; break } catch { Start-Sleep -Seconds 1 } }; if(-not $ok){ exit 1 }"
if errorlevel 1 (
  echo       ERROR: Backend health check failed: %BACKEND_URL%
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
echo       OK

echo [7/8] Checking miniQMT live-trading bridge...
set "RADAR_STATUS="
set "RADAR_MODE="
set "RADAR_AS_OF="
for /f "usebackq tokens=1,2,3 delims=|" %%a in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r=Invoke-RestMethod -Uri '%RADAR_URL%' -TimeoutSec 5; Write-Output ($r.status.ToString() + '|' + $r.realtime_mode.ToString() + '|' + $(if($null -eq $r.as_of){'none'}else{$r.as_of.ToString()})) } catch { exit 1 }"`) do (
  set "RADAR_STATUS=%%a"
  set "RADAR_MODE=%%b"
  set "RADAR_AS_OF=%%c"
)
if not defined RADAR_MODE (
  echo       WARN: Market radar status is unavailable at %RADAR_URL%.
) else (
  echo       Market radar: status=!RADAR_STATUS! mode=!RADAR_MODE! as_of=!RADAR_AS_OF!
  if /i "!RADAR_MODE!"=="offline" echo       NOTICE: miniQMT realtime is offline; the latest daily radar snapshot remains available.
  if /i "!RADAR_MODE!"=="polling_30s" echo       NOTICE: realtime push is degraded; radar is using the 30-second QMT polling fallback.
)
if not defined QMT_ACCOUNT_ID (
  echo       SKIP: miniQMT account is optional and QMT_ACCOUNT_ID is not configured.
) else if not defined QMT_TRADER_PATH (
  echo       SKIP: miniQMT account is optional and QMT_TRADER_PATH is not configured.
) else (
  echo       OPTIONAL: miniQMT account config found. Open the miniQMT client before using /live.
  echo       OPTIONAL: status can be checked at http://%BACKEND_HOST%:%BACKEND_PORT%/api/live-trading/status
)

echo [8/8] Building and starting frontend on %FRONTEND_HOST%:%FRONTEND_PORT%...
pushd "%FRONTEND_DIR%"
call npm run build
if errorlevel 1 (
  popd
  echo       ERROR: Frontend production build failed.
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)
popd
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath 'cmd.exe' -ArgumentList @('/c','set VITE_API_PROXY_TARGET=http://%BACKEND_HOST%:%BACKEND_PORT%&& npm run preview -- --host %FRONTEND_HOST% --port %FRONTEND_PORT% --strictPort') -WorkingDirectory '%FRONTEND_DIR%' -WindowStyle Hidden"
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
echo Market radar:  %FRONTEND_URL%/market-radar
echo Radar status:  %RADAR_URL%
echo Sync health:   http://%SYNC_HOST%:%SYNC_PORT%/health
echo Live trading:  %FRONTEND_URL%/trade
echo Frontend:      %FRONTEND_URL%
echo.
if "%OPEN_BROWSER%"=="1" start "" "%FRONTEND_URL%/data"
if "%NO_PAUSE%"=="0" pause
exit /b 0

:resolve_frontend_port
set "PREFERRED_FRONTEND_PORT=%FRONTEND_PORT%"
set "FRONTEND_PORT="
for /f "usebackq delims=" %%p in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$candidates=@([int]'%PREFERRED_FRONTEND_PORT%') + (3511..3599); $seen=@{}; foreach($port in $candidates){ if($seen.ContainsKey($port)){ continue }; $seen[$port]=$true; $listener=$null; try { $listener=[System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback,$port); $listener.Start(); $listener.Stop(); Write-Output $port; exit 0 } catch { if($listener){$listener.Stop()} } }; exit 1"`) do set "FRONTEND_PORT=%%p"
if not defined FRONTEND_PORT exit /b 1
if not "%FRONTEND_PORT%"=="%PREFERRED_FRONTEND_PORT%" echo       WARN: frontend port %PREFERRED_FRONTEND_PORT% is unavailable; using %FRONTEND_PORT%.
if not exist "%ROOT%\.runtime" mkdir "%ROOT%\.runtime" >nul 2>&1
>"%ROOT%\.runtime\frontend-port.txt" echo %FRONTEND_PORT%
exit /b 0

:stop_project_processes
set "STALE_FRONTEND_PORT=%FRONTEND_PORT%"
if exist "%ROOT%\.runtime\frontend-port.txt" set /p STALE_FRONTEND_PORT=<"%ROOT%\.runtime\frontend-port.txt"
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%\tools\stop-gaoshouplatform-services.ps1" -ProjectRoot "%ROOT%" -BackendPort "%BACKEND_PORT%" -SyncPort "%SYNC_PORT%" -FrontendPort "%STALE_FRONTEND_PORT%" -GracefulTimeoutSeconds 20
if errorlevel 1 echo       WARN: verified stale-process shutdown failed; ports will be checked without killing unknown owners.
exit /b 0

:assert_ports_free
set "PORTS_BUSY=0"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ports=@([int]'%BACKEND_PORT%',[int]'%SYNC_PORT%'); $ranges=netsh interface ipv4 show excludedportrange protocol=tcp | Select-String '^\s*(\d+)\s+(\d+)\s*$'; foreach($match in $ranges){$start=[int]$match.Matches[0].Groups[1].Value; $end=[int]$match.Matches[0].Groups[2].Value; foreach($port in $ports){if($port -ge $start -and $port -le $end){Write-Host ('      ERROR: port ' + $port + ' is reserved by Windows (' + $start + '-' + $end + ')'); exit 1}}}"
if errorlevel 1 exit /b 1
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
