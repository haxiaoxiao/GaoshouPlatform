@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title GaoshouPlatform Launcher

set "ROOT=E:\Projects\GaoshouPlatform"
set "BACKEND_DIR=%ROOT%\backend"
set "FRONTEND_DIR=%ROOT%\frontend"
set "PYTHON=%BACKEND_DIR%\.venv\Scripts\python.exe"
set "BACKEND_URL=http://127.0.0.1:8000/health"
set "FRONTEND_URL=http://127.0.0.1:3500"
set "NO_PAUSE=0"
if /i "%~1"=="--no-pause" set "NO_PAUSE=1"

echo ========================================
echo   GaoshouPlatform Startup
echo ========================================
echo.

if not exist "%ROOT%" (
  echo [ERROR] Project root not found: %ROOT%
  goto fail
)

if not exist "%PYTHON%" (
  echo [ERROR] Backend Python not found: %PYTHON%
  goto fail
)

if not exist "%FRONTEND_DIR%\package.json" (
  echo [ERROR] Frontend package.json not found: %FRONTEND_DIR%
  goto fail
)

echo [1/5] Cleaning old project processes...
call :stop_project_processes
timeout /t 1 /nobreak >nul
echo       OK

echo [2/5] Starting Redis...
docker start redis-server >nul 2>&1
if errorlevel 1 (
  docker run -d --name redis-server -p 16379:6379 redis:7-alpine >nul 2>&1
)
docker ps --format "{{.Names}}" | findstr /x "redis-server" >nul 2>&1
if errorlevel 1 (
  echo       WARN: Redis is not running. Continue without Redis.
) else (
  echo       OK
)

echo [3/5] Starting ClickHouse...
docker start clickhouse-server >nul 2>&1
docker ps --format "{{.Names}}" | findstr /x "clickhouse-server" >nul 2>&1
if errorlevel 1 (
  echo       WARN: ClickHouse is not running. Data APIs may fail.
) else (
  echo       OK
)

echo [4/5] Starting backend on 127.0.0.1:8000...
start "GaoshouPlatform-Backend" /D "%BACKEND_DIR%" cmd /k ""%PYTHON%" -m uvicorn app.main:app --host 127.0.0.1 --port 8000"
call :wait_http "%BACKEND_URL%" 60
if errorlevel 1 (
  echo       ERROR: Backend health check failed: %BACKEND_URL%
  goto fail
)
echo       OK

echo [5/5] Starting frontend on 127.0.0.1:3500...
start "GaoshouPlatform-Frontend" /D "%FRONTEND_DIR%" cmd /k "npm run dev -- --host 127.0.0.1 --port 3500 --strictPort"
call :wait_http "%FRONTEND_URL%" 60
if errorlevel 1 (
  echo       ERROR: Frontend did not bind to %FRONTEND_URL%
  goto fail
)
echo       OK

echo.
echo ========================================
echo   Startup complete
echo ========================================
echo Backend docs:  http://127.0.0.1:8000/docs
echo Frontend:      http://127.0.0.1:3500
echo.
if "%NO_PAUSE%"=="0" pause
exit /b 0

:fail
echo.
echo ========================================
echo   Startup failed
echo ========================================
echo Check the backend/frontend windows for details.
echo.
if "%NO_PAUSE%"=="0" pause
exit /b 1

:wait_http
set "URL=%~1"
set /a MAX_SECONDS=%~2
set /a WAITED=0
:wait_http_loop
curl -fsS "%URL%" >nul 2>&1
if not errorlevel 1 exit /b 0
if !WAITED! GEQ !MAX_SECONDS! exit /b 1
set /a WAITED+=1
timeout /t 1 /nobreak >nul
goto wait_http_loop

:stop_project_processes
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$self=$PID; $parent=(Get-CimInstance Win32_Process -Filter ('ProcessId=' + $PID) -ErrorAction SilentlyContinue).ParentProcessId; $ports=8000,3500,3501,5173; $ids=@(); Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort } | ForEach-Object { $p=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $_.OwningProcess) -ErrorAction SilentlyContinue; if ($p -and $p.CommandLine -match 'GaoshouPlatform|uvicorn app\.main:app|node_modules.*vite|vite\.js') { $ids += [int]$p.ProcessId } }; Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and (($_.CommandLine -match 'GaoshouPlatform' -and $_.CommandLine -match 'uvicorn app\.main:app|node_modules.*vite|vite\.js') -or ($_.CommandLine -like '*GaoshouPlatform.bat*' -and $_.ProcessId -ne $self -and $_.ProcessId -ne $parent)) } | ForEach-Object { $ids += [int]$_.ProcessId }; $ids | Select-Object -Unique | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }"
exit /b 0
