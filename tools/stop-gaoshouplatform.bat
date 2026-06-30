@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
title GaoshouPlatform Shutdown

for %%I in ("%~dp0..") do set "SCRIPT_ROOT=%%~fI"
if defined GAOSHOU_ROOT (set "ROOT=%GAOSHOU_ROOT%") else (set "ROOT=%SCRIPT_ROOT%")
if defined GAOSHOU_ENV_FILE (set "ENV_FILE=%GAOSHOU_ENV_FILE%") else (set "ENV_FILE=%ROOT%\.env.local")
if defined GAOSHOU_BACKEND_PORT (set "BACKEND_PORT=%GAOSHOU_BACKEND_PORT%") else (set "BACKEND_PORT=8800")
if defined GAOSHOU_SYNC_PORT (set "SYNC_PORT=%GAOSHOU_SYNC_PORT%") else (set "SYNC_PORT=8810")
if defined GAOSHOU_FRONTEND_PORT (set "FRONTEND_PORT=%GAOSHOU_FRONTEND_PORT%") else (set "FRONTEND_PORT=3500")
set "STOP_REDIS=0"
set "NO_PAUSE=0"

if /i "%~1"=="--no-pause" set "NO_PAUSE=1"
if "%GAOSHOU_SKIP_PAUSE%"=="1" set "NO_PAUSE=1"
if /i "%~1"=="--stop-redis" set "STOP_REDIS=1"
if /i "%~2"=="--no-pause" set "NO_PAUSE=1"
if /i "%~2"=="--stop-redis" set "STOP_REDIS=1"

set "MARKET_DATA_BACKEND=parquet"
if exist "%ENV_FILE%" (
  for /f "usebackq tokens=1,* delims==" %%a in ("%ENV_FILE%") do (
    set "K=%%a"
    set "V=%%b"
    if /i "!K!"=="MARKET_DATA_BACKEND" set "MARKET_DATA_BACKEND=!V!"
    if /i "!K!"=="BACKEND_PORT" if not defined GAOSHOU_BACKEND_PORT set "BACKEND_PORT=!V!"
    if /i "!K!"=="SYNC_SERVICE_PORT" if not defined GAOSHOU_SYNC_PORT set "SYNC_PORT=!V!"
    if /i "!K!"=="SYNC_PORT" if not defined GAOSHOU_SYNC_PORT set "SYNC_PORT=!V!"
    if /i "!K!"=="FRONTEND_PORT" if not defined GAOSHOU_FRONTEND_PORT set "FRONTEND_PORT=!V!"
  )
)

echo ========================================
echo   GaoshouPlatform Shutdown
echo ========================================
echo Root:      %ROOT%
echo Env file:  %ENV_FILE%
echo Ports:     backend=%BACKEND_PORT% sync=%SYNC_PORT% frontend=%FRONTEND_PORT%
echo.

echo [1/3] Stopping backend/sync/frontend processes on configured ports...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ports=@([int]'%BACKEND_PORT%',[int]'%SYNC_PORT%',[int]'%FRONTEND_PORT%'); $ids=@(); Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort } | ForEach-Object { $p=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $_.OwningProcess) -ErrorAction SilentlyContinue; if($p -and $p.CommandLine -and ($p.CommandLine -match 'uvicorn app\.(main|sync_main):app|npm run dev|node_modules.*vite|vite\.js')) { $ids += [int]$p.ProcessId } }; $ids | Select-Object -Unique | ForEach-Object { Write-Host ('      Killing PID ' + $_); Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }"
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
)

echo [3/3] Verifying configured ports...
set "PORTS_BUSY=0"
for %%p in (%BACKEND_PORT% %SYNC_PORT% %FRONTEND_PORT%) do (
  netstat -ano 2>nul | findstr ":%%p " | findstr "LISTENING" >nul 2>&1
  if not errorlevel 1 (
    echo       ERROR: port %%p is still listening
    set "PORTS_BUSY=1"
  )
)
if "%PORTS_BUSY%"=="1" (
  echo       One or more configured ports are still occupied.
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
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
