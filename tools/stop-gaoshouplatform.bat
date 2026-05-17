@echo off
setlocal EnableExtensions
chcp 65001 >nul
title GaoshouPlatform Shutdown
set "NO_PAUSE=0"
if /i "%~1"=="--no-pause" set "NO_PAUSE=1"

echo ========================================
echo   GaoshouPlatform Shutdown
echo ========================================
echo.

echo [1/3] Stopping backend/frontend processes...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$self=$PID; $parent=(Get-CimInstance Win32_Process -Filter ('ProcessId=' + $PID) -ErrorAction SilentlyContinue).ParentProcessId; $ports=8000,3500,3501,5173; $ids=@(); Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object { $ports -contains $_.LocalPort } | ForEach-Object { $p=Get-CimInstance Win32_Process -Filter ('ProcessId=' + $_.OwningProcess) -ErrorAction SilentlyContinue; if ($p -and $p.CommandLine -match 'GaoshouPlatform|uvicorn app\.main:app|node_modules.*vite|vite\.js') { $ids += [int]$p.ProcessId } }; Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and (($_.CommandLine -match 'GaoshouPlatform' -and $_.CommandLine -match 'uvicorn app\.main:app|node_modules.*vite|vite\.js') -or ($_.CommandLine -like '*GaoshouPlatform.bat*' -and $_.ProcessId -ne $self -and $_.ProcessId -ne $parent)) } | ForEach-Object { $ids += [int]$_.ProcessId }; $ids | Select-Object -Unique | ForEach-Object { Write-Host ('      Killing PID ' + $_); Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }"
timeout /t 1 /nobreak >nul
echo       OK

echo [2/3] Stopping Docker containers...
docker stop redis-server >nul 2>&1
if errorlevel 1 (
  echo       Redis was not running
) else (
  echo       Redis stopped
)
docker stop clickhouse-server >nul 2>&1
if errorlevel 1 (
  echo       ClickHouse was not running
) else (
  echo       ClickHouse stopped
)

echo [3/3] Verifying ports...
for %%p in (8000 3500 3501 5173) do (
  netstat -ano 2>nul | findstr ":%%p " | findstr "LISTENING" >nul 2>&1
  if not errorlevel 1 (
    echo       WARN: port %%p is still listening
  )
)
echo       Done

echo.
echo ========================================
echo   Shutdown complete
echo ========================================
echo.
if "%NO_PAUSE%"=="0" pause
exit /b 0
