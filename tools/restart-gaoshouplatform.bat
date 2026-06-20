@echo off
setlocal EnableExtensions
chcp 65001 >nul
title GaoshouPlatform Restart

for %%I in ("%~dp0..") do set "SCRIPT_ROOT=%%~fI"
if defined GAOSHOU_ROOT (set "ROOT=%GAOSHOU_ROOT%") else (set "ROOT=%SCRIPT_ROOT%")
set "STOP=%ROOT%\tools\stop-gaoshouplatform.bat"
set "START=%ROOT%\tools\start-gaoshouplatform.bat"

echo ========================================
echo   GaoshouPlatform Restart
echo ========================================
echo.

if not exist "%STOP%" (
  echo [ERROR] %STOP% not found
  exit /b 1
)
if not exist "%START%" (
  echo [ERROR] %START% not found
  exit /b 1
)

echo Stopping...
call "%STOP%" --no-pause
echo.
timeout /t 2 /nobreak >nul
echo Starting...
call "%START%" --no-pause
exit /b 0
