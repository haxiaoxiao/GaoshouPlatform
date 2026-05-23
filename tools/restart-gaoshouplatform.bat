@echo off
setlocal EnableExtensions
chcp 65001 >nul
title GaoshouPlatform Restart

set "ROOT=E:\Projects\GaoshouPlatform"
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
