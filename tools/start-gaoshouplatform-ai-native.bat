@echo off
setlocal
call "%~dp0ai-native-startup\start-gaoshouplatform-ai-native.bat" %*
exit /b %ERRORLEVEL%
