@echo off
setlocal
call "%~dp0ai-native-startup\stop-gaoshouplatform-ai-native.bat" %*
exit /b %ERRORLEVEL%
