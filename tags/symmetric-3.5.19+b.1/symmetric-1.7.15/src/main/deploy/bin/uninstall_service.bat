@echo off
rem ---------------------------------------------------------------------------
rem Un-Install Symmetric as a Windows Service
rem ---------------------------------------------------------------------------

set APPDIR=%~dp0\..
set PRGDIR=%APPDIR%\bin

echo on

"%PRGDIR%\sym_service.exe" -uninstall "Symmetric"