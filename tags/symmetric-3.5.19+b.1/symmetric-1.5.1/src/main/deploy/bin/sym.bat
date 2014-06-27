@echo off

set PRGDIR=%~dp0
set LIBDIR=%PRGDIR%\..
set CLASSPATH=.

for %%i in ("%LIBDIR%\lib\*.jar") do call "%PRGDIR%\cpappend.bat" %%i

echo CLASSPATH=%CLASSPATH%

java org.jumpmind.symmetric.SymmetricLauncher %1 %2 %3 %4 %5 %6 %7 %8 %9
