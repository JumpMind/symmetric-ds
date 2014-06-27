@echo off

set PRGDIR=%~dp0
set LIBDIR=%PRGDIR%\..
set CLASSPATH=.

for %%i in ("%LIBDIR%\lib\*.jar") do call %PRGDIR%\cpappend.bat "%%i"

rem echo CLASSPATH=%CLASSPATH%

java -cp %CLASSPATH% org.jumpmind.symmetric.SymmetricLauncher %1 %2 %3 %4 %5 %6 %7 %8 %9
