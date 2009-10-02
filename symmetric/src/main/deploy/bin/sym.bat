@echo off

set PRGDIR=%~dp0
set LIBDIR=%PRGDIR%\..
set CONFDIR=%PRGDIR%\conf
set CLASSPATH=.

for %%i in ("%LIBDIR%\lib\*.jar") do call "%PRGDIR%\cpappend.bat" %%i

rem echo CLASSPATH=%CLASSPATH%

java -Dsym.keystore.file="%PRGDIR%\..\security\keystore" -Djavax.net.ssl.trustStore="%PRGDIR%\..\security\cacerts" -Dlog4j.configuration=file:%CONFDIR%\log4j.xml -Dsun.net.client.defaultReadTimeout=1800000 -Dsun.net.client.defaultConnectTimeout=1800000 org.jumpmind.symmetric.SymmetricLauncher %1 %2 %3 %4 %5 %6 %7 %8 %9
