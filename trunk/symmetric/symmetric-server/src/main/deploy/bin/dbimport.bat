@echo off

set PRGDIR=%~dp0
set HOMEDIR=%PRGDIR%..
set CONFDIR=%HOMEDIR%\conf
set CLASSPATH=.

for %%i in ("%HOMEDIR%\lib\*.jar") do call "%PRGDIR%cpappend.bat" %%i

for %%i in ("%HOMEDIR%\web\WEB-INF\lib\*.jar") do call "%PRGDIR%cpappend.bat" %%i

rem echo CLASSPATH=%CLASSPATH%

java -Duser.language=en -Djava.io.tmpdir=../tmp -Dsym.keystore.file="%HOMEDIR%\security\keystore" -Djavax.net.ssl.trustStore="%HOMEDIR%\security\cacerts" -Dlog4j.configuration="file:%CONFDIR%\log4j.xml" -Dsun.net.client.defaultReadTimeout=1800000 -Dsun.net.client.defaultConnectTimeout=1800000 org.jumpmind.symmetric.DbImport %1 %2 %3 %4 %5 %6 %7 %8 %9