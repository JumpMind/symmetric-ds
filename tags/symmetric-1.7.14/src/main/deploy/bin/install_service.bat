@echo off
rem ---------------------------------------------------------------------------
rem Install Symmetric as a Windows Service
rem ---------------------------------------------------------------------------

set APPDIR=%~dp0\..
set PRGDIR=%APPDIR%\bin
set LIBDIR=%APPDIR%\lib
set CLASSPATH=%APPDIR%

for %%i in ("%LIBDIR%\*.jar") do call "%PRGDIR%\cpappend.bat" %%i

echo on

"%PRGDIR%\sym_service.exe" -install "Symmetric" ^
	"%JAVA_HOME%\jre\bin\client\jvm.dll" ^
	-Djava.class.path="%CLASSPATH%" ^
	-Dlog4j.configuration="file:%APPDIR%\conf\log4j.xml" ^
	-Dsym.keystore.file="%APPDIR%\security\keystore" ^
	-Djavax.net.ssl.trustStore="%APPDIR%\security\cacerts" ^
	-start org.jumpmind.symmetric.SymmetricLauncher ^
	-method main ^
	-params --properties conf/symmetric.properties --port 8080 --server ^
	-out logs/service-out.log ^
	-err logs/service-err.log ^
	-current "%APPDIR%"



