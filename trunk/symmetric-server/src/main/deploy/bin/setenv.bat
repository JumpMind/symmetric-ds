@REM
@REM Licensed to JumpMind Inc under one or more contributor
@REM license agreements.  See the NOTICE file distributed
@REM with this work for additional information regarding
@REM copyright ownership.  JumpMind Inc licenses this file
@REM to you under the GNU General Public License, version 3.0 (GPLv3)
@REM (the "License"); you may not use this file except in compliance
@REM with the License.
@REM
@REM You should have received a copy of the GNU General Public License,
@REM version 3.0 (GPLv3) along with this library; if not, see
@REM <http://www.gnu.org/licenses/>.
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off

cd /D %~dp0..
set SYM_HOME=%CD%
cd bin

set SYM_OPTIONS=-Dfile.encoding=utf-8 ^
-Duser.language=en ^
-Djava.io.tmpdir="%SYM_HOME%\tmp" ^
-Dorg.eclipse.jetty.server.Request.maxFormContentSize=800000 ^
-Dorg.eclipse.jetty.server.Request.maxFormKeys=100000 ^
-Dsym.keystore.file="%SYM_HOME%\security\keystore" ^
-Djavax.net.ssl.trustStore="%SYM_HOME%\security\cacerts" ^
-Dlog4j.configuration="file:%SYM_HOME%\conf\log4j.xml" ^
-Dsun.net.client.defaultReadTimeout=1800000 ^
-Dsun.net.client.defaultConnectTimeout=1800000 ^
-XX:+HeapDumpOnOutOfMemoryError ^
-XX:HeapDumpPath="%SYM_HOME%\tmp" 

set SYM_JAVA=java
if /i NOT "%JAVA_HOME%" == "" set SYM_JAVA=%JAVA_HOME%\bin\java

@REM Check whether or not we are using Java 5
for /f "delims=" %%i in ('"%SYM_HOME%\bin\isjava5.bat"') do set isjava5=%%i
if %isjava5% GTR 0 goto :java5

@REM Build classpath using wildcards
set CLASSPATH=%SYM_HOME%\patches;%SYM_HOME%\lib\*;%SYM_HOME%\web\WEB-INF\lib\*
goto :eof

@REM Build classpath with individual JAR entries
:java5
set CLASSPATH=%SYM_HOME%\patches
for %%i in ("%SYM_HOME%\lib\*.jar") do call "%SYM_HOME%\bin\cpappend.bat" %%i
for %%i in ("%SYM_HOME%\web\WEB-INF\lib\*.jar") do call "%SYM_HOME%\bin\cpappend.bat" %%i
