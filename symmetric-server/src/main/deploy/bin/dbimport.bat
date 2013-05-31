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

set PRGDIR=%~dp0
set HOMEDIR=%PRGDIR%..
set CONFDIR=%HOMEDIR%\conf
set CLASSPATH=%HOMEDIR%\patches

for %%i in ("%HOMEDIR%\lib\*.jar") do call "%PRGDIR%cpappend.bat" %%i

for %%i in ("%HOMEDIR%\web\WEB-INF\lib\*.jar") do call "%PRGDIR%cpappend.bat" %%i

rem echo CLASSPATH=%CLASSPATH%

java -Duser.language=en -Djava.io.tmpdir=../tmp -Dsym.keystore.file="%HOMEDIR%\security\keystore" -Djavax.net.ssl.trustStore="%HOMEDIR%\security\cacerts" -Dlog4j.configuration="file:%CONFDIR%\log4j.xml" -Dsun.net.client.defaultReadTimeout=1800000 -Dsun.net.client.defaultConnectTimeout=1800000 org.jumpmind.symmetric.DbImportCommand %1 %2 %3 %4 %5 %6 %7 %8 %9