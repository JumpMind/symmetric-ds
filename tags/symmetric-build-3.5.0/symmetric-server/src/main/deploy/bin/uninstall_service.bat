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
setlocal

rem Copyright (c) 1999, 2009 Tanuki Software, Ltd.
rem http://www.tanukisoftware.com
rem All rights reserved.
rem
rem This software is the proprietary information of Tanuki Software.
rem You shall use it only in accordance with the terms of the
rem license agreement you entered into with Tanuki Software.
rem http://wrapper.tanukisoftware.org/doc/english/licenseOverview.html
rem
rem Java Service Wrapper general NT service uninstall script.
rem Optimized for use with version 3.3.6 of the Wrapper.
rem

if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0

rem Decide on the wrapper binary.
set _WRAPPER_BASE=sym_service
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
echo %_REALPATH%%_WRAPPER_BASE%.exe
pause
goto :eof

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF="%~f1"
if not %_WRAPPER_CONF%=="" goto startup
set _WRAPPER_CONF="%_REALPATH%..\conf\sym_service.conf"

rem
rem Uninstall the Wrapper as an NT service.
rem
:startup
"%_WRAPPER_EXE%" -r %_WRAPPER_CONF%
if not errorlevel 1 goto :eof
pause

