@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.


if not exist "%JAVA_HOME%\bin\java.exe" echo Please set the JAVA_HOME variable in your environment, We need java(x64)! & EXIT /B 1
set "JAVA=%JAVA_HOME%\bin\java.exe"

setlocal

set BASE_DIR=%~dp0
set BASE_DIR=%BASE_DIR:~0,-1%
for %%d in (%BASE_DIR%) do set BASE_DIR=%%~dpd

set CLASSPATH=.;%BASE_DIR%conf;%BASE_DIR%lib\*;%CLASSPATH%

REM Example of JAVA_MAJOR_VERSION value: '1', '9', '10', '11', ...
REM '1' means releases before Java 9

for /f "tokens=2 delims=" %%v in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    for /f "tokens=1 delims=." %%m in ("%%v") do set "JAVA_MAJOR_VERSION=%%m"
)

if "%JAVA_MAJOR_VERSION%"=="" (
    set "JAVA_MAJOR_VERSION=0"
)

if %JAVA_MAJOR_VERSION% lss 17 (
   set "JAVA_OPT=%JAVA_OPT% -server -Xms2g -Xmx2g -Xmn1g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m"
   set "JAVA_OPT=%JAVA_OPT% -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:-UseParNewGC"
   set "JAVA_OPT=%JAVA_OPT% -verbose:gc -Xloggc:"%USERPROFILE%\rmq_srv_gc.log" -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
   set "JAVA_OPT=%JAVA_OPT% -XX:-OmitStackTraceInFastThrow"
   set "JAVA_OPT=%JAVA_OPT% -XX:-UseLargePages"
   set "JAVA_OPT=%JAVA_OPT% %JAVA_OPT_EXT% -cp "%CLASSPATH%""
) else (
   set "JAVA_OPT=%JAVA_OPT% -server -Xms2g -Xmx2g -Xmn1g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=320m"
   rem set "JAVA_OPT=%JAVA_OPT% -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:-UseParNewGC"
   rem set "JAVA_OPT=%JAVA_OPT% -verbose:gc -Xloggc:"%USERPROFILE%\rmq_srv_gc.log" -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
   set "JAVA_OPT=%JAVA_OPT% -XX:-OmitStackTraceInFastThrow"
   set "JAVA_OPT=%JAVA_OPT% -XX:-UseLargePages"
   set "JAVA_OPT=%JAVA_OPT% %JAVA_OPT_EXT% -cp "%CLASSPATH%""
)

"%JAVA%" %JAVA_OPT% %*