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

if not exist "%JAVA_HOME%\bin\jps.exe" echo Please set the JAVA_HOME variable in your environment, We need java(x64)! & EXIT /B 1

setlocal

set "PATH=%JAVA_HOME%\bin;%PATH%"

if /I "%1" == "broker" (
    echo killing broker
    for /f "tokens=1" %%i in ('jps -m ^| find "BrokerStartup"') do ( taskkill /F /PID %%i )
    echo Done!
) else if /I "%1" == "namesrv" (
    echo killing name server

    for /f "tokens=1" %%i in ('jps -m ^| find "NamesrvStartup"') do ( taskkill /F /PID %%i )

    echo Done!
) else if /I "%1" == "controller" (
    echo killing controller server

    for /f "tokens=1" %%i in ('jps -m ^| find "ControllerStartup"') do ( taskkill /F /PID %%i )

    echo Done!
) else (
    echo Unknown role to kill, please specify broker or namesrv or controller
)