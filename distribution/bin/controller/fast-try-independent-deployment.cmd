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

set ROCKETMQ_HOME=%cd%
if not exist "%ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n0.conf" echo Make sure the %ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n0.conf exists & EXIT /B 1
if not exist "%ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n1.conf" echo Make sure the %ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n1.conf exists & EXIT /B 1
if not exist "%ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n2.conf" echo Make sure the %ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n2.conf exists & EXIT /B 1

set "JAVA_OPT_EXT= -server -Xms512m -Xmx512m"
start call "%ROCKETMQ_HOME%\bin\runserver.cmd" org.apache.rocketmq.controller.ControllerStartup -c %ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n0.conf
IF %ERRORLEVEL% EQU 0 (
   ECHO "Controller start OK"
)
timeout /T 3 /NOBREAK
start call "%ROCKETMQ_HOME%\bin\runserver.cmd" org.apache.rocketmq.controller.ControllerStartup -c %ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n1.conf
IF %ERRORLEVEL% EQU 0 (
   ECHO "Controller start OK"
)
timeout /T 3 /NOBREAK
start call "%ROCKETMQ_HOME%\bin\runserver.cmd" org.apache.rocketmq.controller.ControllerStartup -c %ROCKETMQ_HOME%\conf\controller\cluster-3n-independent\controller-n2.conf
IF %ERRORLEVEL% EQU 0 (
   ECHO "Controller start OK"
)
