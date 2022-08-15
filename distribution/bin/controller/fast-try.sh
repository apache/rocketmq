#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Revise the base dir
CURRENT_DIR="$(cd "$(dirname "$0")"; pwd)"
RMQ_DIR=$CURRENT_DIR/../..
cd $RMQ_DIR

startNameserver() {
    export JAVA_OPT_EXT=" -Xms512m -Xmx512m  "
    conf_name=$1
    nohup bin/mqnamesrv -c $conf_name &
}

startBroker() {
    export JAVA_OPT_EXT=" -Xms1g -Xmx1g  "
    conf_name=$1
    nohup bin/mqbroker -c $conf_name &
}

stopNameserver() {
    PIDS=$(ps -ef|grep java|grep NamesrvStartup|grep -v grep|awk '{print $2}')
    if [ ! -z "$PIDS" ]; then
        kill -s TERM $PIDS
    fi
}

stopBroker() {
    conf_name=$1
    PIDS=$(ps -ef|grep java|grep BrokerStartup|grep $conf_name|grep -v grep|awk '{print $2}')
    i=1
    while [ ! -z "$PIDS" -a $i -lt 5 ]
    do
        echo "Waiting to kill ..."
        kill -s TERM $PIDS
        i=`expr $i + 1`
        sleep 2
        PIDS=$(ps -ef|grep java|grep BrokerStartup|grep $conf_name|grep -v grep|awk '{print $2}')
    done
    PIDS=$(ps -ef|grep java|grep BrokerStartup|grep $conf_name|grep -v grep|awk '{print $2}')
    if [ ! -z "$PIDS" ]; then
        kill -9 $PIDS
    fi
}

stopAll() {
    ps -ef|grep java|grep BrokerStartup|grep -v grep|awk '{print $2}'|xargs kill
    stopNameserver
    stopBroker ./conf/controller/quick-start/broker-n0.conf
    stopBroker ./conf/controller/quick-start/broker-n1.conf
}

startAll() {
    startNameserver ./conf/controller/quick-start/namesrv.conf
    startBroker ./conf/controller/quick-start/broker-n0.conf
    startBroker ./conf/controller/quick-start/broker-n1.conf
}

checkConf() {
    if [ ! -f ./conf/controller/quick-start/broker-n0.conf -o ! -f ./conf/controller/quick-start/broker-n1.conf -o ! -f ./conf/controller/quick-start/namesrv.conf ]; then
        echo "Make sure the ./conf/controller/quick-start/broker-n0.conf, ./conf/controller/quick-start/broker-n1.conf, ./conf/controller/quick-start/namesrv.conf exists"
        exit 1
    fi
}



## Main
if [ $# -lt 1 ]; then
    echo "Usage: sh $0 start|stop"
    exit 1
fi
action=$1
checkConf
case $action in
    "start")
        startAll
        exit
        ;;
    "stop")
        stopAll
        ;;
    *)
        echo "Usage: sh $0 start|stop"
        ;;
esac

