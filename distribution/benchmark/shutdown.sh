#!/bin/sh

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

case $1 in
    producer)

    pid=`ps ax | grep -i 'org.apache.rocketmq.example.benchmark.Producer' |grep java | grep -v grep | awk '{print $1}'`
    if [ -z "$pid" ] ; then
            echo "No benchmark producer running."
            exit -1;
    fi

    echo "The benchmark producer(${pid}) is running..."

    kill ${pid}

    echo "Send shutdown request to benchmark producer(${pid}) OK"
    ;;
    consumer)

    pid=`ps ax | grep -i 'org.apache.rocketmq.example.benchmark.Consumer' |grep java | grep -v grep | awk '{print $1}'`
    if [ -z "$pid" ] ; then
            echo "No benchmark consumer running."
            exit -1;
    fi

    echo "The benchmark consumer(${pid}) is running..."

    kill ${pid}

    echo "Send shutdown request to benchmark consumer(${pid}) OK"
    ;;
    tproducer)

    pid=`ps ax | grep -i 'org.apache.rocketmq.example.benchmark.TransactionProducer' |grep java | grep -v grep | awk '{print $1}'`
    if [ -z "$pid" ] ; then
            echo "No benchmark transaction producer running."
            exit -1;
    fi

    echo "The benchmark transaction producer(${pid}) is running..."

    kill ${pid}

    echo "Send shutdown request to benchmark transaction producer(${pid}) OK"
    ;;
    bproducer)

    pid=`ps ax | grep -i 'org.apache.rocketmq.example.benchmark.BatchProducer' |grep java | grep -v grep | awk '{print $1}'`
    if [ -z "$pid" ] ; then
            echo "No benchmark batch producer running."
            exit -1;
    fi

    echo "The benchmark batch producer(${pid}) is running..."

    kill ${pid}

    echo "Send shutdown request to benchmark batch producer(${pid}) OK"
    ;;
    *)
    echo "Usage: shutdown producer | consumer | tproducer | bproducer"
esac
