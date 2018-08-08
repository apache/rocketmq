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

#===========================================================================================
# Java Environment Setting
#===========================================================================================
error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/usr/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

#===========================================================================================
# JVM Configuration
#===========================================================================================
# Get the max heap used by a jvm, which used all the ram available to the container.
MAX_POSSIBLE_HEAP_STR=$(java -XX:+UnlockExperimentalVMOptions -XX:MaxRAMFraction=1 -XshowSettings:vm -version |& awk '/Max\. Heap Size \(Estimated\): [0-9KMG]+/{ print $5}')
MAX_POSSIBLE_HEAP=$MAX_POSSIBLE_HEAP_STR
CAL_UNIT=${MAX_POSSIBLE_HEAP_STR: -1}
if [ "$CAL_UNIT" == "G" -o "$CAL_UNIT" == "g" ]; then
  MAX_POSSIBLE_HEAP=$(echo ${MAX_POSSIBLE_HEAP_STR:0:${#MAX_POSSIBLE_HEAP_STR}-1} `expr 1 \* 1024 \* 1024 \* 1024` | awk '{printf "%d",$1*$2}')
elif [ "$CAL_UNIT" == "M" -o "$CAL_UNIT" == "m" ]; then
  MAX_POSSIBLE_HEAP=$(echo ${MAX_POSSIBLE_HEAP_STR:0:${#MAX_POSSIBLE_HEAP_STR}-1} `expr 1 \* 1024 \* 1024` | awk '{printf "%d",$1*$2}')
elif [ "$CAL_UNIT" == "K" -o "$CAL_UNIT" == "k" ]; then
  MAX_POSSIBLE_HEAP=$(echo ${MAX_POSSIBLE_HEAP_STR:0:${#MAX_POSSIBLE_HEAP_STR}-1} `expr 1 \* 1024` | awk '{printf "%d",$1*$2}')
fi

# Dynamically calculate parameters, for reference.
Xms=$[MAX_POSSIBLE_HEAP/4]
Xmx=$[MAX_POSSIBLE_HEAP/4]
Xmn=$[MAX_POSSIBLE_HEAP/8]
MaxDirectMemorySize=$[MAX_POSSIBLE_HEAP/4]
# Set for `JAVA_OPT`.
JAVA_OPT="${JAVA_OPT} -server -Xms${Xms} -Xmx${Xmx} -Xmn${Xmn}"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0 -XX:SurvivorRatio=8"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:/dev/shm/mq_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintAdaptiveSizePolicy"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -XX:+AlwaysPreTouch"
JAVA_OPT="${JAVA_OPT} -XX:MaxDirectMemorySize=${MaxDirectMemorySize}"
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages -XX:-UseBiasedLocking"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${JAVA_HOME}/jre/lib/ext:${BASE_DIR}/lib"
#JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} ${JAVA_OPT_EXT}"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"

numactl --interleave=all pwd > /dev/null 2>&1
if [ $? -eq 0 ]
then
	if [ -z "$RMQ_NUMA_NODE" ] ; then
		numactl --interleave=all $JAVA ${JAVA_OPT} $@
	else
		numactl --cpunodebind=$RMQ_NUMA_NODE --membind=$RMQ_NUMA_NODE $JAVA ${JAVA_OPT} $@
	fi
else
	$JAVA ${JAVA_OPT} $@
fi
