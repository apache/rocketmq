#!/bin/sh

#
# $Id: runbroker.sh 1831 2013-05-16 01:39:51Z shijia.wxr $
#

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname opts"
  exit 1
fi

BASE_DIR=$(dirname $0)/..
CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

JAVA_OPT_1="-server -Xms4g -Xmx4g -Xmn2g -XX:PermSize=128m -XX:MaxPermSize=320m"
JAVA_OPT_2="-XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC"
JAVA_OPT_3="-verbose:gc -Xloggc:${HOME}/rocketmq_gc.log -XX:+PrintGCDetails"
JAVA_OPT_4="-XX:-OmitStackTraceInFastThrow"
JAVA_OPT_5="-Djava.ext.dirs=${BASE_DIR}/lib"
#JAVA_OPT_6="-Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT_7="-cp ${CLASSPATH}"

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

JAVA="$JAVA_HOME/bin/java"

JAVA_OPTS="${JAVA_OPT_1} ${JAVA_OPT_2} ${JAVA_OPT_3} ${JAVA_OPT_4} ${JAVA_OPT_5} ${JAVA_OPT_6} ${JAVA_OPT_7}"

numactl --interleave=all pwd > /dev/null 2>&1
if [ $? -eq 0 ]
then
    numactl --interleave=all $JAVA $JAVA_OPTS $@
else
    $JAVA $JAVA_OPTS $@
fi
