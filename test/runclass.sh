#!/bin/sh

#
# $Id: runclass.sh 857 2012-12-24 06:31:31Z shijia.wxr $
#

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname opts"
  exit 1
fi

BASE_DIR=$(dirname $0)/..
CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

JAVA_OPT_1="-server -Xms1g -Xmx1g -Xmn256m -XX:PermSize=128m -XX:MaxPermSize=320m"
JAVA_OPT_2="-XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC"
JAVA_OPT_3="-verbose:gc -Xloggc:${HOME}/rocketmq_client_gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
JAVA_OPT_4="-XX:-OmitStackTraceInFastThrow"
JAVA_OPT_5="-Djava.ext.dirs=${BASE_DIR}/lib"
JAVA_OPT_6="-cp ${CLASSPATH}"

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

JAVA="$JAVA_HOME/bin/java"

JAVA_OPTS="${JAVA_OPT_1} ${JAVA_OPT_2} ${JAVA_OPT_3} ${JAVA_OPT_4} ${JAVA_OPT_5} ${JAVA_OPT_6}"

$JAVA $JAVA_OPTS $@
