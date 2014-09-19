#!/bin/sh

#
# $Id: runserver.sh 1831 2013-05-16 01:39:51Z shijia.wxr $
#

function_error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

if [ $# -lt 1 ];
then
  function_error_exit "USAGE: $0 classname opts"
fi

BASE_DIR=$(dirname $0)/..
CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

JAVA_OPT="${JAVA_OPT} -server -Xms4g -Xmx4g -Xmn2g -XX:PermSize=128m -XX:MaxPermSize=320m"
JAVA_OPT="${JAVA_OPT} -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:${HOME}/rmq_srv_gc.log -XX:+PrintGCDetails"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib"
#JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

JAVA="$JAVA_HOME/bin/java"

[ ! -e "$JAVA" ] && function_error_exit "Please set the JAVA_HOME variable in your environment, We need java!"

$JAVA ${JAVA_OPT} $@
