#!/bin/sh

#
# $Id: tools.sh 587 2012-11-20 03:26:56Z shijia.wxr $
#

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname opts"
  exit 1
fi

BASE_DIR=$(dirname $0)/..
CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

JAVA_OPT_1="-Djava.ext.dirs=${BASE_DIR}/lib -cp ${CLASSPATH}"


if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

JAVA="$JAVA_HOME/bin/java"

JAVA_OPTS="${JAVA_OPT_1}"

$JAVA $JAVA_OPTS $@
