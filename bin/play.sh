#!/bin/sh

#
# Name Server
#
nohup sh mqnamesrv > ns.log 2>&1 &

#
# Service Addr
#
ADDR=`hostname -i`:9876

#
# Broker
#
nohup sh mqbroker -n ${ADDR} > bk.log 2>&1 &

echo "Start Name Server and Broker Successfully, ${ADDR}"
