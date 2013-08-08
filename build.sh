#!/bin/sh
PROG_NAME=rocketmq
SHELL_PATH=$(cd "$(dirname "$0")"; pwd)
#NS="10.232.26.122 10.232.25.81"
BROKER="10.232.25.82 10.232.25.83"
NS="10.232.26.122"
#BROKER="10.232.25.82"
NS_PORT=9876
TGZ_NAME=alibaba-rocketmq-3.0.0-SNAPSHOT.tar.gz

usage() {
    echo "Usage: ${PROG_NAME} {package|dispatch|start|stop|ns|broker|cleanlog|cleanstore|clean|show}"
    exit 1;
}

package(){
	echo "############################# package #################################"
	sh develop.sh
}

dispatch(){
	echo "############################# dispatch #################################"
	pgmscp -b ${NS} ${BROKER} target/${TGZ_NAME} /home/manhong.yqd
	pgm -b ${NS} ${BROKER} "tar zxvf ${TGZ_NAME}"
	pgm -b ${NS} ${BROKER} 'rm -rf devenv'
	pgm -b ${NS} ${BROKER} 'mv alibaba-rocketmq devenv'
	pgm -b ${NS} ${BROKER} 'chmod +x devenv/bin/*'
	pgm -b ${NS} ${BROKER} 'sed -i s/#JAVA_OPT_6=/JAVA_OPT_6=/g devenv/bin/runbroker.sh'
	pgm -b ${NS} ${BROKER} 'sed -i s/#JAVA_OPT_6=/JAVA_OPT_6=/g devenv/bin/runserver.sh'
}

startup_ns(){
	echo "############################# startup nameserver #################################"
	pgm -b ${NS} 'killall -9 java'
	sleep 1
	pgm -b ${NS} 'rm -rf rocketmqlogs'
	pgm -b ${NS} './devenv/bin/mqnamesrv' &
	sleep 1
	pgm -b ${NS} 'ps -ef|grep java'
	sleep 1
	pgm -b ${NS} "netstat -an|grep ${NS_PORT}"
}

startup_broker(){
	echo "############################# startup broker #################################"
	pgm -b ${BROKER} 'killall -9 java'
	sleep 1
	pgm -b ${BROKER} 'rm -rf rocketmqlogs'
	tmp=`echo ${NS} | sed "s/ /:${NS_PORT};/g"`":${NS_PORT}"
	echo "nameserver:${tmp}"
        pgm -b ${BROKER} "./devenv/bin/mqbroker -n '${tmp}'" &
	sleep 1
        pgm -b ${BROKER} 'ps -ef|grep java'
	sleep 1
        pgm -b ${BROKER} "netstat -an|grep '${NS_PORT}'"
}

start(){
	startup_ns
	startup_broker
}

stop(){
	pgm -b ${NS} ${BROKER} 'killall -9 java'
}

cleanlog(){
	echo "############################# startup log #################################"
	pgm -b ${NS} ${BROKER} 'rm -rf rocketmqlogs'
}

cleanstore(){
	echo "############################# clean store #################################"
        pgm -b ${NS} ${BROKER} 'rm -rf store'
}

show(){
	pgm -b ${NS} ${BROKER} 'ps -ef|grep java'
        pgm -b ${NS} ${BROKER} "netstat -an|grep '${NS_PORT}'"
}

case "$1" in
	package)
		package
	;;
	dispatch)
		dispatch
	;;
	start)
        	start
	;;
	stop)
        	stop
    	;;
	ns)
        	startup_ns
	;;
	broker)
        	startup_broker
	;;
	cleanlog)
		cleanlog
	;;
	cleanstore)
		cleanstore
	;;
	clean)
		cleanlog
		cleanstore
	;;
	show)
		show
	;;
	*)
        	usage
    ;;
esac
