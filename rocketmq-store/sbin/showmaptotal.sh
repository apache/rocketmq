#!/bin/sh
while [ "1" == "1" ]
do
	ps ax | grep -i 'com.taobao.metaq' |grep java | grep -v grep | awk '{print $1}' | xargs pmap |grep metastore |wc -l
	sleep 1
done
