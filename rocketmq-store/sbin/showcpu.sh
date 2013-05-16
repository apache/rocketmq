#!/bin/sh
while [ "1" == "1" ]
do
	ps -eo "%p %C %c" |grep java
	sleep 1
done