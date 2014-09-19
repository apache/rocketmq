#!/bin/sh
while [ "1" == "1" ]
do
        dirty=`cat /proc/vmstat |grep nr_dirty|awk '{print $2}'`
        total=`cat /proc/vmstat |grep nr_file_pages|awk '{print $2}'`
        ratio=`echo "scale=4; $dirty/$total * 100" | bc`
        echo "$dirty		$total		${ratio}%"
        sleep 1
done