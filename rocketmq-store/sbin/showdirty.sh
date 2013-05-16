#!/bin/sh
while [ "1" == "1" ]
do
        NOW=`date +%H%M%S`
        output=dirty.`date +%Y%m%d`
        DIRTY=`cat /proc/vmstat |grep nr_dirty`
        echo $NOW $DIRTY >> $output
        sleep 1
done
