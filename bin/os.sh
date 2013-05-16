#!/bin/sh

sudo sysctl vm.overcommit_memory=1
sudo sysctl vm.min_free_kbytes=5000000
sudo sysctl vm.drop_caches=1
sudo sysctl vm.zone_reclaim_mode=0
sudo sysctl vm.max_map_count=655360
sudo sysctl vm.dirty_background_ratio=50
sudo sysctl vm.dirty_ratio=50
sudo sysctl vm.page-cluster=3
sudo sysctl vm.dirty_writeback_centisecs=360000
sudo sysctl vm.swappiness=10

NOFILE=`ulimit -n`
echo "nofile=${NOFILE}"

if [ -d /sys/block/sda ]; then
	SCHEDULER=`cat /sys/block/sda/queue/scheduler`
	READ_AHEAD_KB=`cat /sys/block/sda/queue/read_ahead_kb`
	echo "/sys/block/sda/queue/scheduler=${SCHEDULER}"
	echo "/sys/block/sda/queue/read_ahead_kb=${READ_AHEAD_KB}"
fi

if [ -d /sys/block/sdb ]; then
	SCHEDULER=`cat /sys/block/sdb/queue/scheduler`
	READ_AHEAD_KB=`cat /sys/block/sdb/queue/read_ahead_kb`
	echo "/sys/block/sdb/queue/scheduler=${SCHEDULER}"
	echo "/sys/block/sdb/queue/read_ahead_kb=${READ_AHEAD_KB}"
fi

if [ -d /sys/block/sdc ]; then
	SCHEDULER=`cat /sys/block/sdc/queue/scheduler`
	READ_AHEAD_KB=`cat /sys/block/sdc/queue/read_ahead_kb`
	echo "/sys/block/sdc/queue/scheduler=${SCHEDULER}"
	echo "/sys/block/sdc/queue/read_ahead_kb=${READ_AHEAD_KB}"
fi

if [ -d /sys/block/sdd ]; then
	SCHEDULER=`cat /sys/block/sdd/queue/scheduler`
	READ_AHEAD_KB=`cat /sys/block/sdd/queue/read_ahead_kb`
	echo "/sys/block/sdd/queue/scheduler=${SCHEDULER}"
	echo "/sys/block/sdd/queue/read_ahead_kb=${READ_AHEAD_KB}"
fi

echo
echo "TODO: Change 'nofile' value in /etc/security/limits.conf"
echo "TODO: Change io scheduler value to deadline"
