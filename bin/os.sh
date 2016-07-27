#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#
# Execute Only Once
#

sudo /sbin/sysctl -w vm.overcommit_memory=1
sudo /sbin/sysctl -w vm.min_free_kbytes=5000000
sudo /sbin/sysctl -w vm.extra_free_kbytes=5000000
sudo /sbin/sysctl -w vm.drop_caches=1
sudo /sbin/sysctl -w vm.zone_reclaim_mode=0
sudo /sbin/sysctl -w vm.max_map_count=655360
sudo /sbin/sysctl -w vm.dirty_background_ratio=50
sudo /sbin/sysctl -w vm.dirty_ratio=50
sudo /sbin/sysctl -w vm.page-cluster=3
sudo /sbin/sysctl -w vm.dirty_writeback_centisecs=360000
sudo /sbin/sysctl -w vm.swappiness=10
/sbin/sysctl -p

echo 'ulimit -n 655350' >> /etc/profile
echo 'admin hard nofile 655350' >> /etc/security/limits.conf

DISK=`df -k | sort -n -r -k 2 | awk -F/ 'NR==1 {gsub(/[0-9].*/,"",$3); print $3}'`
[ "$DISK" = 'cciss' ] && DISK='cciss!c0d0'
echo 'deadline' > /sys/block/$DISK/queue/scheduler


echo "---------------------------------------------------------------"
/sbin/sysctl vm.overcommit_memory
/sbin/sysctl vm.min_free_kbytes
/sbin/sysctl vm.extra_free_kbytes
/sbin/sysctl vm.drop_caches
/sbin/sysctl vm.zone_reclaim_mode
/sbin/sysctl vm.max_map_count
/sbin/sysctl vm.dirty_background_ratio
/sbin/sysctl vm.dirty_ratio
/sbin/sysctl vm.page-cluster
/sbin/sysctl vm.dirty_writeback_centisecs
/sbin/sysctl vm.swappiness

su - admin -c 'ulimit -n'
cat /sys/block/$DISK/queue/scheduler
