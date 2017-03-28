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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export PATH=$PATH:/sbin

while true; do
    nr_free_pages=`fgrep -A 10 Normal /proc/zoneinfo  |grep nr_free_pages  |awk -F ' ' '{print $2}'`
    high=`fgrep -A 10 Normal /proc/zoneinfo  |grep high  |awk -F ' ' '{print $2}'`

    NOW_DATE=`date +%D`
    NOW_TIME=`date +%T`

    if [ ${nr_free_pages} -le ${high} ]; then
        sysctl -w vm.drop_caches=3
        nr_free_pages_new=`fgrep -A 10 Normal /proc/zoneinfo  |grep nr_free_pages  |awk -F ' ' '{print $2}'`

        printf "%s %s [CLEAN] nr_free_pages < high, clean cache. nr_free_pages=%s ====> nr_free_pages=%s\n" "${NOW_DATE}" "${NOW_TIME}" ${nr_free_pages} ${nr_free_pages_new}

        sysctl -w vm.drop_caches=1
        echo
        echo
        echo
    else
        printf "%s %s [NOTHING] nr_free_pages=%s high=%s\n" "${NOW_DATE}" "${NOW_TIME}" ${nr_free_pages} ${high}
    fi

    sleep 1
done
