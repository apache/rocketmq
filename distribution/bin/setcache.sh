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

#
# GB
#
function changeFreeCache()
{
    EXTRA=$1
    MIN=$2
    sysctl -w vm.extra_free_kbytes=${EXTRA}000000
    sysctl -w vm.min_free_kbytes=${MIN}000000
    sysctl -w vm.swappiness=0
}


if [ $# -ne 2 ]
then
    echo "Usage: $0 extra_free_kbytes(GB) min_free_kbytes(GB)"
    echo "Example: $0 3 1"
    exit
fi

changeFreeCache $1 $2
