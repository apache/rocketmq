#!/bin/bash

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

if [ -z "$ROCKETMQ_HOME" ]; then
  ## resolve links - $0 may be a link to maven's home
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ]; do
    ls=$(ls -ld "$PRG")
    link=$(expr "$ls" : '.*-> \(.*\)$')
    if expr "$link" : '/.*' >/dev/null; then
      PRG="$link"
    else
      PRG="$(dirname "$PRG")/$link"
    fi
  done

  saveddir=$(pwd)

  ROCKETMQ_HOME=$(dirname "$PRG")/..

  # make it fully qualified
  ROCKETMQ_HOME=$(cd "$ROCKETMQ_HOME" && pwd)

  cd "$saveddir"
fi

export ROCKETMQ_HOME

namesrvAddr=
while [ -z "${namesrvAddr}" ]; do
  read -p "Enter name server address list:" namesrvAddr
done

clusterName=
while [ -z "${clusterName}" ]; do
  read -p "Choose a cluster to export:" clusterName
done

read -p "Enter file path to export [default /tmp/rocketmq/export]:" filePath
if [ -z "${filePath}" ]; then
  filePath="/tmp/rocketmq/config"
fi

if [[ -e ${filePath} ]]; then
  rm -rf ${filePath}
fi

sh ${ROCKETMQ_HOME}/bin/mqadmin exportMetrics -c ${clusterName} -n ${namesrvAddr} -f ${filePath}
sh ${ROCKETMQ_HOME}/bin/mqadmin exportConfigs -c ${clusterName} -n ${namesrvAddr} -f ${filePath}
sh ${ROCKETMQ_HOME}/bin/mqadmin exportMetadata -c ${clusterName} -n ${namesrvAddr} -f ${filePath}

cd ${filePath} || exit

configs=$(cat ./configs.json)
if [ -z "$configs" ]; then
  configs="{}"
fi
metadata=$(cat ./metadata.json)
if [ -z "$metadata" ]; then
  metadata="{}"
fi
metrics=$(cat ./metrics.json)
if [ -z "$metrics" ]; then
  metrics="{}"
fi

echo "{
    \"configs\": ${configs},
    \"metadata\": ${metadata},
    \"metrics\": ${metrics}
  }" >rocketmq-metadata-export.json

echo -e "[INFO] The RocketMQ metadata has been exported to the file:${filePath}/rocketmq-metadata-export.json"
