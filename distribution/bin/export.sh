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

checkCmd() {
  if ! command -v wget > /dev/null 2>&1; then
    echo -e '[ERROR] This system does not contains wget command, please install it!'
    exit 1
  fi
}

download() {
  if [[ -e /tmp/rocketmq/ ]]; then
    rm -rf /tmp/rocketmq/
  fi

  wget --no-check-certificate -P /tmp/rocketmq/ https://ons-migration.oss-cn-hangzhou.aliyuncs.com/rocketmq-for-export.tar.gz
  if [[ $? -ne 0 ]]; then
    echo -e "[ERROR] Download rocketmq error, please make sure this file exists"
    exit 1
  else
    echo -e "[INFO] Download rocketmq completed，file path: $(pwd)/rocketmq-for-export.tar.gz"
  fi

  tar -zxvf /tmp/rocketmq/rocketmq-for-export.tar.gz -C /tmp/rocketmq/ > /dev/null 2>&1

  echo -e "[INFO] Unzip rocketmq-for-export.tar.gz completed"
}

doExport() {

  cd /tmp/rocketmq/rocketmq-for-export || exit

  ROCKETMQ_HOME=$(pwd)
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


  sh ./bin/mqadmin exportMetrics -c ${clusterName} -n ${namesrvAddr} -f ${filePath}
  sh ./bin/mqadmin exportConfigs -c ${clusterName} -n ${namesrvAddr} -f ${filePath}
  sh ./bin/mqadmin exportMetadata -c ${clusterName} -n ${namesrvAddr} -f ${filePath}

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

  echo -e "[INFO] The RocketMQ metadata has been export to the file:${filePath}/rocketmq-metadata-export.json"
}

checkCmd

download

doExport
