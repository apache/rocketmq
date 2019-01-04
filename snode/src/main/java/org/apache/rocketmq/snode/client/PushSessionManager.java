/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.snode.client;

import java.util.concurrent.ConcurrentHashMap;

public class PushSessionManager {
    private final ConcurrentHashMap<String/*Topic*/, ConcurrentHashMap<Integer/*QueueId*/, ClientChannelInfo>> topicConsumerTable = new ConcurrentHashMap<>(2048);

    public void updateTopicConsumerTable(String topic, int queueId, ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Integer, ClientChannelInfo> clientChannelInfoMap = this.topicConsumerTable.get(topic);

        if (clientChannelInfoMap == null) {
            clientChannelInfoMap = new ConcurrentHashMap<>();
            ConcurrentHashMap prev = this.topicConsumerTable.putIfAbsent(topic, clientChannelInfoMap);
            if (prev != null) {
                clientChannelInfoMap = prev;
            }
        }
        clientChannelInfoMap.put(queueId, clientChannelInfo);
    }

    public ClientChannelInfo getClientInfoTable(String topic, long queueId) {
        ConcurrentHashMap<Integer, ClientChannelInfo> clientChannelInfoMap = this.topicConsumerTable.get(topic);
        if (clientChannelInfoMap != null) {
            return clientChannelInfoMap.get(queueId);
        }
        return null;
    }

    public void removeConsumerTopicTable(String topic, Integer queueId, ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Integer, ClientChannelInfo> clientChannelInfoMap = this.topicConsumerTable.get(topic);
        if (clientChannelInfoMap != null) {
            ClientChannelInfo old = clientChannelInfoMap.get(queueId);
            //TODO Thread safe issue: wait for the next heartbeat
            if (old == clientChannelInfo) {
                clientChannelInfoMap.remove(queueId, clientChannelInfo);
            }
        }

    }
}
