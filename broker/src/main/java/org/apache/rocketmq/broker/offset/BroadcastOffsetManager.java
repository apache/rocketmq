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
package org.apache.rocketmq.broker.offset;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ServiceThread;

/**
 * manage the offset of broadcast.
 * now, use this to support switch remoting client between proxy and broker
 */
public class BroadcastOffsetManager extends ServiceThread {
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private final BrokerController brokerController;
    private final BrokerConfig brokerConfig;

    /**
     * k: topic@groupId
     * v: the pull offset of all client of all queue
     */
    protected final ConcurrentHashMap<String /* topic@groupId */, BroadcastOffsetData> offsetStoreMap =
        new ConcurrentHashMap<>();

    public BroadcastOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();
    }

    public void updateOffset(String topic, String group, int queueId, long offset, String clientId, boolean fromProxy) {
        BroadcastOffsetData broadcastOffsetData = offsetStoreMap.computeIfAbsent(
            buildKey(topic, group), key -> new BroadcastOffsetData(topic, group));

        broadcastOffsetData.clientOffsetStore.compute(clientId, (clientIdKey, broadcastTimedOffsetStore) -> {
            if (broadcastTimedOffsetStore == null) {
                broadcastTimedOffsetStore = new BroadcastTimedOffsetStore(fromProxy);
            }

            broadcastTimedOffsetStore.timestamp = System.currentTimeMillis();
            broadcastTimedOffsetStore.fromProxy = fromProxy;
            broadcastTimedOffsetStore.offsetStore.updateOffset(queueId, offset, true);
            return broadcastTimedOffsetStore;
        });
    }

    /**
     * the time need init offset
     * 1. client connect to proxy -> client connect to broker
     * 2. client connect to broker -> client connect to proxy
     * 3. client connect to proxy at the first time
     *
     * @return -1 means no init offset, use the queueOffset in pullRequestHeader
     */
    public Long queryInitOffset(String topic, String groupId, int queueId, String clientId, long requestOffset,
        boolean fromProxy) {

        BroadcastOffsetData broadcastOffsetData = offsetStoreMap.get(buildKey(topic, groupId));
        if (broadcastOffsetData == null) {
            if (fromProxy && requestOffset < 0) {
                return getOffset(null, topic, groupId, queueId);
            } else {
                return -1L;
            }
        }

        final AtomicLong offset = new AtomicLong(-1L);
        broadcastOffsetData.clientOffsetStore.compute(clientId, (clientIdK, offsetStore) -> {
            if (offsetStore == null) {
                offsetStore = new BroadcastTimedOffsetStore(fromProxy);
            }

            if (offsetStore.fromProxy && requestOffset < 0) {
                // when from proxy and requestOffset is -1
                // means proxy need a init offset to pull message
                offset.set(getOffset(offsetStore, topic, groupId, queueId));
                return offsetStore;
            }

            if (offsetStore.fromProxy == fromProxy) {
                return offsetStore;
            }

            offset.set(getOffset(offsetStore, topic, groupId, queueId));
            return offsetStore;
        });
        return offset.get();
    }

    private long getOffset(BroadcastTimedOffsetStore offsetStore, String topic, String groupId, int queueId) {
        long storeOffset = -1;
        if (offsetStore != null) {
            storeOffset = offsetStore.offsetStore.readOffset(queueId);
        }
        if (storeOffset < 0) {
            storeOffset =
                brokerController.getConsumerOffsetManager().queryOffset(broadcastGroupId(groupId), topic, queueId);
        }
        if (storeOffset < 0) {
            if (this.brokerController.getMessageStore().checkInMemByConsumeOffset(topic, queueId, 0, 1)) {
                storeOffset = 0;
            } else {
                storeOffset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId, true);
            }
        }
        return storeOffset;
    }

    /**
     * 1. scan expire offset
     * 2. calculate the min offset of all client of one topic@group,
     * and then commit consumer offset by group@broadcast
     */
    protected void scanOffsetData() {
        for (String k : offsetStoreMap.keySet()) {
            BroadcastOffsetData broadcastOffsetData = offsetStoreMap.get(k);
            if (broadcastOffsetData == null) {
                continue;
            }

            Map<Integer, Long> queueMinOffset = new HashMap<>();

            for (String clientId : broadcastOffsetData.clientOffsetStore.keySet()) {
                broadcastOffsetData.clientOffsetStore
                    .computeIfPresent(clientId, (clientIdKey, broadcastTimedOffsetStore) -> {
                        long interval = System.currentTimeMillis() - broadcastTimedOffsetStore.timestamp;
                        boolean clientIsOnline = brokerController.getConsumerManager().findChannel(broadcastOffsetData.group, clientId) != null;
                        if (clientIsOnline || interval < Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireSecond()).toMillis()) {
                            Set<Integer> queueSet = broadcastTimedOffsetStore.offsetStore.queueList();
                            for (Integer queue : queueSet) {
                                long offset = broadcastTimedOffsetStore.offsetStore.readOffset(queue);
                                offset = Math.min(queueMinOffset.getOrDefault(queue, offset), offset);
                                queueMinOffset.put(queue, offset);
                            }
                        }
                        if (clientIsOnline && interval >= Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireMaxSecond()).toMillis()) {
                            return null;
                        }
                        if (!clientIsOnline && interval >= Duration.ofSeconds(brokerConfig.getBroadcastOffsetExpireSecond()).toMillis()) {
                            return null;
                        }
                        return broadcastTimedOffsetStore;
                    });
            }

            offsetStoreMap.computeIfPresent(k, (key, broadcastOffsetDataVal) -> {
                if (broadcastOffsetDataVal.clientOffsetStore.isEmpty()) {
                    return null;
                }
                return broadcastOffsetDataVal;
            });

            queueMinOffset.forEach((queueId, offset) ->
                this.brokerController.getConsumerOffsetManager().commitOffset("BroadcastOffset",
                broadcastGroupId(broadcastOffsetData.group), broadcastOffsetData.topic, queueId, offset));
        }
    }

    private String buildKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    /**
     * @param group group of users
     * @return the groupId used to commit offset
     */
    private static String broadcastGroupId(String group) {
        return group + TOPIC_GROUP_SEPARATOR + "broadcast";
    }

    @Override
    public String getServiceName() {
        return "BroadcastOffsetManager";
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            this.waitForRunning(Duration.ofSeconds(5).toMillis());
        }
    }

    @Override
    protected void onWaitEnd() {
        this.scanOffsetData();
    }

    public static class BroadcastOffsetData {
        private final String topic;
        private final String group;
        private final ConcurrentHashMap<String /* clientId */, BroadcastTimedOffsetStore> clientOffsetStore;

        public BroadcastOffsetData(String topic, String group) {
            this.topic = topic;
            this.group = group;
            this.clientOffsetStore = new ConcurrentHashMap<>();
        }
    }

    public static class BroadcastTimedOffsetStore {

        /**
         * the timeStamp of last update occurred
         */
        private volatile long timestamp;

        /**
         * mark the offset of this client is updated by proxy or not
         */
        private volatile boolean fromProxy;

        /**
         * the pulled offset of each queue
         */
        private final BroadcastOffsetStore offsetStore;

        public BroadcastTimedOffsetStore(boolean fromProxy) {
            this.timestamp = System.currentTimeMillis();
            this.fromProxy = fromProxy;
            this.offsetStore = new BroadcastOffsetStore();
        }
    }
}
