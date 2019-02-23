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
package org.apache.rocketmq.snode.offset;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.exception.SnodeException;

public class ConsumerOffsetManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    private ConcurrentMap<String/* Enode@Topic@Group */, ConcurrentMap<Integer, CacheOffset>> offsetTable =
        new ConcurrentHashMap<>(512);

    private SnodeController snodeController;

    public ConsumerOffsetManager(SnodeController brokerController) {
        this.snodeController = brokerController;
    }

    private String buildKey(final String enodeName, final String topic, final String consumerGroup) {
        if (enodeName == null || topic == null || consumerGroup == null) {
            log.warn("Build key parameter error enodeName: {}, topic: {} consumerGroup:{}",
                enodeName, topic, consumerGroup);
            throw new SnodeException(ResponseCode.PARAMETER_ERROR, "Build key parameter error!");
        }
        StringBuilder sb = new StringBuilder(50);
        sb.append(enodeName).append(TOPIC_GROUP_SEPARATOR).append(topic).append(TOPIC_GROUP_SEPARATOR).append(consumerGroup);
        return sb.toString();
    }

    public void cacheOffset(final String enodeName, final String clientHost, final String group, final String topic,
        final int queueId,
        final long offset) {
        // EnodeName@Topic@group
        String key = buildKey(enodeName, topic, group);
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        ConcurrentMap<Integer, CacheOffset> map = this.offsetTable.get(key);
        CacheOffset cacheOffset = new CacheOffset(key, offset, System.currentTimeMillis());
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            ConcurrentMap<Integer, CacheOffset> prev = this.offsetTable.putIfAbsent(key, map);
            map = prev != null ? prev : map;
            map.put(queueId, cacheOffset);
        } else {
            CacheOffset storeOffset = map.put(queueId, cacheOffset);
            if (storeOffset != null && offset < storeOffset.getOffset()) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost: {}, key: {}, queueId: {}, requestOffset: {}, storeOffset: {}",
                    clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    public long queryCacheOffset(final String enodeName, final String group, final String topic, final int queueId) {
        String key = buildKey(enodeName, topic, group);
        ConcurrentMap<Integer, CacheOffset> map = this.offsetTable.get(key);
        if (map == null) {
            map = new ConcurrentHashMap<>();
            map = this.offsetTable.putIfAbsent(key, map);
        }
        CacheOffset cacheOffset = map.get(queueId);
        try {
            if (cacheOffset != null) {
                if (!this.snodeController.getSnodeConfig().isEmbeddedModeEnable() && System.currentTimeMillis() - cacheOffset.getUpdateTimestamp() > snodeController.getSnodeConfig().getLoadOffsetInterval()) {
                    long offset = this.snodeController.getEnodeService().queryOffset(enodeName, group, topic, queueId);
                    cacheOffset.setOffset(offset);
                    cacheOffset.setUpdateTimestamp(System.currentTimeMillis());
                } else {
                    long offset = this.snodeController.getEnodeService().queryOffset(enodeName, group, topic, queueId);
                    cacheOffset.setOffset(offset);
                }
            } else {
                long offset = this.snodeController.getEnodeService().queryOffset(enodeName, group, topic, queueId);
                cacheOffset = new CacheOffset(key, offset, System.currentTimeMillis());
                map.put(queueId, cacheOffset);
            }
        } catch (Exception ex) {
            log.warn("Load offset error, enodeName: {}, group:{},topic:{} queueId:{}", enodeName, group, topic, queueId);
        }
        return cacheOffset.getOffset();
    }

    public void commitOffset(final RemotingChannel remotingChannel, final String enodeName, final String clientHost,
        final String group, final String topic,
        final int queueId,
        final long offset) {
        cacheOffset(enodeName, clientHost, group, topic, queueId, offset);
        this.snodeController.getEnodeService().persistOffset(remotingChannel, enodeName, group, topic, queueId, offset);
    }

    public class CacheOffset {
        private String key;
        private long offset;
        private long updateTimestamp;

        public CacheOffset(final String key, final long offset, final long updateTimestamp) {
            this.key = key;
            this.offset = offset;
            this.updateTimestamp = updateTimestamp;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public long getUpdateTimestamp() {
            return updateTimestamp;
        }

        public void setUpdateTimestamp(long updateTimestamp) {
            this.updateTimestamp = updateTimestamp;
        }
    }
}
