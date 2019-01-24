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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.serialize.RemotingSerializable;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.exception.SnodeException;

public class ConsumerOffsetManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    private ConcurrentMap<String/* Enode@Topic@Group */, ConcurrentMap<Integer, Long>> offsetTable =
        new ConcurrentHashMap<>(512);

    private transient SnodeController snodeController;

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

    private boolean offsetBehindMuchThanData(final String enodeName, final String topic,
        ConcurrentMap<Integer, Long> table) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            RemotingCommand remotingCommand = this.snodeController.getEnodeService().getMinOffsetInQueue(enodeName, topic, next.getKey());
            long minOffsetInStore = 0;
            if (remotingCommand != null) {
                switch (remotingCommand.getCode()) {
                    case ResponseCode.SUCCESS: {
                        GetMinOffsetResponseHeader responseHeader =
                            (GetMinOffsetResponseHeader) remotingCommand.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
                        minOffsetInStore = responseHeader.getOffset();
                    }
                    default:
                        break;
                }
            } else {
                throw new SnodeException(ResponseCode.QUERY_OFFSET_ERROR, "Query min offset error!");
            }
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }
        return result;
    }

    public void commitOffset(final String enodeName, final String clientHost, final String group, final String topic,
        final int queueId,
        final long offset) {
        // Topic@group
        String key = buildKey(enodeName, topic, group);
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<>(32);
            ConcurrentMap<Integer, Long> prev = this.offsetTable.putIfAbsent(key, map);
            map = prev != null ? prev : map;
            map.put(queueId, offset);
        } else {
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost: {}, key: {}, queueId: {}, requestOffset: {}, storeOffset: {}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    public long queryOffset(final String enodeName, final String group, final String topic, final int queueId) {
        String key = buildKey(enodeName, topic, group);
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null)
                return offset;
        }

        return -1;
    }


    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public Map<Integer, Long> queryOffset(final String enodeName, final String group, final String topic) {
        // topic@group
        String key = buildKey(enodeName, topic, group);
        return this.offsetTable.get(key);
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<Integer, Long>(offsets));
        }
    }

    public void persist() {
        for (Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            ConcurrentHashMap<Integer, Long> map = (ConcurrentHashMap<Integer, Long>) offSetEntry.getValue();
            String key = offSetEntry.getKey();
            String[] keys = key.split(TOPIC_GROUP_SEPARATOR);
            if (keys.length == 3) {
                String enodeName = keys[0];
                String topic = keys[1];
                String consumerGroup = keys[2];
                for (Entry<Integer, Long> queueEntry : map.entrySet()) {
                    Integer queueId = queueEntry.getKey();
                    Long offset = queueEntry.getValue();
                    this.snodeController.getEnodeService().persistOffset(enodeName, consumerGroup, topic, queueId, offset);
                }
            } else {
                log.error("Persist offset split keys error:{}", key);
            }
        }
    }
}
