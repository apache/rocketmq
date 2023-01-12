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
package org.apache.rocketmq.store.tiered.metadata;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.tiered.common.TieredMessageStoreConfig;

public class TieredStoreMetadataManager extends ConfigManager implements TieredStoreMetadataStore {
    private final AtomicInteger maxTopicId = new AtomicInteger(0);
    private final ConcurrentMap<String /*topic*/, TopicMetadata> topicMetadataTable = new ConcurrentHashMap<>(1024);
    private final ConcurrentMap<String /*topic*/, ConcurrentMap<Integer /*queueId*/, QueueMetadata>> queueMetadataTable = new ConcurrentHashMap<>(1024);
    private final TieredMessageStoreConfig storeConfig;

    public TieredStoreMetadataManager(TieredMessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }
    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String encode(boolean prettyFormat) {
        TieredStoreMetadataSerializeWrapper dataWrapper = new TieredStoreMetadataSerializeWrapper();
        dataWrapper.setMaxTopicId(maxTopicId);
        dataWrapper.setTopicMetadataTable(topicMetadataTable);
        dataWrapper.setQueueMetadataTable(new HashMap<>(queueMetadataTable));
        return dataWrapper.toJson(false);
    }

    @Override
    public String configFilePath() {
        return storeConfig.getStorePathRootDir() + File.separator + "config" + File.separator + "tieredStoreMetadata.json";
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TieredStoreMetadataSerializeWrapper dataWrapper =
                TieredStoreMetadataSerializeWrapper.fromJson(jsonString, TieredStoreMetadataSerializeWrapper.class);
            if (dataWrapper != null) {
                maxTopicId.set(dataWrapper.getMaxTopicId().get());
                topicMetadataTable.putAll(dataWrapper.getTopicMetadataTable());
                dataWrapper.getQueueMetadataTable()
                    .forEach((topic, map) -> queueMetadataTable.put(topic, new ConcurrentHashMap<>(map)));
            }
        }
    }

    @Override
    @Nullable
    public TopicMetadata getTopic(String topic) {
        return topicMetadataTable.get(topic);
    }

    @Override
    public void iterateTopic(Consumer<TopicMetadata> callback) {
        topicMetadataTable.values().forEach(callback);
    }

    @Override
    public TopicMetadata addTopic(String topic, long reserveTime) {
        TopicMetadata old = getTopic(topic);
        if (old != null) {
            return old;
        }
        TopicMetadata metadata = new TopicMetadata(maxTopicId.getAndIncrement(), topic, reserveTime);
        topicMetadataTable.put(topic, metadata);
        return metadata;
    }

    @Override
    public void updateTopicReserveTime(String topic, long reserveTime) {
        TopicMetadata metadata = getTopic(topic);
        if (metadata == null) {
            return;
        }
        metadata.setReserveTime(reserveTime);
        metadata.setUpdateTimestamp(System.currentTimeMillis());
    }

    @Override
    public void updateTopicStatus(String topic, int status) {
        TopicMetadata metadata = getTopic(topic);
        if (metadata == null) {
            return;
        }
        metadata.setStatus(status);
        metadata.setUpdateTimestamp(System.currentTimeMillis());
    }

    @Override
    public void deleteTopic(String topic) {
        topicMetadataTable.remove(topic);
    }

    @Override
    @Nullable
    public QueueMetadata getQueue(MessageQueue queue) {
        if (!queueMetadataTable.containsKey(queue.getTopic())) {
            return null;
        }
        return queueMetadataTable.get(queue.getTopic())
            .get(queue.getQueueId());
    }

    @Override
    public void iterateQueue(String topic, Consumer<QueueMetadata> callback) {
        queueMetadataTable.get(topic)
            .values()
            .forEach(callback);
    }

    @Override
    public QueueMetadata addQueue(MessageQueue queue, long baseOffset) {
        QueueMetadata old = getQueue(queue);
        if (old != null) {
            return old;
        }
        QueueMetadata metadata = new QueueMetadata(queue, baseOffset, baseOffset);
        queueMetadataTable.computeIfAbsent(queue.getTopic(), topic -> new ConcurrentHashMap<>())
            .put(queue.getQueueId(), metadata);
        return metadata;
    }

    @Override
    public void updateQueue(QueueMetadata metadata) {
        MessageQueue queue = metadata.getQueue();
        if (queueMetadataTable.containsKey(queue.getTopic())) {
            ConcurrentMap<Integer, QueueMetadata> metadataMap = queueMetadataTable.get(queue.getTopic());
            if (metadataMap.containsKey(queue.getQueueId())) {
                metadata.setUpdateTimestamp(System.currentTimeMillis());
                metadataMap.put(queue.getQueueId(), metadata);
            }
        }
    }

    @Override
    public void deleteQueue(MessageQueue queue) {
        if (queueMetadataTable.containsKey(queue.getTopic())) {
            queueMetadataTable.get(queue.getTopic())
                .remove(queue.getQueueId());
        }
    }
}
