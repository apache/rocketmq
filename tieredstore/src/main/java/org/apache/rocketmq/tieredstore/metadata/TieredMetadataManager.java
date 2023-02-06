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
package org.apache.rocketmq.tieredstore.metadata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.io.File;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;

public class TieredMetadataManager extends ConfigManager implements TieredMetadataStore {
    private final AtomicInteger maxTopicId = new AtomicInteger(0);
    private final ConcurrentMap<String /*topic*/, TopicMetadata> topicMetadataTable = new ConcurrentHashMap<>(1024);
    private final ConcurrentMap<String /*topic*/, ConcurrentMap<Integer /*queueId*/, QueueMetadata>> queueMetadataTable = new ConcurrentHashMap<>(1024);
    private final ConcurrentMap<MessageQueue, ConcurrentMap<Long /*baseOffset*/, FileSegmentMetadata>> commitLogFileSegmentTable = new ConcurrentHashMap<>(1024);
    private final ConcurrentMap<MessageQueue, ConcurrentMap<Long /*baseOffset*/, FileSegmentMetadata>> consumeQueueFileSegmentTable = new ConcurrentHashMap<>(1024);
    private final ConcurrentMap<MessageQueue, ConcurrentMap<Long /*baseOffset*/, FileSegmentMetadata>> indexFileSegmentTable = new ConcurrentHashMap<>(1024);
    private final TieredMessageStoreConfig storeConfig;

    public TieredMetadataManager(TieredMessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        load();
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String encode(boolean prettyFormat) {
        TieredMetadataSerializeWrapper dataWrapper = new TieredMetadataSerializeWrapper();
        dataWrapper.setMaxTopicId(maxTopicId);
        dataWrapper.setTopicMetadataTable(topicMetadataTable);
        dataWrapper.setQueueMetadataTable(new HashMap<>(queueMetadataTable));
        dataWrapper.setCommitLogFileSegmentTable(new HashMap<>(commitLogFileSegmentTable));
        dataWrapper.setConsumeQueueFileSegmentTable(new HashMap<>(consumeQueueFileSegmentTable));
        dataWrapper.setIndexFileSegmentTable(new HashMap<>(indexFileSegmentTable));
        if (prettyFormat) {
            JSON.toJSONString(dataWrapper, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.PrettyFormat);
        }
        return JSON.toJSONString(dataWrapper, SerializerFeature.DisableCircularReferenceDetect);
    }

    @Override
    public String configFilePath() {
        return storeConfig.getStorePathRootDir() + File.separator + "config" + File.separator + "tieredStoreMetadata.json";
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TieredMetadataSerializeWrapper dataWrapper =
                TieredMetadataSerializeWrapper.fromJson(jsonString, TieredMetadataSerializeWrapper.class);
            if (dataWrapper != null) {
                maxTopicId.set(dataWrapper.getMaxTopicId().get());
                topicMetadataTable.putAll(dataWrapper.getTopicMetadataTable());
                dataWrapper.getQueueMetadataTable()
                    .forEach((topic, map) -> queueMetadataTable.put(topic, new ConcurrentHashMap<>(map)));
                dataWrapper.getCommitLogFileSegmentTable()
                    .forEach((mq, map) -> commitLogFileSegmentTable.put(mq, new ConcurrentHashMap<>(map)));
                dataWrapper.getConsumeQueueFileSegmentTable()
                    .forEach((mq, map) -> consumeQueueFileSegmentTable.put(mq, new ConcurrentHashMap<>(map)));
                dataWrapper.getIndexFileSegmentTable()
                    .forEach((mq, map) -> indexFileSegmentTable.put(mq, new ConcurrentHashMap<>(map)));
            }
        }
    }

    @Override
    public void setMaxTopicId(int maxTopicId) {
        this.maxTopicId.set(maxTopicId);
    }

    @Nullable
    @Override
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
        persist();
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
        persist();
    }

    @Override
    public void updateTopicStatus(String topic, int status) {
        TopicMetadata metadata = getTopic(topic);
        if (metadata == null) {
            return;
        }
        metadata.setStatus(status);
        metadata.setUpdateTimestamp(System.currentTimeMillis());
        persist();
    }

    @Override
    public void deleteTopic(String topic) {
        topicMetadataTable.remove(topic);
        persist();
    }

    @Nullable
    @Override
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
        persist();
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
                persist();
            }
        }
    }

    @Override
    public void deleteQueue(MessageQueue queue) {
        if (queueMetadataTable.containsKey(queue.getTopic())) {
            queueMetadataTable.get(queue.getTopic())
                .remove(queue.getQueueId());
        }
        persist();
    }

    @Nullable
    @Override
    public FileSegmentMetadata getFileSegment(TieredFileSegment fileSegment) {
        switch (fileSegment.getFileType()) {
            case COMMIT_LOG:
                if (commitLogFileSegmentTable.containsKey(fileSegment.getMessageQueue())) {
                    return commitLogFileSegmentTable.get(fileSegment.getMessageQueue())
                        .get(fileSegment.getBaseOffset());
                }
                break;
            case CONSUME_QUEUE:
                if (consumeQueueFileSegmentTable.containsKey(fileSegment.getMessageQueue())) {
                    return consumeQueueFileSegmentTable.get(fileSegment.getMessageQueue())
                        .get(fileSegment.getBaseOffset());
                }
                break;
            case INDEX:
                if (indexFileSegmentTable.containsKey(fileSegment.getMessageQueue())) {
                    return indexFileSegmentTable.get(fileSegment.getMessageQueue())
                        .get(fileSegment.getBaseOffset());
                }
                break;
        }
        return null;
    }

    @Override
    public void iterateFileSegment(Consumer<FileSegmentMetadata> callback) {
        commitLogFileSegmentTable.forEach((mq, map) -> map.forEach((offset, metadata) -> callback.accept(metadata)));
        consumeQueueFileSegmentTable.forEach((mq, map) -> map.forEach((offset, metadata) -> callback.accept(metadata)));
        indexFileSegmentTable.forEach((mq, map) -> map.forEach((offset, metadata) -> callback.accept(metadata)));
    }

    @Override
    public void iterateFileSegment(TieredFileSegment.FileSegmentType type, String topic, int queueId,
        Consumer<FileSegmentMetadata> callback) {
        MessageQueue messageQueue = new MessageQueue(topic, storeConfig.getBrokerName(), queueId);
        switch (type) {
            case COMMIT_LOG:
                if (commitLogFileSegmentTable.containsKey(messageQueue)) {
                    commitLogFileSegmentTable.get(messageQueue)
                        .forEach((offset, metadata) -> callback.accept(metadata));
                }
                break;
            case CONSUME_QUEUE:
                if (consumeQueueFileSegmentTable.containsKey(messageQueue)) {
                    consumeQueueFileSegmentTable.get(messageQueue)
                        .forEach((offset, metadata) -> callback.accept(metadata));
                }
                break;
            case INDEX:
                if (indexFileSegmentTable.containsKey(messageQueue)) {
                    indexFileSegmentTable.get(messageQueue)
                        .forEach((offset, metadata) -> callback.accept(metadata));
                }
                break;
        }
    }

    @Override
    public FileSegmentMetadata updateFileSegment(TieredFileSegment fileSegment) {
        FileSegmentMetadata old = getFileSegment(fileSegment);

        if (old == null) {
            FileSegmentMetadata metadata = new FileSegmentMetadata(fileSegment.getMessageQueue(),
                fileSegment.getFileType().getType(),
                fileSegment.getBaseOffset(),
                fileSegment.getPath());
            if (fileSegment.isClosed()) {
                metadata.setStatus(FileSegmentMetadata.STATUS_DELETED);
            }
            metadata.setBeginTimestamp(fileSegment.getBeginTimestamp());
            metadata.setEndTimestamp(fileSegment.getEndTimestamp());
            switch (fileSegment.getFileType()) {
                case COMMIT_LOG:
                    commitLogFileSegmentTable.computeIfAbsent(fileSegment.getMessageQueue(), mq -> new ConcurrentHashMap<>())
                        .put(fileSegment.getBaseOffset(), metadata);
                    break;
                case CONSUME_QUEUE:
                    consumeQueueFileSegmentTable.computeIfAbsent(fileSegment.getMessageQueue(), mq -> new ConcurrentHashMap<>())
                        .put(fileSegment.getBaseOffset(), metadata);
                    break;
                case INDEX:
                    indexFileSegmentTable.computeIfAbsent(fileSegment.getMessageQueue(), mq -> new ConcurrentHashMap<>())
                        .put(fileSegment.getBaseOffset(), metadata);
                    break;
            }
            persist();
            return metadata;
        }

        if (old.getStatus() == FileSegmentMetadata.STATUS_NEW && fileSegment.isFull() && !fileSegment.needCommit()) {
            old.setStatus(FileSegmentMetadata.STATUS_SEALED);
            old.setSealTimestamp(System.currentTimeMillis());
        }
        if (fileSegment.isClosed()) {
            old.setStatus(FileSegmentMetadata.STATUS_DELETED);
        }
        old.setSize(fileSegment.getCommitPosition());
        old.setBeginTimestamp(fileSegment.getBeginTimestamp());
        old.setEndTimestamp(fileSegment.getEndTimestamp());
        persist();
        return old;
    }

    @Override
    public void deleteFileSegment(MessageQueue mq) {
        commitLogFileSegmentTable.remove(mq);
        consumeQueueFileSegmentTable.remove(mq);
        indexFileSegmentTable.remove(mq);
        persist();
    }

    @Override
    public void deleteFileSegment(TieredFileSegment fileSegment) {
        switch (fileSegment.getFileType()) {
            case COMMIT_LOG:
                if (commitLogFileSegmentTable.containsKey(fileSegment.getMessageQueue())) {
                    commitLogFileSegmentTable.get(fileSegment.getMessageQueue())
                        .remove(fileSegment.getBaseOffset());
                }
                break;
            case CONSUME_QUEUE:
                if (consumeQueueFileSegmentTable.containsKey(fileSegment.getMessageQueue())) {
                    consumeQueueFileSegmentTable.get(fileSegment.getMessageQueue())
                        .remove(fileSegment.getBaseOffset());
                }
                break;
            case INDEX:
                if (indexFileSegmentTable.containsKey(fileSegment.getMessageQueue())) {
                    indexFileSegmentTable.get(fileSegment.getMessageQueue())
                        .remove(fileSegment.getBaseOffset());
                }
                break;
        }
        persist();
    }

    @Override
    public void destroy() {
        maxTopicId.set(0);
        topicMetadataTable.clear();
        queueMetadataTable.clear();
        commitLogFileSegmentTable.clear();
        consumeQueueFileSegmentTable.clear();
        indexFileSegmentTable.clear();
        persist();
    }
}
