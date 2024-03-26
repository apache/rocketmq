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
import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;

public class DefaultMetadataStore extends ConfigManager implements MetadataStore {

    private static final int DEFAULT_CAPACITY = 1024;
    private static final String DEFAULT_CONFIG_NAME = "config";
    private static final String DEFAULT_FILE_NAME = "tieredStoreMetadata.json";

    private final AtomicLong topicSequenceNumber;
    private final MessageStoreConfig storeConfig;
    private final ConcurrentMap<String /* topic */, TopicMetadata> topicMetadataTable;
    private final ConcurrentMap<String /* topic */, ConcurrentMap<Integer, QueueMetadata>> queueMetadataTable;

    // Declare concurrent mapping tables to store file segment metadata
    // Key: filePath -> Value: <baseOffset, metadata>
    private final ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> commitLogFileSegmentTable;
    private final ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> consumeQueueFileSegmentTable;
    private final ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> indexFileSegmentTable;

    public DefaultMetadataStore(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.topicSequenceNumber = new AtomicLong(-1L);
        this.topicMetadataTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
        this.queueMetadataTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
        this.commitLogFileSegmentTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
        this.consumeQueueFileSegmentTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
        this.indexFileSegmentTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
        this.load();
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String encode(boolean prettyFormat) {
        TieredMetadataSerializeWrapper dataWrapper = new TieredMetadataSerializeWrapper();
        dataWrapper.setTopicSerialNumber(topicSequenceNumber);
        dataWrapper.setTopicMetadataTable(topicMetadataTable);
        dataWrapper.setQueueMetadataTable(new ConcurrentHashMap<>(queueMetadataTable));
        dataWrapper.setCommitLogFileSegmentTable(new ConcurrentHashMap<>(commitLogFileSegmentTable));
        dataWrapper.setConsumeQueueFileSegmentTable(new ConcurrentHashMap<>(consumeQueueFileSegmentTable));
        dataWrapper.setIndexFileSegmentTable(new ConcurrentHashMap<>(indexFileSegmentTable));

        if (prettyFormat) {
            return JSON.toJSONString(
                dataWrapper, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.PrettyFormat);
        } else {
            return JSON.toJSONString(
                dataWrapper, SerializerFeature.DisableCircularReferenceDetect);
        }
    }

    @Override
    public String configFilePath() {
        return Paths.get(storeConfig.getStorePathRootDir(), DEFAULT_CONFIG_NAME, DEFAULT_FILE_NAME).toString();
    }

    @Override
    public boolean load() {
        return super.load();
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            TieredMetadataSerializeWrapper dataWrapper =
                TieredMetadataSerializeWrapper.fromJson(jsonString, TieredMetadataSerializeWrapper.class);
            if (dataWrapper != null) {
                this.topicSequenceNumber.set(dataWrapper.getTopicSerialNumber().get());
                this.topicMetadataTable.putAll(dataWrapper.getTopicMetadataTable());
                dataWrapper.getQueueMetadataTable().forEach(
                    (topic, entry) -> this.queueMetadataTable.put(topic, new ConcurrentHashMap<>(entry)));
                dataWrapper.getCommitLogFileSegmentTable().forEach(
                    (filePath, entry) -> this.commitLogFileSegmentTable.put(filePath, new ConcurrentHashMap<>(entry)));
                dataWrapper.getConsumeQueueFileSegmentTable().forEach(
                    (filePath, entry) -> this.consumeQueueFileSegmentTable.put(filePath, new ConcurrentHashMap<>(entry)));
                dataWrapper.getIndexFileSegmentTable().forEach(
                    (filePath, entry) -> this.indexFileSegmentTable.put(filePath, new ConcurrentHashMap<>(entry)));
            }
        }
    }

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
        TopicMetadata metadata = new TopicMetadata(topicSequenceNumber.incrementAndGet(), topic, reserveTime);
        topicMetadataTable.put(topic, metadata);
        persist();
        return metadata;
    }

    @Override
    public void updateTopic(TopicMetadata topicMetadata) {
        TopicMetadata metadata = getTopic(topicMetadata.getTopic());
        if (metadata == null) {
            return;
        }
        metadata.setUpdateTimestamp(System.currentTimeMillis());
        topicMetadataTable.put(topicMetadata.getTopic(), topicMetadata);
        persist();
    }

    @Override
    public void deleteTopic(String topic) {
        topicMetadataTable.remove(topic);
        persist();
    }

    @Override
    public QueueMetadata getQueue(MessageQueue mq) {
        return queueMetadataTable.getOrDefault(mq.getTopic(), new ConcurrentHashMap<>()).get(mq.getQueueId());
    }

    @Override
    public void iterateQueue(String topic, Consumer<QueueMetadata> callback) {
        queueMetadataTable.get(topic).values().forEach(callback);
    }

    @Override
    public QueueMetadata addQueue(MessageQueue mq, long baseOffset) {
        QueueMetadata old = getQueue(mq);
        if (old != null) {
            return old;
        }
        QueueMetadata metadata = new QueueMetadata(mq, baseOffset, baseOffset);
        queueMetadataTable.computeIfAbsent(mq.getTopic(), topic -> new ConcurrentHashMap<>())
            .put(mq.getQueueId(), metadata);
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
            }
            persist();
        }
    }

    @Override
    public void deleteQueue(MessageQueue mq) {
        if (queueMetadataTable.containsKey(mq.getTopic())) {
            queueMetadataTable.get(mq.getTopic()).remove(mq.getQueueId());
        }
        persist();
    }

    @VisibleForTesting
    public Map<String, ConcurrentMap<Long, FileSegmentMetadata>> getTableByFileType(
        FileSegmentType fileType) {

        switch (fileType) {
            case COMMIT_LOG:
                return commitLogFileSegmentTable;
            case CONSUME_QUEUE:
                return consumeQueueFileSegmentTable;
            case INDEX:
                return indexFileSegmentTable;
        }
        return new HashMap<>();
    }

    @Override
    public FileSegmentMetadata getFileSegment(
        String basePath, FileSegmentType fileType, long baseOffset) {

        return Optional.ofNullable(this.getTableByFileType(fileType).get(basePath))
            .map(fileMap -> fileMap.get(baseOffset)).orElse(null);
    }

    @Override
    public void updateFileSegment(FileSegmentMetadata fileSegmentMetadata) {
        FileSegmentType fileType =
            FileSegmentType.valueOf(fileSegmentMetadata.getType());
        ConcurrentMap<Long, FileSegmentMetadata> offsetTable = this.getTableByFileType(fileType)
            .computeIfAbsent(fileSegmentMetadata.getPath(), s -> new ConcurrentHashMap<>());
        offsetTable.put(fileSegmentMetadata.getBaseOffset(), fileSegmentMetadata);
        persist();
    }

    @Override
    public void iterateFileSegment(Consumer<FileSegmentMetadata> callback) {
        commitLogFileSegmentTable
            .forEach((filePath, map) -> map.forEach((offset, metadata) -> callback.accept(metadata)));
        consumeQueueFileSegmentTable
            .forEach((filePath, map) -> map.forEach((offset, metadata) -> callback.accept(metadata)));
        indexFileSegmentTable
            .forEach((filePath, map) -> map.forEach((offset, metadata) -> callback.accept(metadata)));
    }

    @Override
    public void iterateFileSegment(String basePath, FileSegmentType fileType, Consumer<FileSegmentMetadata> callback) {
        this.getTableByFileType(fileType).getOrDefault(basePath, new ConcurrentHashMap<>())
            .forEach((offset, metadata) -> callback.accept(metadata));
    }

    @Override
    public void deleteFileSegment(String filePath, FileSegmentType fileType) {
        Map<String, ConcurrentMap<Long, FileSegmentMetadata>> offsetTable = this.getTableByFileType(fileType);
        if (offsetTable != null) {
            offsetTable.remove(filePath);
        }
        persist();
    }

    @Override
    public void deleteFileSegment(String basePath, FileSegmentType fileType, long baseOffset) {
        ConcurrentMap<Long, FileSegmentMetadata> offsetTable = this.getTableByFileType(fileType).get(basePath);
        if (offsetTable != null) {
            offsetTable.remove(baseOffset);
        }
        persist();
    }

    @Override
    public void destroy() {
        topicSequenceNumber.set(0L);
        topicMetadataTable.clear();
        queueMetadataTable.clear();
        commitLogFileSegmentTable.clear();
        consumeQueueFileSegmentTable.clear();
        indexFileSegmentTable.clear();
        persist();
    }

    static class TieredMetadataSerializeWrapper extends RemotingSerializable {

        private AtomicLong topicSerialNumber = new AtomicLong(0L);

        private ConcurrentMap<String /* topic */, TopicMetadata> topicMetadataTable;
        private ConcurrentMap<String /* topic */, ConcurrentMap<Integer /* queueId */, QueueMetadata>> queueMetadataTable;

        // Declare concurrent mapping tables to store file segment metadata
        // Key: filePath -> Value: <baseOffset, metadata>
        private ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> commitLogFileSegmentTable;
        private ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> consumeQueueFileSegmentTable;
        private ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> indexFileSegmentTable;

        public TieredMetadataSerializeWrapper() {
            this.topicMetadataTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
            this.queueMetadataTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
            this.commitLogFileSegmentTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
            this.consumeQueueFileSegmentTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
            this.indexFileSegmentTable = new ConcurrentHashMap<>(DEFAULT_CAPACITY);
        }

        public AtomicLong getTopicSerialNumber() {
            return topicSerialNumber;
        }

        public void setTopicSerialNumber(AtomicLong topicSerialNumber) {
            this.topicSerialNumber = topicSerialNumber;
        }

        public ConcurrentMap<String, TopicMetadata> getTopicMetadataTable() {
            return topicMetadataTable;
        }

        public void setTopicMetadataTable(
            ConcurrentMap<String, TopicMetadata> topicMetadataTable) {
            this.topicMetadataTable = topicMetadataTable;
        }

        public ConcurrentMap<String, ConcurrentMap<Integer, QueueMetadata>> getQueueMetadataTable() {
            return queueMetadataTable;
        }

        public void setQueueMetadataTable(
            ConcurrentMap<String, ConcurrentMap<Integer, QueueMetadata>> queueMetadataTable) {
            this.queueMetadataTable = queueMetadataTable;
        }

        public ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> getCommitLogFileSegmentTable() {
            return commitLogFileSegmentTable;
        }

        public void setCommitLogFileSegmentTable(
            ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> commitLogFileSegmentTable) {
            this.commitLogFileSegmentTable = commitLogFileSegmentTable;
        }

        public ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> getConsumeQueueFileSegmentTable() {
            return consumeQueueFileSegmentTable;
        }

        public void setConsumeQueueFileSegmentTable(
            ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> consumeQueueFileSegmentTable) {
            this.consumeQueueFileSegmentTable = consumeQueueFileSegmentTable;
        }

        public ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> getIndexFileSegmentTable() {
            return indexFileSegmentTable;
        }

        public void setIndexFileSegmentTable(
            ConcurrentMap<String, ConcurrentMap<Long, FileSegmentMetadata>> indexFileSegmentTable) {
            this.indexFileSegmentTable = indexFileSegmentTable;
        }
    }
}
