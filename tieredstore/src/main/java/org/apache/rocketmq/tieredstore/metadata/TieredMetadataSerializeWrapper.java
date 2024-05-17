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

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore.DEFAULT_CAPACITY;

public class TieredMetadataSerializeWrapper extends RemotingSerializable {
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
