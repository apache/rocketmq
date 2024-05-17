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

import java.util.function.Consumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;

/**
 * Provides tiered metadata storage service to store metadata information of Topic, Queue, FileSegment, etc.
 */
public interface MetadataStore {

    /**
     * Get the metadata information of specified Topic.
     *
     * @param topic The name of Topic.
     * @return The metadata information of specified Topic, or null if it does not exist.
     */
    TopicMetadata getTopic(String topic);

    /**
     * Add a new metadata information of Topic.
     *
     * @param topic       The name of Topic.
     * @param reserveTime The reserve time.
     * @return The newly added metadata information of Topic.
     */
    TopicMetadata addTopic(String topic, long reserveTime);

    void updateTopic(TopicMetadata topicMetadata);

    void iterateTopic(Consumer<TopicMetadata> callback);

    void deleteTopic(String topic);

    QueueMetadata getQueue(MessageQueue mq);

    QueueMetadata addQueue(MessageQueue mq, long baseOffset);

    void updateQueue(QueueMetadata queueMetadata);

    void iterateQueue(String topic, Consumer<QueueMetadata> callback);

    void deleteQueue(MessageQueue mq);

    FileSegmentMetadata getFileSegment(String basePath, FileSegmentType fileType, long baseOffset);

    void updateFileSegment(FileSegmentMetadata fileSegmentMetadata);

    void iterateFileSegment(Consumer<FileSegmentMetadata> callback);

    void iterateFileSegment(String basePath, FileSegmentType fileType, Consumer<FileSegmentMetadata> callback);

    void deleteFileSegment(String basePath, FileSegmentType fileType);

    void deleteFileSegment(String basePath, FileSegmentType fileType, long baseOffset);

    void destroy();

    void recoverWhenBecomeMaster();
}
