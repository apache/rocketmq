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

/**
 * Provides tiered metadata storage service to store metadata information of Topic, Queue, FileSegment, etc.
 */
public interface TieredMetadataStore {

    /**
     * Set the sequence number of Topic, the start index from 0.
     *
     * @param topicSequenceNumber The sequence number of Topic.
     */
    void setTopicSequenceNumber(long topicSequenceNumber);

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

    /**
     * Queue metadata operation
     *
     * @see QueueMetadata
     */
    QueueMetadata getQueue(MessageQueue mq);

    QueueMetadata addQueue(MessageQueue mq, long baseOffset);

    void updateQueue(QueueMetadata queueMetadata);

    void iterateQueue(String topic, Consumer<QueueMetadata> callback);

    void deleteQueue(MessageQueue mq);

    /**
     * Get the metadata information of specified file segment.
     *
     * @param basePath   The file path.
     * @param fileType   The file type.
     * @param baseOffset The start offset of file segment.
     * @return The metadata information of specified file segment, or null if it does not exist.
     */
    FileSegmentMetadata getFileSegment(String basePath, FileSegmentType fileType, long baseOffset);

    /**
     * Update the metadata information of a file segment.
     *
     * @param fileSegmentMetadata The metadata information of the file segment.
     */
    void updateFileSegment(FileSegmentMetadata fileSegmentMetadata);

    /**
     * Traverse all metadata information of file segment
     * and execute the callback function for each metadata information.
     *
     * @param callback The traversal callback function.
     */
    void iterateFileSegment(Consumer<FileSegmentMetadata> callback);

    /**
     * Traverse all the metadata information of the file segments in the specified file path
     * and execute the callback function for each metadata information.
     *
     * @param basePath The file path.
     * @param callback The traversal callback function.
     */
    void iterateFileSegment(String basePath, FileSegmentType fileType, Consumer<FileSegmentMetadata> callback);

    /**
     * Delete all the metadata information of the file segments.
     *
     * @param basePath The file path.
     */
    void deleteFileSegment(String basePath, FileSegmentType fileType);

    /**
     * Delete the metadata information of a specified file segment.
     *
     * @param basePath   The file path.
     * @param fileType   The file type.
     * @param baseOffset The start offset of the file segment.
     */
    void deleteFileSegment(String basePath, FileSegmentType fileType, long baseOffset);

    /**
     * Clean all metadata in disk
     */
    void destroy();
}
