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
import javax.annotation.Nullable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;

public interface TieredMetadataStore {
    /**
     * Topic metadata operation
     *
     * @see TopicMetadata
     */
    void setMaxTopicId(int maxTopicId);

    @Nullable
    TopicMetadata getTopic(String topic);

    void iterateTopic(Consumer<TopicMetadata> callback);

    TopicMetadata addTopic(String topic, long reserveTime);

    void updateTopicReserveTime(String topic, long reserveTime);

    void updateTopicStatus(String topic, int status);

    void deleteTopic(String topic);

    /**
     * Queue metadata operation
     *
     * @see QueueMetadata
     */
    @Nullable
    QueueMetadata getQueue(MessageQueue queue);

    void iterateQueue(String topic, Consumer<QueueMetadata> callback);

    QueueMetadata addQueue(MessageQueue queue, long baseOffset);

    void updateQueue(QueueMetadata metadata);

    void deleteQueue(MessageQueue queue);

    /**
     * File segment metadata operation
     *
     * @see FileSegmentMetadata
     */
    @Nullable
    FileSegmentMetadata getFileSegment(TieredFileSegment fileSegment);

    void iterateFileSegment(Consumer<FileSegmentMetadata> callback);

    void iterateFileSegment(TieredFileSegment.FileSegmentType type, String topic, int queueId,
        Consumer<FileSegmentMetadata> callback);

    FileSegmentMetadata updateFileSegment(TieredFileSegment fileSegment);

    void deleteFileSegment(MessageQueue mq);

    void deleteFileSegment(TieredFileSegment fileSegment);

    /**
     * Clean all metadata
     */
    void destroy();
}
