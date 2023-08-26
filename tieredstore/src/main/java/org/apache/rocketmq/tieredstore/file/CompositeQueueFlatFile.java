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

package org.apache.rocketmq.tieredstore.file;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.metadata.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class CompositeQueueFlatFile extends CompositeFlatFile {

    private final MessageQueue messageQueue;
    private long topicSequenceNumber;
    private QueueMetadata queueMetadata;
    private final TieredIndexFile indexFile;

    public CompositeQueueFlatFile(TieredFileAllocator fileQueueFactory, MessageQueue messageQueue) {
        super(fileQueueFactory, TieredStoreUtil.toPath(messageQueue));
        this.messageQueue = messageQueue;
        this.recoverQueueMetadata();
        this.indexFile = TieredFlatFileManager.getIndexFile(storeConfig);
    }

    @Override
    public void initOffset(long offset) {
        if (!consumeQueue.isInitialized()) {
            queueMetadata.setMinOffset(offset);
            queueMetadata.setMaxOffset(offset);
            metadataStore.updateQueue(queueMetadata);
        }
        super.initOffset(offset);
    }

    public void recoverQueueMetadata() {
        TopicMetadata topicMetadata = this.metadataStore.getTopic(messageQueue.getTopic());
        if (topicMetadata == null) {
            topicMetadata = this.metadataStore.addTopic(messageQueue.getTopic(), -1L);
        }
        this.topicSequenceNumber = topicMetadata.getTopicId();

        queueMetadata = this.metadataStore.getQueue(messageQueue);
        if (queueMetadata == null) {
            queueMetadata = this.metadataStore.addQueue(messageQueue, -1);
        }
        if (queueMetadata.getMaxOffset() < queueMetadata.getMinOffset()) {
            queueMetadata.setMaxOffset(queueMetadata.getMinOffset());
        }
    }

    public void flushMetadata() {
        try {
            queueMetadata.setMinOffset(super.getConsumeQueueMinOffset());
            queueMetadata.setMaxOffset(super.getConsumeQueueMaxOffset());
            metadataStore.updateQueue(queueMetadata);
        } catch (Exception e) {
            LOGGER.error("CompositeFlatFile#flushMetadata error, topic: {}, queue: {}",
                messageQueue.getTopic(), messageQueue.getQueueId(), e);
        }
    }

    /**
     * Building indexes with offsetId is no longer supported because offsetId has changed in tiered storage
     */
    public AppendResult appendIndexFile(DispatchRequest request) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

        if (StringUtils.isNotBlank(request.getUniqKey())) {
            AppendResult result = indexFile.append(messageQueue, (int) topicSequenceNumber,
                request.getUniqKey(), request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
            if (result != AppendResult.SUCCESS) {
                return result;
            }
        }

        for (String key : request.getKeys().split(MessageConst.KEY_SEPARATOR)) {
            if (StringUtils.isNotBlank(key)) {
                AppendResult result = indexFile.append(messageQueue, (int) topicSequenceNumber,
                    key, request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
                if (result != AppendResult.SUCCESS) {
                    return result;
                }
            }
        }
        return AppendResult.SUCCESS;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.flushMetadata();
    }

    @Override
    public void destroy() {
        super.destroy();
        metadataStore.deleteQueue(messageQueue);
    }
}
