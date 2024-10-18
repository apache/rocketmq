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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.common.AppendResult;

public interface FlatFileInterface {

    long getTopicId();

    Lock getFileLock();

    MessageQueue getMessageQueue();

    boolean isFlatFileInit();

    void initOffset(long offset);

    boolean rollingFile(long interval);

    /**
     * Appends a message to the commit log file
     *
     * @param message thByteBuffere message to append
     * @return append result
     */
    AppendResult appendCommitLog(ByteBuffer message);

    AppendResult appendCommitLog(SelectMappedBufferResult message);

    /**
     * Append message to consume queue file, but does not commit it immediately
     *
     * @param request the dispatch request
     * @return append result
     */
    AppendResult appendConsumeQueue(DispatchRequest request);

    List<DispatchRequest> getDispatchRequestList();

    void release();

    long getMinStoreTimestamp();

    long getMaxStoreTimestamp();

    long getFirstMessageOffset();

    long getCommitLogMinOffset();

    long getCommitLogMaxOffset();

    long getCommitLogCommitOffset();

    long getConsumeQueueMinOffset();

    long getConsumeQueueMaxOffset();

    long getConsumeQueueCommitOffset();

    void addMessageToBufferList(SelectMappedBufferResult bufferResult);

    /**
     * Persist commit log file and consume queue file
     */
    CompletableFuture<Boolean> commitAsync();

    /**
     * Asynchronously retrieves the message at the specified consume queue offset
     *
     * @param consumeQueueOffset consume queue offset.
     * @return the message inner object serialized content
     */
    CompletableFuture<ByteBuffer> getMessageAsync(long consumeQueueOffset);

    /**
     * Get message from commitLog file at specified offset and length
     *
     * @param offset the offset
     * @param length the length
     * @return the message inner object serialized content
     */
    CompletableFuture<ByteBuffer> getCommitLogAsync(long offset, int length);

    /**
     * Asynchronously retrieves the consume queue message at the specified queue offset
     *
     * @param consumeQueueOffset consume queue offset.
     * @return the consumer queue unit serialized content
     */
    CompletableFuture<ByteBuffer> getConsumeQueueAsync(long consumeQueueOffset);

    /**
     * Asynchronously reads the message body from the consume queue file at the specified offset and count
     *
     * @param consumeQueueOffset the message offset
     * @param count              the number of messages to read
     * @return the consumer queue unit serialized content
     */
    CompletableFuture<ByteBuffer> getConsumeQueueAsync(long consumeQueueOffset, int count);

    /**
     * Gets the start offset in the consume queue based on the timestamp and boundary type.
     * The consume queues consist of ordered units, and their storage times are non-decreasing
     * sequence. If the specified message exists, it returns the offset of either the first
     * or last message, depending on the boundary type. If the specified message does not exist,
     * it returns the offset of the next message as the pull offset. For example:
     * ------------------------------------------------------------
     *   store time   : 40, 50, 50, 50, 60, 60, 70
     *   queue offset : 10, 11, 12, 13, 14, 15, 16
     * ------------------------------------------------------------
     *   query timestamp | boundary | result (reason)
     *         35        |    -     |   10 (minimum offset)
     *         45        |    -     |   11 (next offset)
     *         50        |   lower  |   11
     *         50        |   upper  |   13
     *         60        |    -     |   14 (default to lower)
     *         75        |    -     |   17 (maximum offset + 1)
     * ------------------------------------------------------------
     * @param timestamp    The search time
     * @param boundaryType 'lower' or 'upper' to determine the boundary
     * @return Returns the offset of the message in the consume queue
     */
    CompletableFuture<Long> getQueueOffsetByTimeAsync(long timestamp, BoundaryType boundaryType);

    /**
     * Shutdown process
     */
    void shutdown();

    /**
     * Destroys expired files
     */
    void destroyExpiredFile(long timestamp);

    /**
     * Delete file
     */
    void destroy();
}
