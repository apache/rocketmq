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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.store.DispatchRequest;
import org.rocksdb.RocksDBException;

public interface ConsumeQueueStoreInterface {

    /**
     * Start the consumeQueueStore
     */
    void start();

    /**
     * Load from file.
     * @return true if loaded successfully.
     */
    boolean load();

    /**
     * load after destroy
     */
    boolean loadAfterDestroy();

    /**
     * Recover from file.
     */
    void recover();

    /**
     * Recover concurrently from file.
     * @return true if recovered successfully.
     */
    boolean recoverConcurrently();

    /**
     * Shutdown the consumeQueueStore
     * @return true if shutdown successfully.
     */
    boolean shutdown();

    /**
     * destroy all consumeQueues
     */
    void destroy();

    /**
     * destroy the specific consumeQueue
     * @throws RocksDBException only in rocksdb mode
     */
    void destroy(ConsumeQueueInterface consumeQueue) throws RocksDBException;

    /**
     * Flush cache to file.
     * @param consumeQueue the consumeQueue will be flushed
     * @param flushLeastPages  the minimum number of pages to be flushed
     * @return true if any data has been flushed.
     */
    boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages);

    /**
     * clean expired data from minPhyOffset
     * @param minPhyOffset
     */
    void cleanExpired(long minPhyOffset);

    /**
     * Check files.
     */
    void checkSelf();

    /**
     * Delete expired files ending at min commit log position.
     * @param consumeQueue
     * @param minCommitLogPos min commit log position
     * @return deleted file numbers.
     */
    int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos);

    /**
     * Is the first file available?
     * @param consumeQueue
     * @return true if it's available
     */
    boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue);

    /**
     * Does the first file exist?
     * @param consumeQueue
     * @return true if it exists
     */
    boolean isFirstFileExist(ConsumeQueueInterface consumeQueue);

    /**
     * Roll to next file.
     * @param consumeQueue
     * @param offset next beginning offset
     * @return the beginning offset of the next file
     */
    long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset);

    /**
     * truncate dirty data
     * @param offsetToTruncate
     * @throws RocksDBException only in rocksdb mode
     */
    void truncateDirty(long offsetToTruncate) throws RocksDBException;

    /**
     * Apply the dispatched request and build the consume queue. This function should be idempotent.
     *
     * @param consumeQueue consume queue
     * @param request dispatch request
     */
    void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request);

    /**
     * Apply the dispatched request. This function should be idempotent.
     *
     * @param request dispatch request
     * @throws RocksDBException only in rocksdb mode will throw exception
     */
    void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException;

    /**
     * range query cqUnit(ByteBuffer) in rocksdb
     * @param topic
     * @param queueId
     * @param startIndex
     * @param num
     * @return the byteBuffer list of the topic-queueId in rocksdb
     * @throws RocksDBException only in rocksdb mode
     */
    List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException;

    /**
     * query cqUnit(ByteBuffer) in rocksdb
     * @param topic
     * @param queueId
     * @param startIndex
     * @return the byteBuffer of the topic-queueId in rocksdb
     * @throws RocksDBException only in rocksdb mode
     */
    ByteBuffer get(final String topic, final int queueId, final long startIndex) throws RocksDBException;

    /**
     * get consumeQueue table
     * @return the consumeQueue table
     */
    ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable();

    /**
     * Increase queue offset.
     * @param messageNum message number
     */
    void increaseQueueOffset(String topic, int queueId, short messageNum);

    /**
     * Get queue offset.
     * @param topic topic
     * @param queueId queue id
     * @throws RocksDBException only in rocksdb mode
     * @return queue offset
     */
    long getQueueOffset(String topic, int queueId) throws RocksDBException;

    /**
     * Increase lmq offset
     * @param queueKey
     * @param messageNum
     */
    void increaseLmqOffset(String queueKey, short messageNum);

    /**
     * get lmq queue offset
     * @param queueKey
     * @return
     */
    long getLmqQueueOffset(String queueKey);

    /**
     * recover topicQueue table by minPhyOffset
     * @param minPhyOffset
     */
    void recoverOffsetTable(long minPhyOffset);

    /**
     * set topicQueue table
     * @param topicQueueTable
     */
    void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable);

    /**
     * remove topic-queueId from topicQueue table
     * @param topic
     * @param queueId
     */
    void removeTopicQueueTable(String topic, Integer queueId);

    /**
     * get topicQueue table
     * @return the topicQueue table
     */
    ConcurrentMap getTopicQueueTable();

    /**
     * get the max physical offset in consumeQueue
     * @param topic
     * @param queueId
     * @return
     */
    Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId);

    /**
     * get maxOffset of specific topic-queueId in topicQueue table
     * @param topic
     * @param queueId
     * @return the max offset in QueueOffsetOperator
     */
    Long getMaxOffset(String topic, int queueId);

    /**
     * get max physic offset in consumeQueue
     * @return the max physic offset in consumeQueue
     * @throws RocksDBException only in rocksdb mode
     */
    long getMaxPhyOffsetInConsumeQueue() throws RocksDBException;

    /**
     * get min logic offset of specific topic-queueId in consumeQueue
     * @param topic
     * @param queueId
     * @return the min logic offset of specific topic-queueId in consumeQueue
     * @throws RocksDBException only in rocksdb mode
     */
    long getMinOffsetInQueue(final String topic, final int queueId) throws RocksDBException;

    /**
     * get max logic offset of specific topic-queueId in consumeQueue
     * @param topic
     * @param queueId
     * @return the max logic offset of specific topic-queueId in consumeQueue
     * @throws RocksDBException only in rocksdb mode
     */
    long getMaxOffsetInQueue(final String topic, final int queueId) throws RocksDBException;

    /**
     * Get the message whose timestamp is the smallest, greater than or equal to the given time and when there are more
     * than one message satisfy the condition, decide which one to return based on boundaryType.
     * @param timestamp    timestamp
     * @param boundaryType Lower or Upper
     * @return the offset(index)
     * @throws RocksDBException only in rocksdb mode
     */
    long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) throws RocksDBException;

    /**
     * find or create the consumeQueue
     * @param topic
     * @param queueId
     * @return the consumeQueue
     */
    ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId);

    /**
     * find the consumeQueueMap of topic
     * @param topic
     * @return the consumeQueueMap of topic
     */
    ConcurrentMap<Integer, ConsumeQueueInterface> findConsumeQueueMap(String topic);

    /**
     * get the total size of all consumeQueue
     * @return the total size of all consumeQueue
     */
    long getTotalSize();

    /**
     * Get store time from commitlog by cqUnit
     * @param cqUnit
     * @return
     */
    long getStoreTime(CqUnit cqUnit);
}
