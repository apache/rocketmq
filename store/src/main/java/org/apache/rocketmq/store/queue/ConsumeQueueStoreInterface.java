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

import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.exception.StoreException;
import org.rocksdb.RocksDBException;

public interface ConsumeQueueStoreInterface {

    /**
     * Load from file.
     * @return true if loaded successfully.
     */
    boolean load();

    /**
     * Recover from file.
     * @param concurrently whether to recover concurrently
     */
    void recover(boolean concurrently) throws RocksDBException;

    /**
     * Get the dispatch offset in consume queue store, messages whose phyOffset larger than this offset need
     * to be dispatched. The dispatch offset only used in recover.
     *
     * @return the dispatch phyOffset
     */
    long getDispatchFromPhyOffset();

    /**
     * Start the consumeQueueStore
     */
    void start();

    /**
     * Used to determine whether to start doDispatch from this commitLog mappedFile
     *
     * @param phyOffset      the offset of the first message in this commitlog mappedFile
     * @param storeTimestamp the timestamp of the first message in this commitlog mappedFile
     * @return whether to start recovering from this MappedFile
     */
    boolean isMappedFileMatchedRecover(long phyOffset, long storeTimestamp,
        boolean recoverNormally) throws RocksDBException;

    /**
     * Shutdown the consumeQueueStore
     * @return true if shutdown successfully.
     */
    boolean shutdown();

    /**
     * destroy all consumeQueues
     * @param loadAfterDestroy reload store after destroy, only used in RocksDB mode
     */
    void destroy(boolean loadAfterDestroy);

    /**
     * delete topic
     */
    boolean deleteTopic(String topic);

    /**
     * Flush all nested consume queues to disk
     *
     * @throws StoreException if there is an error during flush
     */
    void flush() throws StoreException;

    /**
     * clean expired data from minCommitLogOffset
     * @param minCommitLogOffset Minimum commit log offset
     */
    void cleanExpired(long minCommitLogOffset);

    /**
     * Check files.
     */
    void checkSelf();

    /**
     * truncate dirty data
     * @param offsetToTruncate
     * @throws RocksDBException only in rocksdb mode
     */
    void truncateDirty(long offsetToTruncate) throws RocksDBException;

    /**
     * Apply the dispatched request. This function should be idempotent.
     *
     * @param request dispatch request
     * @throws RocksDBException only in rocksdb mode will throw exception
     */
    void putMessagePositionInfoWrapper(DispatchRequest request) throws RocksDBException;

    /**
     * get consumeQueue table
     * @return the consumeQueue table
     */
    ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable();

    /**
     * Assign queue offset.
     * @param msg message itself
     * @throws RocksDBException only in rocksdb mode
     */
    void assignQueueOffset(MessageExtBrokerInner msg) throws RocksDBException;

    /**
     * Increase queue offset.
     * @param msg message itself
     * @param messageNum message number
     */
    void increaseQueueOffset(MessageExtBrokerInner msg, short messageNum);

    /**
     * Increase lmq offset
     * @param topic Topic/Queue name
     * @param queueId Queue ID
     * @param delta amount to increase
     */
    void increaseLmqOffset(String topic, int queueId, short delta) throws ConsumeQueueException;

    /**
     * get lmq queue offset
     * @param topic
     * @param queueId
     * @return
     */
    long getLmqQueueOffset(String topic, int queueId) throws ConsumeQueueException;

    /**
     * recover topicQueue table by minPhyOffset
     * @param minPhyOffset
     */
    void recoverOffsetTable(long minPhyOffset);

    /**
     * get maxOffset of specific topic-queueId in topicQueue table
     *
     * @param topic Topic name
     * @param queueId Queue identifier
     * @return the max offset in QueueOffsetOperator
     * @throws ConsumeQueueException if there is an error while retrieving max consume queue offset
     */
    Long getMaxOffset(String topic, int queueId) throws ConsumeQueueException;

    /**
     * get min logic offset of specific topic-queueId in consumeQueue
     * @param topic
     * @param queueId
     * @return the min logic offset of specific topic-queueId in consumeQueue
     * @throws RocksDBException only in rocksdb mode
     */
    long getMinOffsetInQueue(final String topic, final int queueId) throws RocksDBException;

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
     * only find consumeQueue
     *
     * @param topic
     * @param queueId
     * @return the consumeQueue
     */
    ConsumeQueueInterface getConsumeQueue(String topic, int queueId);

    /**
     * get the total size of all consumeQueue
     * @return the total size of all consumeQueue
     */
    long getTotalSize();

}
