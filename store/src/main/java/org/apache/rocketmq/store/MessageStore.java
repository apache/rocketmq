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
package org.apache.rocketmq.store;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.hook.PutMessageHook;
import org.apache.rocketmq.store.hook.SendMessageBackHook;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.ConsumeQueueStore;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.PerfCounter;

/**
 * This class defines contracting interfaces to implement, allowing third-party vendor to use customized message store.
 */
public interface MessageStore {

    /**
     * Load previously stored messages.
     *
     * @return true if success; false otherwise.
     */
    boolean load();

    /**
     * Launch this message store.
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * Shutdown this message store.
     */
    void shutdown();

    /**
     * Destroy this message store. Generally, all persistent files should be removed after invocation.
     */
    void destroy();

    /**
     * Store a message into store in async manner, the processor can process the next request rather than wait for
     * result when result is completed, notify the client in async manner
     *
     * @param msg MessageInstance to store
     * @return a CompletableFuture for the result of store operation
     */
    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        return CompletableFuture.completedFuture(putMessage(msg));
    }

    /**
     * Store a batch of messages in async manner
     *
     * @param messageExtBatch the message batch
     * @return a CompletableFuture for the result of store operation
     */
    default CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        return CompletableFuture.completedFuture(putMessages(messageExtBatch));
    }

    /**
     * Store a message into store.
     *
     * @param msg Message instance to store
     * @return result of store operation.
     */
    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    /**
     * Store a batch of messages.
     *
     * @param messageExtBatch Message batch.
     * @return result of storing batch messages.
     */
    PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

    /**
     * Query at most <code>maxMsgNums</code> messages belonging to <code>topic</code> at <code>queueId</code> starting
     * from given <code>offset</code>. Resulting messages will further be screened using provided message filter.
     *
     * @param group         Consumer group that launches this query.
     * @param topic         Topic to query.
     * @param queueId       Queue ID to query.
     * @param offset        Logical offset to start from.
     * @param maxMsgNums    Maximum count of messages to query.
     * @param messageFilter Message filter used to screen desired messages.
     * @return Matched messages.
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * Asynchronous get message
     * @see #getMessage(String, String, int, long, int, MessageFilter) getMessage
     *
     * @param group         Consumer group that launches this query.
     * @param topic         Topic to query.
     * @param queueId       Queue ID to query.
     * @param offset        Logical offset to start from.
     * @param maxMsgNums    Maximum count of messages to query.
     * @param messageFilter Message filter used to screen desired messages.
     * @return Matched messages.
     */
    CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * Query at most <code>maxMsgNums</code> messages belonging to <code>topic</code> at <code>queueId</code> starting
     * from given <code>offset</code>. Resulting messages will further be screened using provided message filter.
     *
     * @param group           Consumer group that launches this query.
     * @param topic           Topic to query.
     * @param queueId         Queue ID to query.
     * @param offset          Logical offset to start from.
     * @param maxMsgNums      Maximum count of messages to query.
     * @param maxTotalMsgSize Maximum total msg size of the messages
     * @param messageFilter   Message filter used to screen desired messages.
     * @return Matched messages.
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

    /**
     * Asynchronous get message
     * @see #getMessage(String, String, int, long, int, int, MessageFilter) getMessage
     *
     * @param group           Consumer group that launches this query.
     * @param topic           Topic to query.
     * @param queueId         Queue ID to query.
     * @param offset          Logical offset to start from.
     * @param maxMsgNums      Maximum count of messages to query.
     * @param maxTotalMsgSize Maximum total msg size of the messages
     * @param messageFilter   Message filter used to screen desired messages.
     * @return Matched messages.
     */
    CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId,
        final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

    /**
     * Get maximum offset of the topic queue.
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return Maximum offset at present.
     */
    long getMaxOffsetInQueue(final String topic, final int queueId);

    /**
     * Get maximum offset of the topic queue.
     *
     * @param topic     Topic name.
     * @param queueId   Queue ID.
     * @param committed return the max offset in ConsumeQueue if true, or the max offset in CommitLog if false
     * @return Maximum offset at present.
     */
    long getMaxOffsetInQueue(final String topic, final int queueId, final boolean committed);

    /**
     * Get the minimum offset of the topic queue.
     *
     * @param topic   Topic name.
     * @param queueId Queue ID.
     * @return Minimum offset at present.
     */
    long getMinOffsetInQueue(final String topic, final int queueId);

    TimerMessageStore getTimerMessageStore();

    void setTimerMessageStore(TimerMessageStore timerMessageStore);

    /**
     * Get the offset of the message in the commit log, which is also known as physical offset.
     *
     * @param topic              Topic of the message to lookup.
     * @param queueId            Queue ID.
     * @param consumeQueueOffset offset of consume queue.
     * @return physical offset.
     */
    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * Look up the physical offset of the message whose store timestamp is as specified.
     *
     * @param topic     Topic of the message.
     * @param queueId   Queue ID.
     * @param timestamp Timestamp to look up.
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    /**
     * Look up the physical offset of the message whose store timestamp is as specified with specific boundaryType.
     *
     * @param topic        Topic of the message.
     * @param queueId      Queue ID.
     * @param timestamp    Timestamp to look up.
     * @param boundaryType Lower or Upper
     * @return physical offset which matches.
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp, final BoundaryType boundaryType);

    /**
     * Look up the message by given commit log offset.
     *
     * @param commitLogOffset physical offset.
     * @return Message whose physical offset is as specified.
     */
    MessageExt lookMessageByOffset(final long commitLogOffset);

    /**
     * Look up the message by given commit log offset and size.
     *
     * @param commitLogOffset physical offset.
     * @param size            message size
     * @return Message whose physical offset is as specified.
     */
    MessageExt lookMessageByOffset(long commitLogOffset, int size);

    /**
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    /**
     * Get one message from the specified commit log offset.
     *
     * @param commitLogOffset commit log offset.
     * @param msgSize         message size.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    /**
     * Get the running information of this store.
     *
     * @return message store running info.
     */
    String getRunningDataInfo();

    long getTimingMessageCount(String topic);

    /**
     * Message store runtime information, which should generally contains various statistical information.
     *
     * @return runtime information of the message store in format of key-value pairs.
     */
    HashMap<String, String> getRuntimeInfo();

    /**
     * HA runtime information
     * @return runtime information of ha
     */
    HARuntimeInfo getHARuntimeInfo();

    /**
     * Get the maximum commit log offset.
     *
     * @return maximum commit log offset.
     */
    long getMaxPhyOffset();

    /**
     * Get the minimum commit log offset.
     *
     * @return minimum commit log offset.
     */
    long getMinPhyOffset();

    /**
     * Get the store time of the earliest message in the given queue.
     *
     * @param topic   Topic of the messages to query.
     * @param queueId Queue ID to find.
     * @return store time of the earliest message.
     */
    long getEarliestMessageTime(final String topic, final int queueId);

    /**
     * Get the store time of the earliest message in this store.
     *
     * @return timestamp of the earliest message in this store.
     */
    long getEarliestMessageTime();

    /**
     * Asynchronous get the store time of the earliest message in this store.
     * @see #getEarliestMessageTime() getEarliestMessageTime
     *
     * @return timestamp of the earliest message in this store.
     */
    CompletableFuture<Long> getEarliestMessageTimeAsync(final String topic, final int queueId);

    /**
     * Get the store time of the message specified.
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     */
    long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * Asynchronous get the store time of the message specified.
     * @see #getMessageStoreTimeStamp(String, int, long) getMessageStoreTimeStamp
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     */
    CompletableFuture<Long> getMessageStoreTimeStampAsync(final String topic, final int queueId,
        final long consumeQueueOffset);

    /**
     * Get the total number of the messages in the specified queue.
     *
     * @param topic   Topic
     * @param queueId Queue ID.
     * @return total number.
     */
    long getMessageTotalInQueue(final String topic, final int queueId);

    /**
     * Get the raw commit log data starting from the given offset, which should used for replication purpose.
     *
     * @param offset starting offset.
     * @return commit log data.
     */
    SelectMappedBufferResult getCommitLogData(final long offset);

    /**
     * Get the raw commit log data starting from the given offset, across multiple mapped files.
     *
     * @param offset starting offset.
     * @param size   size of data to get
     * @return commit log data.
     */
    List<SelectMappedBufferResult> getBulkCommitLogData(final long offset, final int size);

    /**
     * Append data to commit log.
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @param dataStart   the start index of data array
     * @param dataLength  the length of data array
     * @return true if success; false otherwise.
     */
    boolean appendToCommitLog(final long startOffset, final byte[] data, int dataStart, int dataLength);

    /**
     * Execute file deletion manually.
     */
    void executeDeleteFilesManually();

    /**
     * Query messages by given key.
     *
     * @param topic  topic of the message.
     * @param key    message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin  begin timestamp.
     * @param end    end timestamp.
     */
    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
        final long end);

    /**
     * Asynchronous query messages by given key.
     * @see #queryMessage(String, String, int, long, long) queryMessage
     *
     * @param topic  topic of the message.
     * @param key    message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin  begin timestamp.
     * @param end    end timestamp.
     */
    CompletableFuture<QueryMessageResult> queryMessageAsync(final String topic, final String key, final int maxNum,
        final long begin, final long end);

    /**
     * Update HA master address.
     *
     * @param newAddr new address.
     */
    void updateHaMasterAddress(final String newAddr);

    /**
     * Update master address.
     *
     * @param newAddr new address.
     */
    void updateMasterAddress(final String newAddr);

    /**
     * Return how much the slave falls behind.
     *
     * @return number of bytes that slave falls behind.
     */
    long slaveFallBehindMuch();

    /**
     * Return the current timestamp of the store.
     *
     * @return current time in milliseconds since 1970-01-01.
     */
    long now();

    /**
     * Delete topic's consume queue file and unused stats.
     * This interface allows user delete system topic.
     *
     * @param deleteTopics unused topic name set
     * @return the number of the topics which has been deleted.
     */
    int deleteTopics(final Set<String> deleteTopics);

    /**
     * Clean unused topics which not in retain topic name set.
     *
     * @param retainTopics all valid topics.
     * @return number of the topics deleted.
     */
    int cleanUnusedTopic(final Set<String> retainTopics);

    /**
     * Clean expired consume queues.
     */
    void cleanExpiredConsumerQueue();

    /**
     * Check if the given message has been swapped out of the memory.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is no longer in memory; false otherwise.
     * @deprecated As of RIP-57, replaced by {@link #checkInMemByConsumeOffset(String, int, long, int)}, see <a href="https://github.com/apache/rocketmq/issues/5837">this issue</a> for more details
     */
    @Deprecated
    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * Check if the given message is in the page cache.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is in page cache; false otherwise.
     */
    boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize);

    /**
     * Check if the given message is in store.
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is in store; false otherwise.
     */
    boolean checkInStoreByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * Get number of the bytes that have been stored in commit log and not yet dispatched to consume queue.
     *
     * @return number of the bytes to dispatch.
     */
    long dispatchBehindBytes();

    /**
     * Flush the message store to persist all data.
     *
     * @return maximum offset flushed to persistent storage device.
     */
    long flush();

    /**
     * Get the current flushed offset.
     *
     * @return flushed offset
     */
    long getFlushedWhere();

    /**
     * Reset written offset.
     *
     * @param phyOffset new offset.
     * @return true if success; false otherwise.
     */
    boolean resetWriteOffset(long phyOffset);

    /**
     * Get confirm offset.
     *
     * @return confirm offset.
     */
    long getConfirmOffset();

    /**
     * Set confirm offset.
     *
     * @param phyOffset confirm offset to set.
     */
    void setConfirmOffset(long phyOffset);

    /**
     * Check if the operation system page cache is busy or not.
     *
     * @return true if the OS page cache is busy; false otherwise.
     */
    boolean isOSPageCacheBusy();

    /**
     * Get lock time in milliseconds of the store by far.
     *
     * @return lock time in milliseconds.
     */
    long lockTimeMills();

    /**
     * Check if the transient store pool is deficient.
     *
     * @return true if the transient store pool is running out; false otherwise.
     */
    boolean isTransientStorePoolDeficient();

    /**
     * Get the dispatcher list.
     *
     * @return list of the dispatcher.
     */
    LinkedList<CommitLogDispatcher> getDispatcherList();

    /**
     * Add dispatcher.
     *
     * @param dispatcher commit log dispatcher to add
     */
    void addDispatcher(CommitLogDispatcher dispatcher);

    /**
     * Get consume queue of the topic/queue. If consume queue not exist, will return null
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueueInterface getConsumeQueue(String topic, int queueId);

    /**
     * Get consume queue of the topic/queue. If consume queue not exist, will create one then return it.
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueueInterface findConsumeQueue(String topic, int queueId);

    /**
     * Get BrokerStatsManager of the messageStore.
     *
     * @return BrokerStatsManager.
     */
    BrokerStatsManager getBrokerStatsManager();

    /**
     * Will be triggered when a new message is appended to commit log.
     *
     * @param msg           the msg that is appended to commit log
     * @param result        append message result
     * @param commitLogFile commit log file
     */
    void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile);

    /**
     * Will be triggered when a new dispatch request is sent to message store.
     *
     * @param dispatchRequest dispatch request
     * @param doDispatch      do dispatch if true
     * @param commitLogFile   commit log file
     * @param isRecover       is from recover process
     * @param isFileEnd       if the dispatch request represents 'file end'
     */
    void onCommitLogDispatch(DispatchRequest dispatchRequest, boolean doDispatch, MappedFile commitLogFile,
        boolean isRecover, boolean isFileEnd);

    /**
     * Get the message store config
     *
     * @return the message store config
     */
    MessageStoreConfig getMessageStoreConfig();

    /**
     * Get the statistics service
     *
     * @return the statistics service
     */
    StoreStatsService getStoreStatsService();

    /**
     * Get the store checkpoint component
     *
     * @return the checkpoint component
     */
    StoreCheckpoint getStoreCheckpoint();

    /**
     * Get the system clock
     *
     * @return the system clock
     */
    SystemClock getSystemClock();

    /**
     * Get the commit log
     *
     * @return the commit log
     */
    CommitLog getCommitLog();

    /**
     * Get running flags
     *
     * @return running flags
     */
    RunningFlags getRunningFlags();

    /**
     * Get the transient store pool
     *
     * @return the transient store pool
     */
    TransientStorePool getTransientStorePool();

    /**
     * Get the HA service
     *
     * @return the HA service
     */
    HAService getHaService();

    /**
     * Get the allocate-mappedFile service
     *
     * @return the allocate-mappedFile service
     */
    AllocateMappedFileService getAllocateMappedFileService();

    /**
     * Truncate dirty logic files
     *
     * @param phyOffset physical offset
     */
    void truncateDirtyLogicFiles(long phyOffset);

    /**
     * Destroy logics files
     */
    void destroyLogics();

    /**
     * Unlock mappedFile
     *
     * @param unlockMappedFile the file that needs to be unlocked
     */
    void unlockMappedFile(MappedFile unlockMappedFile);

    /**
     * Get the perf counter component
     *
     * @return the perf counter component
     */
    PerfCounter.Ticks getPerfCounter();

    /**
     * Get the queue store
     *
     * @return the queue store
     */
    ConsumeQueueStore getQueueStore();

    /**
     * If 'sync disk flush' is configured in this message store
     *
     * @return yes if true, no if false
     */
    boolean isSyncDiskFlush();

    /**
     * If this message store is sync master role
     *
     * @return yes if true, no if false
     */
    boolean isSyncMaster();

    /**
     * Assign a message to queue offset. If there is a race condition, you need to lock/unlock this method
     * yourself.
     *
     * @param msg        message
     */
    void assignOffset(MessageExtBrokerInner msg);

    /**
     * Increase queue offset in memory table. If there is a race condition, you need to lock/unlock this method
     *
     * @param msg        message
     * @param messageNum message num
     */
    void increaseOffset(MessageExtBrokerInner msg, short messageNum);

    /**
     * Get master broker message store in process in broker container
     *
     * @return
     */
    MessageStore getMasterStoreInProcess();

    /**
     * Set master broker message store in process
     *
     * @param masterStoreInProcess
     */
    void setMasterStoreInProcess(MessageStore masterStoreInProcess);

    /**
     * Use FileChannel to get data
     *
     * @param offset
     * @param size
     * @param byteBuffer
     * @return
     */
    boolean getData(long offset, int size, ByteBuffer byteBuffer);

    /**
     * Set the number of alive replicas in group.
     *
     * @param aliveReplicaNums number of alive replicas
     */
    void setAliveReplicaNumInGroup(int aliveReplicaNums);

    /**
     * Get the number of alive replicas in group.
     *
     * @return number of alive replicas
     */
    int getAliveReplicaNumInGroup();

    /**
     * Wake up AutoRecoverHAClient to start HA connection.
     */
    void wakeupHAClient();

    /**
     * Get master flushed offset.
     *
     * @return master flushed offset
     */
    long getMasterFlushedOffset();

    /**
     * Get broker init max offset.
     *
     * @return broker max offset in startup
     */
    long getBrokerInitMaxOffset();

    /**
     * Set master flushed offset.
     *
     * @param masterFlushedOffset master flushed offset
     */
    void setMasterFlushedOffset(long masterFlushedOffset);

    /**
     * Set broker init max offset.
     *
     * @param brokerInitMaxOffset broker init max offset
     */
    void setBrokerInitMaxOffset(long brokerInitMaxOffset);

    /**
     * Calculate the checksum of a certain range of data.
     *
     * @param from begin offset
     * @param to   end offset
     * @return checksum
     */
    byte[] calcDeltaChecksum(long from, long to);

    /**
     * Truncate commitLog and consume queue to certain offset.
     *
     * @param offsetToTruncate offset to truncate
     * @return true if truncate succeed, false otherwise
     */
    boolean truncateFiles(long offsetToTruncate);

    /**
     * Check if the offset is align with one message.
     *
     * @param offset offset to check
     * @return true if align, false otherwise
     */
    boolean isOffsetAligned(long offset);

    /**
     * Get put message hook list
     *
     * @return List of PutMessageHook
     */
    List<PutMessageHook> getPutMessageHookList();

    /**
     * Set send message back hook
     *
     * @param sendMessageBackHook
     */
    void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook);

    /**
     * Get send message back hook
     *
     * @return SendMessageBackHook
     */
    SendMessageBackHook getSendMessageBackHook();

    //The following interfaces are used for duplication mode

    /**
     * Get last mapped file and return lase file first Offset
     *
     * @return lastMappedFile first Offset
     */
    long getLastFileFromOffset();

    /**
     * Get last mapped file
     *
     * @param startOffset
     * @return true when get the last mapped file, false when get null
     */
    boolean getLastMappedFile(long startOffset);

    /**
     * Set physical offset
     *
     * @param phyOffset
     */
    void setPhysicalOffset(long phyOffset);

    /**
     * Return whether mapped file is empty
     *
     * @return whether mapped file is empty
     */
    boolean isMappedFilesEmpty();

    /**
     * Get state machine version
     *
     * @return state machine version
     */
    long getStateMachineVersion();

    /**
     * Check message and return size
     *
     * @param byteBuffer
     * @param checkCRC
     * @param checkDupInfo
     * @param readBody
     * @return DispatchRequest
     */
    DispatchRequest checkMessageAndReturnSize(final ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean checkDupInfo, final boolean readBody);

    /**
     * Get remain transientStoreBuffer numbers
     *
     * @return remain transientStoreBuffer numbers
     */
    int remainTransientStoreBufferNumbs();

    /**
     * Get remain how many data to commit
     *
     * @return remain how many data to commit
     */
    long remainHowManyDataToCommit();

    /**
     * Get remain how many data to flush
     *
     * @return remain how many data to flush
     */
    long remainHowManyDataToFlush();

    /**
     * Get whether message store is shutdown
     *
     * @return whether shutdown
     */
    boolean isShutdown();

    /**
     * Estimate number of messages, within [from, to], which match given filter
     *
     * @param topic   Topic name
     * @param queueId Queue ID
     * @param from    Lower boundary of the range, inclusive.
     * @param to      Upper boundary of the range, inclusive.
     * @param filter  The message filter.
     * @return Estimate number of messages matching given filter.
     */
    long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter);

    /**
     * Get metrics view of store
     *
     * @return List of metrics selector and view pair
     */
    List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView();

    /**
     * Init store metrics
     *
     * @param meter                     opentelemetry meter
     * @param attributesBuilderSupplier metrics attributes builder
     */
    void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier);
}
