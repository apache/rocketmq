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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * File based Ack buffer merge service.
 *
 * <p>buffer checkpoint in memory then enqueue them into system revive queue then wait to be acked.
 *
 * <p>Two in-memory data structures drive the merge logic:
 * <ul>
 *   <li>{@link #buffer} — maps {@code mergeKey} to {@link PopCheckPointWrapper},
 *       tracking which sub-messages within a CK batch have been acked
 *       (via {@code bits} bitmask) and which have been persisted
 *       (via {@code toStoreBits} bitmask)</li>
 *   <li>{@link #commitOffsets} — maps {@code topic@cid@queueId} to an ordered
 *       queue of {@link PopCheckPointWrapper}s for sequential offset committing</li>
 * </ul>
 *
 * <p>The background {@link #scan()} thread periodically evaluates each buffered CK:
 * <ul>
 *   <li><b>All acks received</b> — removes the CK from the buffer without writing
 *       anything to storage (clean completion)</li>
 *   <li><b>About to expire</b> ({@code reviveTime - now < popCkStayBufferTimeOut})
 *       or <b>stayed too long</b> — writes the CK and all un-persisted acks
 *       (or batch acks) to the revive topic</li>
 * </ul>
 *
 * <p>This service is enabled by {@code enablePopBufferMerge} and only runs on
 * a master or a slave acting as master. When {@code enablePopBatchAck} is set,
 * multiple ack offsets are packed into a single {@link BatchAckMsg}.
 */
public class PopBufferMergeService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    /**
     * In-memory map of check points.
     *  Key: topic + group + queueId + startOffset + popTime + brokerName
     *  Value: check point wrapper
     * use cases:
     * - scan: iterate buffer
     * - addAckMsg: get check point from buffer and mark ack state of Check Point
     */
    ConcurrentHashMap<String/*mergeKey*/, PopCheckPointWrapper>
        buffer = new ConcurrentHashMap<>(1024 * 16);
    /**
     * manage check point of given consumer and given queue
     *   Key: topic@cid@queueId
     *   Value: check point queue of specific consumer and queue
     * use cases:
     * - getLatestOffset: get consumer next start offset of given queue
     * - scanGarbage
     * - getOffsetTotalSize: get total popping num
     * - isQueueFull
     */
    ConcurrentHashMap<String/*topic@cid@queueId*/, QueueWithTime<PopCheckPointWrapper>> commitOffsets =
        new ConcurrentHashMap<>();
    private volatile boolean serving = true;
    private AtomicInteger counter = new AtomicInteger(0);
    private int scanTimes = 0;
    private final BrokerController brokerController;
    private final PopMessageProcessor popMessageProcessor;
    private final PopMessageProcessor.QueueLockManager queueLockManager;
    private final long interval = 5;
    private final long minute5 = 5 * 60 * 1000;
    private final int countOfMinute1 = (int) (60 * 1000 / interval);
    private final int countOfSecond1 = (int) (1000 / interval);
    private final int countOfSecond30 = (int) (30 * 1000 / interval);

    private final List<Byte> batchAckIndexList = new ArrayList<>(32);
    private volatile boolean master = false;

    public PopBufferMergeService(BrokerController brokerController, PopMessageProcessor popMessageProcessor) {
        this.brokerController = brokerController;
        this.popMessageProcessor = popMessageProcessor;
        this.queueLockManager = popMessageProcessor.getQueueLockManager();
    }

    private boolean isShouldRunning() {
        if (this.brokerController.getBrokerConfig().isEnableSlaveActingMaster()) {
            return true;
        }
        this.master = brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        return this.master;
    }

    @Override
    public String getServiceName() {
        if (this.brokerController != null && this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + PopBufferMergeService.class.getSimpleName();
        }
        return PopBufferMergeService.class.getSimpleName();
    }

    @Override
    public void run() {
        // scan
        while (!this.isStopped()) {
            try {
                // env check
                if (!isShouldRunning()) {
                    // slave
                    this.waitForRunning(interval * 200 * 5);
                    POP_LOGGER.info("Broker is {}, {}, clear all data",
                        brokerController.getMessageStoreConfig().getBrokerRole(), this.master);
                    this.buffer.clear();
                    this.commitOffsets.clear();
                    continue;
                }

                scan();
                if (scanTimes % countOfSecond30 == 0) {
                    // remove checkpoint which are timeout
                    scanGarbage();
                }

                // waiting
                this.waitForRunning(interval);
                if (!this.serving && this.buffer.size() == 0 && getOffsetTotalSize() == 0) {
                    this.serving = true;
                }
            } catch (Throwable e) {
                POP_LOGGER.error("PopBufferMergeService error", e);
                this.waitForRunning(3000);
            }
        }

        // scan until buffer is empty
        this.serving = false;
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        if (!isShouldRunning()) {
            return;
        }
        if (!brokerController.getBrokerConfig().isInBrokerContainer()) {
            while (this.buffer.size() > 0 || getOffsetTotalSize() > 0) {
                scan();
            }
        }
    }

    /**
     * Drain the {@link #commitOffsets} queues and commit consumer offsets in FIFO order.
     * scanAndCommitOffset may be a better name
     *
     * <p>For each {@code topic@cid@queueId} queue, the method peeks the head (oldest)
     * wrapper and checks whether it is ready to commit:
     * <ul>
     *   <li>Just-offset entry with CK stored</li>
     *   <li>All sub-messages acked ({@link #isCkDone})</li>
     *   <li>All acks persisted and CK stored ({@link #isCkDoneForFinish})</li>
     * </ul>
     *
     * <p>If the head is ready, it is committed and removed. Processing continues
     * to the next wrapper in the same queue. If the head is not ready, the loop
     * breaks — this ensures <b>strict FIFO order</b> and prevents consumer offset
     * regression.
     *
     * <p>Called at the end of {@link #scan()} after the buffer has been processed.
     *
     * @return the total number of remaining wrappers across all queues (for logging)
     */
    private int scanCommitOffset() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            PopCheckPointWrapper pointWrapper;
            while ((pointWrapper = queue.peek()) != null) {
                // 1. just offset & stored, not processed by scan
                // 2. ck is buffer(acked)
                // 3. ck is buffer(not all acked), all ak are stored and ck is stored
                if (pointWrapper.isJustOffset() && pointWrapper.isCkStored() || isCkDone(pointWrapper)
                    || isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                    if (commitOffset(pointWrapper)) {
                        queue.poll();
                    } else {
                        break;
                    }
                } else {
                    if (System.currentTimeMillis() - pointWrapper.getCk().getPopTime()
                        > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2) {
                        POP_LOGGER.warn("[PopBuffer] ck offset long time not commit, {}", pointWrapper);
                    }
                    break;
                }
            }
            final int qs = queue.size();
            count += qs;
            if (qs > 5000 && scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer] offset queue size too long, {}, {}",
                    entry.getKey(), qs);
            }
        }
        return count;
    }

    public long getLatestOffset(String lockKey) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(lockKey);
        if (queue == null) {
            return -1;
        }
        PopCheckPointWrapper pointWrapper = queue.get().peekLast();
        if (pointWrapper != null) {
            return pointWrapper.getNextBeginOffset();
        }
        return -1;
    }

    public long getLatestOffset(String topic, String group, int queueId) {
        return getLatestOffset(KeyBuilder.buildPollingKey(topic, group, queueId));
    }

    /**
     * Remove stale entries from {@link #commitOffsets}.
     *
     * <p>Three types of entries are removed:
     * <ul>
     *   <li>Topic no longer exists (deleted)</li>
     *   <li>Consumer group no longer exists (unsubscribed)</li>
     *   <li>No activity for more than 5 minutes (idle)</li>
     * </ul>
     */
    private void scanGarbage() {
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            // validate checkpoint
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            if (entry.getKey() == null) {
                continue;
            }
            String[] keyArray = entry.getKey().split(PopAckConstants.SPLIT);
            if (keyArray == null || keyArray.length != 3) {
                continue;
            }
            String topic = keyArray[0];
            String cid = keyArray[1];

            // remove if topic no longer exists
            if (brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
                POP_LOGGER.info("[PopBuffer]remove nonexistent topic {} in buffer!", topic);
                iterator.remove();
                continue;
            }

            // remove if subscription group no longer exists
            if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(cid)) {
                POP_LOGGER.info("[PopBuffer]remove nonexistent subscription group {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }

            // remove if idle
            // entry.getValue().getTime() = popTime of last checkpoint enqueued in the queue
            if (System.currentTimeMillis() - entry.getValue().getTime() > minute5) {
                POP_LOGGER.info("[PopBuffer]remove long time not used sub {} of topic {} in buffer!", cid, topic);
                iterator.remove();
                continue;
            }
        }
    }

    private boolean isSubscriptionGroupNotExist(PopCheckPointWrapper pointWrapper) {
        String group = pointWrapper.getCk().getCId();
        return brokerController.getSubscriptionGroupManager()
                .findSubscriptionGroupConfig(group) == null;
    }


    /**
     * Scan and process all buffered checkpoints, then drain the offset commit queue.
     *
     * <p>For each entry in {@link #buffer}:
     * <ul>
     *   <li><b>Consumer group not found</b> — removes the entry silently</li>
     *   <li><b>CK done</b> (all sub-messages acked) — removes from buffer, no store write needed</li>
     *   <li><b>Just-offset entry</b> — writes the CK to the revive topic if not yet stored</li>
     *   <li><b>Needs eviction</b> (service stopped, revive timeout, or stay timeout) —
     *       writes the CK and all un-persisted acks (batch or individual) to the revive topic,
     *       then removes the entry when all persisted</li>
     *   <li><b>Otherwise</b> — leaves the entry in the buffer for the next scan cycle</li>
     * </ul>
     *
     * <p>After processing the buffer, calls {@link #scanCommitOffset()} to commit offsets
     * for finished checkpoints in FIFO order.
     *
     * <p>If the scan duration exceeds {@code popCkStayBufferTimeOut - 1000ms}, the service
     * temporarily stops accepting new CKs ({@link #serving} = false) to avoid backlog.
     */
    private void scan() {
        long startTime = System.currentTimeMillis();
        AtomicInteger count = new AtomicInteger(0);
        int countCk = 0;
        Iterator<Map.Entry<String, PopCheckPointWrapper>> iterator = buffer.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, PopCheckPointWrapper> entry = iterator.next();
            PopCheckPointWrapper pointWrapper = entry.getValue();

            // Skip invalid POP records when consumer group does not exist
            if (isSubscriptionGroupNotExist(pointWrapper)) {
                POP_LOGGER.warn(
                        "[PopBuffer] skip pop record because consumer group not exist, group={}, ck={}",
                        pointWrapper.getCk().getCId(),
                        pointWrapper
                );
                iterator.remove();
                counter.decrementAndGet();
                continue;
            }

            // just process offset(already stored at pull thread), or buffer ck(not stored and ack finish)
            if (pointWrapper.isJustOffset() && pointWrapper.isCkStored() || isCkDone(pointWrapper)
                || isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("[PopBuffer]ck done, {}", pointWrapper);
                }
                iterator.remove();
                counter.decrementAndGet();
                continue;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            // check whether check point is timeout
            boolean removeCk = !this.serving;
            // ck will be timeout
            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut()) {
                removeCk = true;
            }

            // the time stayed is too long
            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime()) {
                removeCk = true;
            }

            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() * 2L) {
                POP_LOGGER.warn("[PopBuffer]ck finish fail, stay too long, {}", pointWrapper);
            }

            // double check
            if (isCkDone(pointWrapper)) { // all checkpoint are acked, do nothing
                continue;
            } else if (pointWrapper.isJustOffset()) { // store checkpoint
                // just offset should be in store.
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    putCkToStore(pointWrapper, this.brokerController.getBrokerConfig().isAppendCkAsync());
                    countCk++;
                }
                continue;
            } else if (removeCk) { // store checkpoint if needed
                // put buffer ak to store
                // revive queue offset < 0 means checkpoint was not stored
                if (pointWrapper.getReviveQueueOffset() < 0) {
                    putCkToStore(pointWrapper, this.brokerController.getBrokerConfig().isAppendCkAsync());
                    countCk++;
                }

                if (!pointWrapper.isCkStored()) {
                    continue;
                }

                // store checkpoint
                if (brokerController.getBrokerConfig().isEnablePopBatchAck()) { // default is false
                    List<Byte> indexList = this.batchAckIndexList;
                    try {
                        for (byte i = 0; i < point.getNum(); i++) {
                            // reput buffer ak to store
                            // if checkpoint is acked and not stored, add to indexList
                            if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                                && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                                indexList.add(i);
                            }
                        }
                        if (indexList.size() > 0) {
                            putBatchAckToStore(pointWrapper, indexList, count);
                        }
                    } finally {
                        indexList.clear();
                    }
                } else {
                    for (byte i = 0; i < point.getNum(); i++) {
                        // reput buffer ak to store
                        // if checkpoint is acked and not stored, call putAckToStore
                        if (DataConverter.getBit(pointWrapper.getBits().get(), i)
                            && !DataConverter.getBit(pointWrapper.getToStoreBits().get(), i)) {
                            putAckToStore(pointWrapper, i, count);
                        }
                    }
                }

                // remove checkpoint from buffer
                if (isCkDoneForFinish(pointWrapper) && pointWrapper.isCkStored()) {
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("[PopBuffer]ck finish, {}", pointWrapper);
                    }
                    iterator.remove();
                    counter.decrementAndGet();
                }
            }
        }

        // scan commitOffsets and commit offset which is needed.
        int offsetBufferSize = scanCommitOffset();

        // calculate scan times
        long eclipse = System.currentTimeMillis() - startTime;
        if (eclipse > brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() - 1000) {
            POP_LOGGER.warn("[PopBuffer]scan stop, because eclipse too long, PopBufferEclipse={}, " +
                    "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                eclipse, count.get(), countCk, counter.get(), offsetBufferSize);
            this.serving = false;
        } else {
            if (scanTimes % countOfSecond1 == 0) {
                POP_LOGGER.info("[PopBuffer]scan, PopBufferEclipse={}, " +
                        "PopBufferToStoreAck={}, PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                    eclipse, count.get(), countCk, counter.get(), offsetBufferSize);
            }
        }
        brokerController.getBrokerMetricsManager().getPopMetricsManager().recordPopBufferScanTimeConsume(eclipse);
        scanTimes++;

        if (scanTimes >= countOfMinute1) {
            counter.set(this.buffer.size());
            scanTimes = 0;
        }
    }

    public int getOffsetTotalSize() {
        int count = 0;
        Iterator<Map.Entry<String, QueueWithTime<PopCheckPointWrapper>>> iterator = this.commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, QueueWithTime<PopCheckPointWrapper>> entry = iterator.next();
            LinkedBlockingDeque<PopCheckPointWrapper> queue = entry.getValue().get();
            count += queue.size();
        }
        return count;
    }

    public int getBufferedCKSize() {
        return this.counter.get();
    }

    /**
     * Atomically set the bit at {@code index} in an {@link AtomicInteger} bitmask.
     *
     * <p>Uses a CAS (compare-and-swap) loop to ensure thread safety without locking.
     * If the bit is already set, this method returns immediately (no-op).
     *
     * @param setBits the atomic bitmask to update
     * @param index   the bit position (0-based)
     */
    private void markBitCAS(AtomicInteger setBits, int index) {
        while (true) {
            int bits = setBits.get();
            if (DataConverter.getBit(bits, index)) {
                break;
            }

            int newBits = DataConverter.setBit(bits, index, true);
            if (setBits.compareAndSet(bits, newBits)) {
                break;
            }
        }
    }

    /**
     * Commit the consumer offset for the checkpoint's {@code topic@cid@queueId}.
     *
     * <p>Called from {@link #scanCommitOffset()} after the checkpoint is confirmed
     * as finished (all acks received or CK stored). The offset is advanced to
     * {@link PopCheckPointWrapper#nextBeginOffset}, which is the offset of the
     * first message after this batch.
     *
     * <p>The operation is guarded by {@link PopMessageProcessor.QueueLockManager}
     * to prevent concurrent offset updates on the same queue.
     *
     * @param wrapper the finished checkpoint wrapper
     * @return {@code true} if the offset was committed or no commit is needed
     *         ({@code nextBeginOffset < 0}); {@code false} if the lock could
     *         not be acquired (caller should retry later)
     */
    private boolean commitOffset(final PopCheckPointWrapper wrapper) {
        if (wrapper.getNextBeginOffset() < 0) {
            return true;
        }

        final PopCheckPoint popCheckPoint = wrapper.getCk();
        final String lockKey = wrapper.getLockKey();

        if (!queueLockManager.tryLock(lockKey)) {
            return false;
        }
        try {
            final long offset = brokerController.getConsumerOffsetManager().queryOffset(popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId());
            if (wrapper.getNextBeginOffset() > offset) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.info("Commit offset, {}, {}", wrapper, offset);
                }
            } else {
                // maybe store offset is not correct.
                POP_LOGGER.warn("Commit offset, consumer offset less than store, {}, {}", wrapper, offset);
            }
            brokerController.getConsumerOffsetManager().commitOffset(getServiceName(),
                popCheckPoint.getCId(), popCheckPoint.getTopic(), popCheckPoint.getQueueId(), wrapper.getNextBeginOffset());
        } finally {
            queueLockManager.unLock(lockKey);
        }
        return true;
    }

    /**
     * Enqueue the checkpoint wrapper into the per-{@code topic@cid@queueId} offset queue
     * for sequential offset committing.
     *
     * <p>The queue is maintained in FIFO order. The {@link #scanCommitOffset()} method
     * drains the queue from the head, ensuring that offsets are committed in the same
     * order as the checkpoints were created, which prevents consumer offset regression.
     *
     * <p>The {@link QueueWithTime#time} is also updated to the CK's pop time so that
     * {@link #scanGarbage()} can identify and remove stale entries after 5 minutes of
     * inactivity.
     *
     * @param pointWrapper the checkpoint wrapper to enqueue
     * @return true if the element was added to the queue successfully
     */
    private boolean putOffsetQueue(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());

        // init with empty queue
        if (queue == null) {
            queue = new QueueWithTime<>();
            QueueWithTime old = this.commitOffsets.putIfAbsent(pointWrapper.getLockKey(), queue);
            if (old != null) {
                queue = old;
            }
        }

        queue.setTime(pointWrapper.getCk().getPopTime());
        return queue.get().offer(pointWrapper);
    }

    private boolean checkQueueOk(PopCheckPointWrapper pointWrapper) {
        QueueWithTime<PopCheckPointWrapper> queue = this.commitOffsets.get(pointWrapper.getLockKey());
        if (queue == null) {
            return true;
        }
        return queue.get().size() < brokerController.getBrokerConfig().getPopCkOffsetMaxQueueSize();
    }

    /**
     * put to store && add to buffer.
     * addAndStoreCheckpoint maybe a better name.
     *
     * @param point check point
     * @param reviveQueueId revive queueId
     * @param reviveQueueOffset revive queueOffset
     * @param nextBeginOffset next offset
     * @return true if success
     */
    public boolean addCkJustOffset(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset,
        long nextBeginOffset) {
        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset, true);

        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ckJustOffset. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }

        // called before buffer operation
        // because store operation will update attributes of pointWrapper
        this.putCkToStore(pointWrapper, checkQueueOk(pointWrapper));

        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, {}", pointWrapper);
        }
        return true;
    }

    /**
     * mock checkpoint then add it to offset queue.
     * this method is called when popped message is:
     * - NO_MATCHED_MESSAGE
     * - OFFSET_FOUND_NULL
     * - MESSAGE_WAS_REMOVING
     * - NO_MATCHED_LOGIC_QUEUE
     */
    public void addCkMock(String group, String topic, int queueId, long startOffset, long invisibleTime,
        long popTime, int reviveQueueId, long nextBeginOffset, String brokerName) {
        // create checkpoint
        final PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) 0);
        ck.setPopTime(popTime);
        ck.setInvisibleTime(invisibleTime);
        ck.setStartOffset(startOffset);
        ck.setCId(group);
        ck.setTopic(topic);
        ck.setQueueId(queueId);
        ck.setBrokerName(brokerName);

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, Long.MAX_VALUE, ck, nextBeginOffset, true);
        pointWrapper.setCkStored(true);

        putOffsetQueue(pointWrapper);

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck just offset, mocked, {}", pointWrapper);
        }
    }

    /**
     * add checkpoint to buffer.
     */
    public boolean addCk(PopCheckPoint point, int reviveQueueId, long reviveQueueOffset, long nextBeginOffset) {
        // validate env and checkpoint
        // key: point.getT() + point.getC() + point.getQ() + point.getSo() + point.getPt()
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.warn("[PopBuffer]add ck, timeout, {}, {}", point, now);
            }
            return false;
        }

        if (this.counter.get() > brokerController.getBrokerConfig().getPopCkMaxBufferSize()) {
            POP_LOGGER.warn("[PopBuffer]add ck, max size, {}, {}", point, this.counter.get());
            return false;
        }

        PopCheckPointWrapper pointWrapper = new PopCheckPointWrapper(reviveQueueId, reviveQueueOffset, point, nextBeginOffset);

        if (!checkQueueOk(pointWrapper)) {
            return false;
        }

        if (this.buffer.containsKey(pointWrapper.getMergeKey())) {
            // when mergeKey conflict
            // will cause PopBufferMergeService.scanCommitOffset cannot poll PopCheckPointWrapper
            POP_LOGGER.warn("[PopBuffer]mergeKey conflict when add ck. ck:{}, mergeKey:{}", pointWrapper, pointWrapper.getMergeKey());
            return false;
        }

        putOffsetQueue(pointWrapper);
        this.buffer.put(pointWrapper.getMergeKey(), pointWrapper);
        this.counter.incrementAndGet();
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]add ck, {}", pointWrapper);
        }
        return true;
    }

    /**
     * Merge a consumer ack into the buffered checkpoint.
     *
     * <p>The ack is not written to the revive topic immediately. Instead, a flag is
     * set in {@link PopCheckPointWrapper#bits} via {@link #markBitCAS}.
     * The pending ack will later be flushed to storage by {@link #scan()} when the
     * checkpoint is evicted (timeout / buffer full / service stopping).
     *
     * <p>Rejection conditions (return false):
     * <ul>
     *   <li>{@code enablePopBufferMerge} is disabled</li>
     *   <li>The service is not serving (too busy)</li>
     *   <li>No matching checkpoint found in {@link #buffer}</li>
     *   <li>The checkpoint is a {@code justOffset} entry (no messages to ack)</li>
     *   <li>The checkpoint is too close to its revive deadline</li>
     *   <li>The checkpoint has been buffered for too long</li>
     * </ul>
     *
     * @param reviveQid revive queue id (used only for logging)
     * @param ackMsg    the ack message from the consumer
     * @return true if the ack was merged successfully
     */
    public boolean addAk(int reviveQid, AckMsg ackMsg) {
        // validate env
        if (!brokerController.getBrokerConfig().isEnablePopBufferMerge()) {
            return false;
        }
        if (!serving) {
            return false;
        }

        try {
            // get and validate checkpoint
            PopCheckPointWrapper pointWrapper = this.buffer.get(ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + ackMsg.getBrokerName());
            if (pointWrapper == null) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, no ck, {}", reviveQid, ackMsg);
                }
                return false;
            }

            if (pointWrapper.isJustOffset()) {
                return false;
            }

            PopCheckPoint point = pointWrapper.getCk();
            long now = System.currentTimeMillis();

            if (point.getReviveTime() - now < brokerController.getBrokerConfig().getPopCkStayBufferTimeOut() + 1500) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, almost timeout for revive, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            if (now - point.getPopTime() > brokerController.getBrokerConfig().getPopCkStayBufferTime() - 1500) {
                if (brokerController.getBrokerConfig().isEnablePopLog()) {
                    POP_LOGGER.warn("[PopBuffer]add ack fail, rqId={}, stay too long, {}, {}, {}", reviveQid, pointWrapper, ackMsg, now);
                }
                return false;
            }

            // merge ackMsg with checkpoint
            if (ackMsg instanceof BatchAckMsg) { // merge batch ackMsg
                for (Long ackOffset : ((BatchAckMsg) ackMsg).getAckOffsetList()) {
                    int indexOfAck = point.indexOfAck(ackOffset);
                    if (indexOfAck > -1) {
                        markBitCAS(pointWrapper.getBits(), indexOfAck);
                    } else {
                        POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                    }
                }
            } else { // merge ackMsg
                int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                if (indexOfAck > -1) {
                    markBitCAS(pointWrapper.getBits(), indexOfAck);
                } else {
                    POP_LOGGER.error("[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}", reviveQid, ackMsg, point);
                    return true;
                }
            }

            // logging
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("[PopBuffer]add ack, rqId={}, {}, {}", reviveQid, pointWrapper, ackMsg);
            }

//            // check ak done
//            if (isCkDone(pointWrapper)) {
//                // cancel ck for timer
//                cancelCkTimer(pointWrapper);
//            }
            return true;
        } catch (Throwable e) {
            POP_LOGGER.error("[PopBuffer]add ack error, rqId=" + reviveQid + ", " + ackMsg, e);
        }

        return false;
    }

    public void clearOffsetQueue(String lockKey) {
        this.commitOffsets.remove(lockKey);
    }

    /**
     * write message(checkpoint) to revive topic, then update pointWrapper related info.
     *
     * @param pointWrapper checkpoint
     * @param runInCurrent async or sync
     */
    private void putCkToStore(final PopCheckPointWrapper pointWrapper, final boolean runInCurrent) {
        if (pointWrapper.getReviveQueueOffset() >= 0) {
            return;
        }

        MessageExtBrokerInner msgInner = popMessageProcessor.buildCkMsg(pointWrapper.getCk(), pointWrapper.getReviveQueueId());

        // Indicates that ck message is storing
        pointWrapper.setReviveQueueOffset(Long.MAX_VALUE);
        // default value of isAppendCkAsync is false
        if (brokerController.getBrokerConfig().isAppendCkAsync() && runInCurrent) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleCkMessagePutResult(putMessageResult, pointWrapper);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put ck to store fail: {}", pointWrapper, throwable);
                pointWrapper.setReviveQueueOffset(-1);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleCkMessagePutResult(putMessageResult, pointWrapper);
        }
    }

    private void handleCkMessagePutResult(PutMessageResult putMessageResult, final PopCheckPointWrapper pointWrapper) {
        brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveCkPutCount(pointWrapper.getCk(), putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            pointWrapper.setReviveQueueOffset(-1);
            POP_LOGGER.error("[PopBuffer]put ck to store fail: {}, {}", pointWrapper, putMessageResult);
            return;
        }
        pointWrapper.setCkStored(true);

        if (putMessageResult.isRemotePut()) {
            //No AppendMessageResult when escaping remotely
            pointWrapper.setReviveQueueOffset(0);
        } else {
            pointWrapper.setReviveQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
        }

        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ck to store ok: {}, {}", pointWrapper, putMessageResult);
        }
    }

    /**
     * Persist message which created by checkpoint to the revive topic.
     *
     * <ul>
     *     <li>create message by checkpoint</li>
     *     <li>write message to revive topic</li>
     *     <li>update pointWrapper related info</li>
     * </ul>
     *
     * @param pointWrapper the checkpoint wrapper containing the original CK
     * @param msgIndex     the sub-message index within the CK batch to ack
     * @param count        atomic counter incremented on successful persistence
     */
    private void putAckToStore(final PopCheckPointWrapper pointWrapper, byte msgIndex, AtomicInteger count) {
        // build ackMsg and Message by checkpoint
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(point.ackOffsetByIndex(msgIndex));
        ackMsg.setStartOffset(point.getStartOffset());
        ackMsg.setConsumerGroup(point.getCId());
        ackMsg.setTopic(point.getTopic());
        ackMsg.setQueueId(point.getQueueId());
        ackMsg.setPopTime(point.getPopTime());
        ackMsg.setBrokerName(point.getBrokerName());
        msgInner.setTopic(popMessageProcessor.getReviveTopic());
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // store message then change store status of the checkpoint
        if (brokerController.getBrokerConfig().isAppendAckAsync()) { // default value is false
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleAckPutMessageResult(ackMsg, putMessageResult, pointWrapper, count, msgIndex);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}", pointWrapper, ackMsg, throwable);
                return null;
            });
        } else {
            // store message
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            // change store status of the checkpoint
            handleAckPutMessageResult(ackMsg, putMessageResult, pointWrapper, count, msgIndex);
        }
    }

    /**
     * update store status of checkpoint if revive message stored successfully.
     *
     * @param ackMsg          the ack message that was persisted
     * @param putMessageResult the result returned by the store
     * @param pointWrapper    the checkpoint wrapper being processed
     * @param count           atomic counter incremented on success
     * @param msgIndex        the sub-message index that was persisted
     */
    private void handleAckPutMessageResult(AckMsg ackMsg, PutMessageResult putMessageResult,
        PopCheckPointWrapper pointWrapper, AtomicInteger count, byte msgIndex) {
        brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put ack to store fail: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
            return;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put ack to store ok: {}, {}, {}", pointWrapper, ackMsg, putMessageResult);
        }
        count.incrementAndGet();
        markBitCAS(pointWrapper.getToStoreBits(), msgIndex);
    }

    private void putBatchAckToStore(final PopCheckPointWrapper pointWrapper, final List<Byte> msgIndexList,
        AtomicInteger count) {
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        final BatchAckMsg batchAckMsg = new BatchAckMsg();

        for (Byte msgIndex : msgIndexList) {
            batchAckMsg.getAckOffsetList().add(point.ackOffsetByIndex(msgIndex));
        }
        batchAckMsg.setStartOffset(point.getStartOffset());
        batchAckMsg.setConsumerGroup(point.getCId());
        batchAckMsg.setTopic(point.getTopic());
        batchAckMsg.setQueueId(point.getQueueId());
        batchAckMsg.setPopTime(point.getPopTime());
        msgInner.setTopic(popMessageProcessor.getReviveTopic());
        msgInner.setBody(JSON.toJSONString(batchAckMsg).getBytes(DataConverter.CHARSET_UTF8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.BATCH_ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(point.getReviveTime());
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genBatchAckUniqueId(batchAckMsg));

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        if (brokerController.getBrokerConfig().isAppendAckAsync()) {
            brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenAccept(putMessageResult -> {
                handleBatchAckPutMessageResult(batchAckMsg, putMessageResult, pointWrapper, count, msgIndexList);
            }).exceptionally(throwable -> {
                POP_LOGGER.error("[PopBuffer]put batchAckMsg to store fail: {}, {}", pointWrapper, batchAckMsg, throwable);
                return null;
            });
        } else {
            PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
            handleBatchAckPutMessageResult(batchAckMsg, putMessageResult, pointWrapper, count, msgIndexList);
        }
    }

    private void handleBatchAckPutMessageResult(BatchAckMsg batchAckMsg, PutMessageResult putMessageResult,
        PopCheckPointWrapper pointWrapper, AtomicInteger count, List<Byte> msgIndexList) {
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]put batch ack to store fail: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
            return;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]put batch ack to store ok: {}, {}, {}", pointWrapper, batchAckMsg, putMessageResult);
        }

        count.addAndGet(msgIndexList.size());
        for (Byte i : msgIndexList) {
            markBitCAS(pointWrapper.getToStoreBits(), i);
        }
    }

    private boolean cancelCkTimer(final PopCheckPointWrapper pointWrapper) {
        // not stored, no need cancel
        if (pointWrapper.getReviveQueueOffset() < 0) {
            return true;
        }
        PopCheckPoint point = pointWrapper.getCk();
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(popMessageProcessor.getReviveTopic());
        msgInner.setBody((pointWrapper.getReviveQueueId() + "-" + pointWrapper.getReviveQueueOffset()).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(pointWrapper.getReviveQueueId());
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());

        msgInner.setDeliverTimeMs(point.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("[PopBuffer]PutMessageCallback cancelCheckPoint fail, {}, {}", pointWrapper, putMessageResult);
            return false;
        }
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("[PopBuffer]cancelCheckPoint, {}", pointWrapper);
        }
        return true;
    }

    /**
     * Check whether all sub-messages in the checkpoint have been acked.
     *
     * <p>Every sub-message has a corresponding bit in
     * {@link PopCheckPointWrapper#bits}. This method returns {@code true} when
     * all bits are set, meaning the CK can be removed from the buffer without
     * writing any ack to the revive topic (clean completion).
     *
     * @param pointWrapper the checkpoint wrapper to check
     * @return {@code true} if every sub-message has been acked
     */
    private boolean isCkDone(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        for (byte i = 0; i < num; i++) {
            if (!DataConverter.getBit(pointWrapper.getBits().get(), i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether all acked sub-messages have been fully persisted.
     *
     * <p>Uses XOR: {@code bits ^ toStoreBits}. A bit is set in the result when
     * the corresponding sub-message has been acked ({@code bits}) but not yet
     * persisted ({@code toStoreBits}). Returns {@code true} only when every
     * acked message has also been persisted, meaning the checkpoint is ready
     * for final cleanup.
     *
     * @param pointWrapper the checkpoint wrapper to check
     * @return {@code true} if no ack remains to be persisted
     */
    private boolean isCkDoneForFinish(PopCheckPointWrapper pointWrapper) {
        byte num = pointWrapper.getCk().getNum();
        int bits = pointWrapper.getBits().get() ^ pointWrapper.getToStoreBits().get();
        for (byte i = 0; i < num; i++) {
            if (DataConverter.getBit(bits, i)) {
                return false;
            }
        }
        return true;
    }

    public class QueueWithTime<T> {
        private final LinkedBlockingDeque<T> queue;
        private long time;

        public QueueWithTime() {
            this.queue = new LinkedBlockingDeque<>();
            this.time = System.currentTimeMillis();
        }

        public void setTime(long popTime) {
            this.time = popTime;
        }

        public long getTime() {
            return time;
        }

        public LinkedBlockingDeque<T> get() {
            return queue;
        }
    }

    public class PopCheckPointWrapper {
        private final int reviveQueueId;
        /**
         * The consume queue offset of the CK message in the revive topic.
         *
         * <p>Three-state indicator:
         * <ul>
         *   <li>{@code -1} — not yet stored; {@link #putCkToStore} will write it</li>
         *   <li>{@code >= 0} — successfully stored; the value is the offset in the
         *       revive topic's consume queue</li>
         *   <li>{@link Long#MAX_VALUE} — a write is in progress (prevents duplicate
         *       writes from concurrent scans)</li>
         * </ul>
         */
        private volatile long reviveQueueOffset;
        private final PopCheckPoint ck;
        // store ack states of messages, one byte for each message
        private final AtomicInteger bits;
        // bits for stored buffer ak, one byte for each message
        private final AtomicInteger toStoreBits;
        // nextOffset of original topic
        private final long nextBeginOffset;
        // topic@group@queueId
        private final String lockKey;
        // topic + group + queueId + startOffset + popTime + brokerName
        private final String mergeKey;
        /**
         * Whether this checkpoint should be written to the revive topic directly.
         *
         * <p>When {@code true}:
         * <ul>
         *   <li>The CK has already been or will be written to the revive topic directly</li>
         *   <li>No Ack merging is needed — {@link #addAk} rejects these entries</li>
         *   <li>The wrapper exists solely to maintain FIFO offset commit order in
         *       {@link #commitOffsets}</li>
         * </ul>
         *
         * @see PopBufferMergeService#addCkJustOffset
         * @see PopBufferMergeService#addCkMock
         */
        private final boolean justOffset;
        // whether check point has stored in revive queue
        private volatile boolean ckStored = false;

        public PopCheckPointWrapper(int reviveQueueId, long reviveQueueOffset, PopCheckPoint point,
            long nextBeginOffset) {
            this.reviveQueueId = reviveQueueId;
            this.reviveQueueOffset = reviveQueueOffset;
            this.ck = point;
            this.bits = new AtomicInteger(0);
            this.toStoreBits = new AtomicInteger(0);
            this.nextBeginOffset = nextBeginOffset;
            this.lockKey = ck.getTopic() + PopAckConstants.SPLIT + ck.getCId() + PopAckConstants.SPLIT + ck.getQueueId();
            this.mergeKey = point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName();
            this.justOffset = false;
        }

        public PopCheckPointWrapper(int reviveQueueId, long reviveQueueOffset, PopCheckPoint point,
            long nextBeginOffset,
            boolean justOffset) {
            this.reviveQueueId = reviveQueueId;
            this.reviveQueueOffset = reviveQueueOffset;
            this.ck = point;
            this.bits = new AtomicInteger(0);
            this.toStoreBits = new AtomicInteger(0);
            this.nextBeginOffset = nextBeginOffset;
            this.lockKey = ck.getTopic() + PopAckConstants.SPLIT + ck.getCId() + PopAckConstants.SPLIT + ck.getQueueId();
            this.mergeKey = point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName();
            this.justOffset = justOffset;
        }

        public int getReviveQueueId() {
            return reviveQueueId;
        }

        public long getReviveQueueOffset() {
            return reviveQueueOffset;
        }

        public boolean isCkStored() {
            return ckStored;
        }

        public void setReviveQueueOffset(long reviveQueueOffset) {
            this.reviveQueueOffset = reviveQueueOffset;
        }

        public PopCheckPoint getCk() {
            return ck;
        }

        public AtomicInteger getBits() {
            return bits;
        }

        public AtomicInteger getToStoreBits() {
            return toStoreBits;
        }

        public long getNextBeginOffset() {
            return nextBeginOffset;
        }

        public String getLockKey() {
            return lockKey;
        }

        public String getMergeKey() {
            return mergeKey;
        }

        public boolean isJustOffset() {
            return justOffset;
        }

        public void setCkStored(boolean ckStored) {
            this.ckStored = ckStored;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("CkWrap{");
            sb.append("rq=").append(reviveQueueId);
            sb.append(", rqo=").append(reviveQueueOffset);
            sb.append(", ck=").append(ck);
            sb.append(", bits=").append(bits);
            sb.append(", sBits=").append(toStoreBits);
            sb.append(", nbo=").append(nextBeginOffset);
            sb.append(", cks=").append(ckStored);
            sb.append(", jo=").append(justOffset);
            sb.append('}');
            return sb.toString();
        }
    }

}
