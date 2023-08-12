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
package org.apache.rocketmq.store.service;

import com.google.common.collect.Sets;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import java.io.File;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;

public class ConsumeQueueService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final DefaultMessageStore messageStore;

    public ConsumeQueueService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return getOffsetInQueueByTime(topic, queueId, timestamp, BoundaryType.LOWER);
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        ConsumeQueueInterface logic = this.findConsumeQueue(topic, queueId);
        if (logic == null) {
            return 0;
        }

        long resultOffset = logic.getOffsetInQueueByTime(timestamp, boundaryType);
        // Make sure the result offset is in valid range.
        resultOffset = Math.max(resultOffset, logic.getMinOffsetInQueue());
        resultOffset = Math.min(resultOffset, logic.getMaxOffsetInQueue());
        return resultOffset;
    }

    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return getStoreTime(logicQueue.getEarliestUnit());
        }

        return -1;
    }

    public long getEarliestMessageTime() {
        long minPhyOffset = messageStore.getMinPhyOffset();
        if (messageStore.getCommitLog() instanceof DLedgerCommitLog) {
            minPhyOffset += DLedgerEntry.BODY_OFFSET;
        }
        final int size = MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION + 8;
        return messageStore.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        return CompletableFuture.completedFuture(getEarliestMessageTime(topic, queueId));
    }

    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return getStoreTime(logicQueue.get(consumeQueueOffset));
        }

        return -1;
    }

    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId,
        long consumeQueueOffset) {
        return CompletableFuture.completedFuture(getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset));
    }

    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueueInterface logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<>();
        if (messageStore.isShutdown()) {
            return messageIds;
        }

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextOffset);
                try {
                    if (bufferConsumeQueue != null && bufferConsumeQueue.hasNext()) {
                        while (bufferConsumeQueue.hasNext()) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, cqUnit.getQueueOffset());
                            nextOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();
                            if (nextOffset >= maxOffset) {
                                return messageIds;
                            }
                        }
                    } else {
                        return messageIds;
                    }
                } finally {
                    if (bufferConsumeQueue != null) {
                        bufferConsumeQueue.release();
                    }
                }
            }
        }
        return messageIds;
    }

    public boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize) {
        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            return false;
        }

        CqUnit firstCQItem = consumeQueue.get(consumeOffset);
        if (firstCQItem == null) {
            return false;
        }
        long startOffsetPy = firstCQItem.getPos();
        if (batchSize <= 1) {
            int size = firstCQItem.getSize();
            return checkInMemByCommitOffset(startOffsetPy, size);
        }

        CqUnit lastCQItem = consumeQueue.get(consumeOffset + batchSize);
        if (lastCQItem == null) {
            int size = firstCQItem.getSize();
            return checkInMemByCommitOffset(startOffsetPy, size);
        }
        long endOffsetPy = lastCQItem.getPos();
        int size = (int) (endOffsetPy - startOffsetPy) + lastCQItem.getSize();
        return checkInMemByCommitOffset(startOffsetPy, size);
    }

    public boolean checkInStoreByConsumeOffset(String topic, int queueId, long consumeOffset) {
        long commitLogOffset = getCommitLogOffsetInQueue(topic, queueId, consumeOffset);
        return checkInDiskByCommitOffset(commitLogOffset);
    }

    public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
        return messageStore.getConsumeQueueStore().findOrCreateConsumeQueue(topic, queueId);
    }
    public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
        if (from < 0) {
            from = 0;
        }

        if (from >= to) {
            return 0;
        }

        if (null == filter) {
            return to - from;
        }

        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (null == consumeQueue) {
            return 0;
        }

        // correct the "from" argument to min offset in queue if it is too small
        long minOffset = consumeQueue.getMinOffsetInQueue();
        if (from < minOffset) {
            long diff = to - from;
            from = minOffset;
            to = from + diff;
        }

        long msgCount = consumeQueue.estimateMessageCount(from, to, filter);
        return msgCount == -1 ? to - from : msgCount;
    }

    public boolean checkInDiskByCommitOffset(long offsetPy) {
        return offsetPy >= messageStore.getCommitLog().getMinOffset();
    }

    public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = messageStore.getConsumeQueueTable().get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void increaseOffset(MessageExtBrokerInner msg, short messageNum) {
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            messageStore.getConsumeQueueStore().increaseQueueOffset(msg, messageNum);
        }
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        return getMaxOffsetInQueue(topic, queueId, true);
    }

    public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) {
        if (committed) {
            ConsumeQueueInterface logic = this.findConsumeQueue(topic, queueId);
            if (logic != null) {
                return logic.getMaxOffsetInQueue();
            }
        } else {
            Long offset = messageStore.getConsumeQueueStore().getMaxOffset(topic, queueId);
            if (offset != null) {
                return offset;
            }
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueueInterface consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue == null) {
            return 0;
        }

        ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(consumeQueueOffset);
        if (bufferConsumeQueue == null) {
            return 0;
        }

        try {
            if (bufferConsumeQueue.hasNext()) {
                return bufferConsumeQueue.next().getPos();
            }
        } finally {
            bufferConsumeQueue.release();
        }

        return 0;
    }

    /**
     * Lazy clean queue offset table.
     * If offset table is cleaned, and old messages are dispatching after the old consume queue is cleaned,
     * consume queue will be created with old offset, then later message with new offset table can not be
     * dispatched to consume queue.
     */
    public int deleteTopics(final Set<String> deleteTopics) {
        if (deleteTopics == null || deleteTopics.isEmpty()) {
            return 0;
        }

        int deleteCount = 0;
        for (String topic : deleteTopics) {
            ConcurrentMap<Integer, ConsumeQueueInterface> queueTable =
                messageStore.getConsumeQueueStore().getConsumeQueueTable().get(topic);

            if (queueTable == null || queueTable.isEmpty()) {
                continue;
            }

            for (ConsumeQueueInterface cq : queueTable.values()) {
                messageStore.getConsumeQueueStore().destroy(cq);
                LOGGER.info("DeleteTopic: ConsumeQueue has been cleaned, topic={}, queueId={}",
                    cq.getTopic(), cq.getQueueId());
                messageStore.getConsumeQueueStore().removeTopicQueueTable(cq.getTopic(), cq.getQueueId());
            }

            // remove topic from cq table
            messageStore.getConsumeQueueStore().getConsumeQueueTable().remove(topic);

            if (messageStore.getBrokerConfig().isAutoDeleteUnusedStats()) {
                messageStore.getBrokerStatsManager().onTopicDeleted(topic);
            }

            // destroy consume queue dir
            String consumeQueueDir = StorePathConfigHelper.getStorePathConsumeQueue(
                messageStore.getMessageStoreConfig().getStorePathRootDir()) + File.separator + topic;
            String consumeQueueExtDir = StorePathConfigHelper.getStorePathConsumeQueueExt(
                messageStore.getMessageStoreConfig().getStorePathRootDir()) + File.separator + topic;
            String batchConsumeQueueDir = StorePathConfigHelper.getStorePathBatchConsumeQueue(
                messageStore.getMessageStoreConfig().getStorePathRootDir()) + File.separator + topic;

            UtilAll.deleteEmptyDirectory(new File(consumeQueueDir));
            UtilAll.deleteEmptyDirectory(new File(consumeQueueExtDir));
            UtilAll.deleteEmptyDirectory(new File(batchConsumeQueueDir));

            LOGGER.info("DeleteTopic: Topic has been destroyed, topic={}", topic);
            deleteCount++;
        }
        return deleteCount;
    }

    public int cleanUnusedTopic(final Set<String> retainTopics) {
        Set<String> consumeQueueTopicSet = messageStore.getConsumeQueueTable().keySet();
        int deleteCount = 0;
        for (String topicName : Sets.difference(consumeQueueTopicSet, retainTopics)) {
            if (retainTopics.contains(topicName) ||
                TopicValidator.isSystemTopic(topicName) ||
                MixAll.isLmq(topicName)) {
                continue;
            }
            deleteCount += this.deleteTopics(Sets.newHashSet(topicName));
        }
        return deleteCount;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = messageStore.getCommitLog().getMinOffset();

        messageStore.getConsumeQueueStore().cleanExpired(minCommitLogOffset);
    }

    public boolean resetWriteOffset(long phyOffset) {
        //copy a new map
        ConcurrentHashMap<String, Long> newMap = new ConcurrentHashMap<>(messageStore.getConsumeQueueStore().getTopicQueueTable());
        SelectMappedBufferResult lastBuffer = null;
        long startReadOffset = phyOffset == -1 ? 0 : phyOffset;
        while ((lastBuffer = messageStore.selectOneMessageByOffset(startReadOffset)) != null) {
            try {
                if (lastBuffer.getStartOffset() > startReadOffset) {
                    startReadOffset = lastBuffer.getStartOffset();
                    continue;
                }

                ByteBuffer bb = lastBuffer.getByteBuffer();
                int magicCode = bb.getInt(bb.position() + 4);
                if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                    startReadOffset += bb.getInt(bb.position());
                    continue;
                } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                    throw new RuntimeException("Unknown magicCode: " + magicCode);
                }

                lastBuffer.getByteBuffer().mark();

                DispatchRequest dispatchRequest = messageStore.checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, messageStore.getMessageStoreConfig().isDuplicationEnable(), true);
                if (!dispatchRequest.isSuccess())
                    break;

                lastBuffer.getByteBuffer().reset();

                MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                if (msg == null) {
                    break;
                }
                String key = msg.getTopic() + "-" + msg.getQueueId();
                Long cur = newMap.get(key);
                if (cur != null && cur > msg.getQueueOffset()) {
                    newMap.put(key, msg.getQueueOffset());
                }
                startReadOffset += msg.getStoreSize();
            } catch (Throwable e) {
                LOGGER.error("resetWriteOffset error.", e);
            } finally {
                if (lastBuffer != null)
                    lastBuffer.release();
            }
        }
        if (messageStore.getCommitLog().resetOffset(phyOffset)) {
            messageStore.getConsumeQueueStore().setTopicQueueTable(newMap);
            return true;
        } else {
            return false;
        }
    }

    protected long getStoreTime(CqUnit result) {
        if (result == null) {
            return -1;

        }

        try {
            final long phyOffset = result.getPos();
            final int size = result.getSize();
            return messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
        } catch (Exception e) {
            return -1;
        }
    }


    private boolean checkInMemByCommitOffset(long offsetPy, int size) {
        SelectMappedBufferResult message = messageStore.getCommitLog().getMessage(offsetPy, size);
        if (message == null) {
            return false;
        }

        try {
            return message.isInMem();
        } finally {
            message.release();
        }
    }

}
