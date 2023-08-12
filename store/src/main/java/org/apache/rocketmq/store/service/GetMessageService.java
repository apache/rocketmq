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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CleanupPolicy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CleanupPolicyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.StoreUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;

public class GetMessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // Max pull msg size
    private final static int MAX_PULL_MSG_SIZE = 128 * 1024 * 1024;

    private final DefaultMessageStore messageStore;

    public GetMessageService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final MessageFilter messageFilter) {
        return getMessage(group, topic, queueId, offset, maxMsgNums, MAX_PULL_MSG_SIZE, messageFilter);
    }

    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        return CompletableFuture.completedFuture(getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter));
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter) {
        if (messageStore.isShutdown()) {
            LOGGER.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!messageStore.getRunningFlags().isReadable()) {
            LOGGER.warn("message store is not readable, so getMessage is forbidden " + messageStore.getRunningFlags().getFlagBits());
            return null;
        }

        Optional<TopicConfig> topicConfig = messageStore.getTopicConfig(topic);
        CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(topicConfig);
        //check request topic flag
        if (Objects.equals(policy, CleanupPolicy.COMPACTION) && messageStore.getMessageStoreConfig().isEnableCompaction()) {
            return messageStore.getCompactionStore().getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize);
        } // else skip

        long beginTime = messageStore.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        final long maxOffsetPy = messageStore.getCommitLog().getMaxOffset();

        ConsumeQueueInterface consumeQueue = messageStore.findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
            } else {
                final int maxFilterMessageSize = Math.max(16000, maxMsgNums * consumeQueue.getUnitSize());
                final boolean diskFallRecorded = messageStore.getMessageStoreConfig().isDiskFallRecorded();

                long maxPullSize = Math.max(maxTotalMsgSize, 100);
                if (maxPullSize > MAX_PULL_MSG_SIZE) {
                    LOGGER.warn("The max pull size is too large maxPullSize={} topic={} queueId={}", maxPullSize, topic, queueId);
                    maxPullSize = MAX_PULL_MSG_SIZE;
                }
                status = GetMessageStatus.NO_MATCHED_MESSAGE;
                long maxPhyOffsetPulling = 0;
                int cqFileNum = 0;

                while (getResult.getBufferTotalSize() <= 0
                    && nextBeginOffset < maxOffset
                    && cqFileNum++ < messageStore.getMessageStoreConfig().getTravelCqFileNumWhenGetMessage()) {
                    ReferredIterator<CqUnit> bufferConsumeQueue = consumeQueue.iterateFrom(nextBeginOffset);

                    if (bufferConsumeQueue == null) {
                        status = GetMessageStatus.OFFSET_FOUND_NULL;
                        nextBeginOffset = nextOffsetCorrection(nextBeginOffset, messageStore.getConsumeQueueStore().rollNextFile(consumeQueue, nextBeginOffset));
                        LOGGER.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                            + maxOffset + ", but access logic queue failed. Correct nextBeginOffset to " + nextBeginOffset);
                        break;
                    }

                    try {
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        while (bufferConsumeQueue.hasNext()
                            && nextBeginOffset < maxOffset) {
                            CqUnit cqUnit = bufferConsumeQueue.next();
                            long offsetPy = cqUnit.getPos();
                            int sizePy = cqUnit.getSize();

                            boolean isInMem = estimateInMemByCommitOffset(offsetPy, maxOffsetPy);

                            if ((cqUnit.getQueueOffset() - offset) * consumeQueue.getUnitSize() > maxFilterMessageSize) {
                                break;
                            }

                            if (isTheBatchFull(sizePy, cqUnit.getBatchNum(), maxMsgNums, maxPullSize, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInMem)) {
                                break;
                            }

                            if (getResult.getBufferTotalSize() >= maxPullSize) {
                                break;
                            }

                            maxPhyOffsetPulling = offsetPy;

                            //Be careful, here should before the isTheBatchFull
                            nextBeginOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum();

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset) {
                                    continue;
                                }
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByConsumeQueue(cqUnit.getValidTagsCodeAsLong(), cqUnit.getCqExtUnit())) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = messageStore.getCommitLog().getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                nextPhyFileStartOffset = messageStore.getCommitLog().rollNextFile(offsetPy);
                                continue;
                            }

                            if (messageStore.getMessageStoreConfig().isColdDataFlowControlEnable() && !MixAll.isSysConsumerGroupForNoColdReadLimit(group) && !selectResult.isInCache()) {
                                getResult.setColdDataSum(getResult.getColdDataSum() + sizePy);
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }
                            messageStore.getStoreStatsService().getGetMessageTransferredMsgCount().add(cqUnit.getBatchNum());
                            getResult.addMessage(selectResult, cqUnit.getQueueOffset(), cqUnit.getBatchNum());
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }
                    } finally {
                        bufferConsumeQueue.release();
                    }
                }

                if (diskFallRecorded) {
                    long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                    messageStore.getBrokerStatsManager().recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                }

                long diff = maxOffsetPy - maxPhyOffsetPulling;
                long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                    * (messageStore.getMessageStoreConfig().getAccessMessageInMemoryMaxRatio() / 100.0));
                getResult.setSuggestPullingFromSlave(diff > memory);
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            messageStore.getStoreStatsService().getGetMessageTimesTotalFound().add(1);
        } else {
            messageStore.getStoreStatsService().getGetMessageTimesTotalMiss().add(1);
        }
        long elapsedTime = messageStore.getSystemClock().now() - beginTime;
        messageStore.getStoreStatsService().setGetMessageEntireTimeMax(elapsedTime);

        // lazy init no data found.
        if (getResult == null) {
            getResult = new GetMessageResult(0);
        }

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic,
        int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
        return CompletableFuture.completedFuture(getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter));
    }


    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE ||
            messageStore.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean estimateInMemByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (messageStore.getMessageStoreConfig().getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) <= memory;
    }

    private boolean isTheBatchFull(int sizePy, int unitBatchNum, int maxMsgNums, long maxMsgSize, int bufferTotal,
        int messageTotal, boolean isInMem) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (messageTotal + unitBatchNum > maxMsgNums) {
            return true;
        }

        if (bufferTotal + sizePy > maxMsgSize) {
            return true;
        }

        if (isInMem) {
            if ((bufferTotal + sizePy) > messageStore.getMessageStoreConfig().getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            return messageTotal > messageStore.getMessageStoreConfig().getMaxTransferCountOnMessageInMemory() - 1;
        } else {
            if ((bufferTotal + sizePy) > messageStore.getMessageStoreConfig().getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            return messageTotal > messageStore.getMessageStoreConfig().getMaxTransferCountOnMessageInDisk() - 1;
        }
    }


}
