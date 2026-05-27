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
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.BatchAckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

/**
 * Per-queue service that reads the revive topic, matches checkpoints with AckMsgs, and
 * revives timed-out messages by re-publishing them to the retry topic.
 *
 * <p>There is only one public method for business: run</p>
 *
 * <p>Each revive queue has its own dedicated {@code PopReviveService} instance.
 * The service periodically:
 * <ol>
 *   <li>Scans the revive topic ({@link #consumeReviveMessage}) to collect CK
 *       (checkpoint) and Ack messages, merging Acks into CK's bitMap</li>
 *   <li>Processes expired checkpoints ({@link #mergeAndRevive}) by re-publishing any
 *       un-acked sub-messages back to the retry topic via {@link #reviveRetry}</li>
 * </ol>
 *
 * <p>This is the <b>file-based</b> revive path (CK + Ack messages are stored in
 * the system revive topic). It is complemented by the KVStore-based path in
 * {@code PopConsumerService} which handles the {@code PopConsumerKVStore} flow.
 */
public class PopReviveService extends ServiceThread {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final int[] ckRewriteIntervalsInSeconds = new int[] { 10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200 };

    private int queueId;
    private BrokerController brokerController;
    private String reviveTopic;
    private long currentReviveMessageTimestamp = -1;
    private volatile boolean shouldRunPopRevive = false;

    /**
     * Tracks checkpoints that are currently being revived.
     *
     * <p>Key — the checkpoint being processed.
     * Value — a pair of (startTime, completed), where:
     * <ul>
     *   <li>{@code startTime} is the timestamp when revival began</li>
     *   <li>{@code completed} is {@code true} once all sub-messages have been
     *       processed (success or failure)</li>
     * </ul>
     *
     * <p>The map is sorted by {@link PopCheckPoint#compareTo} (by startOffset).
     * This ordering is used to drain completed entries from the head, ensuring
     * the revive topic offset is committed strictly in sequence.
     *
     * <p>Concurrency is limited to at most 3 entries at a time (see
     * {@link #mergeAndRevive}). If an entry stays incomplete for over 30
     * seconds, it is considered hung and is skipped via {@link #rePutCK}.
     */
    private final NavigableMap<PopCheckPoint/* oldCK */, Pair<Long/* timestamp */, Boolean/* result */>> inflightReviveRequestMap = Collections.synchronizedNavigableMap(new TreeMap<>());
    private long reviveOffset;

    public PopReviveService(BrokerController brokerController, String reviveTopic, int queueId) {
        this.queueId = queueId;
        this.brokerController = brokerController;
        this.reviveTopic = reviveTopic;
        this.reviveOffset = brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
    }

    @Override
    public String getServiceName() {
        if (brokerController != null && brokerController.getBrokerConfig().isInBrokerContainer()) {
            return brokerController.getBrokerIdentity().getIdentifier() + "PopReviveService_" + this.queueId;
        }
        return "PopReviveService_" + this.queueId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setShouldRunPopRevive(final boolean shouldRunPopRevive) {
        this.shouldRunPopRevive = shouldRunPopRevive;
    }

    public boolean isShouldRunPopRevive() {
        return shouldRunPopRevive;
    }

    /**
     * Re-publish a timed-out message to the retry topic.
     *
     * <p>Constructs a new {@link MessageExtBrokerInner} from the original
     * message, increments the reconsume count (unless suspended), sets the
     * first-pop time and origin group properties, and writes it to the
     * appropriate retry topic (V1 or V2 depending on configuration).
     *
     * <p>If the retry topic does not exist, it is created automatically
     * via {@link #addRetryTopicIfNotExist}.
     *
     * @param popCheckPoint the checkpoint that triggered the revive
     * @param messageExt    the original message to re-publish
     * @return {@code true} if the message was written successfully
     */
    private boolean reviveRetry(PopCheckPoint popCheckPoint, MessageExt messageExt) {
        // convert checkpoint to inner message
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        if (!popCheckPoint.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            msgInner.setTopic(KeyBuilder.buildPopRetryTopic(popCheckPoint.getTopic(), popCheckPoint.getCId(), brokerController.getBrokerConfig().isEnableRetryTopicV2()));
        } else {
            msgInner.setTopic(popCheckPoint.getTopic());
        }
        msgInner.setBody(messageExt.getBody());
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }
        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        if (popCheckPoint.isSuspend()) {
            msgInner.setReconsumeTimes(messageExt.getReconsumeTimes());
        } else {
            msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        }
        msgInner.getProperties().putAll(messageExt.getProperties());
        if (messageExt.getReconsumeTimes() == 0 || msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(popCheckPoint.getPopTime()));
        }
        msgInner.getProperties().put(MessageConst.PROPERTY_ORIGIN_GROUP, popCheckPoint.getCId());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        // set topic and queueId
        addRetryTopicIfNotExist(msgInner.getTopic(), popCheckPoint.getCId());
        msgInner.setQueueId(getRetryQueueId(msgInner.getTopic(), messageExt));

        // store message
        PutMessageResult putMessageResult = brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);

        // logging and metric
        brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveRetryMessageCount(popCheckPoint, putMessageResult.getPutMessageStatus());
        if (brokerController.getBrokerConfig().isEnablePopLog()) {
            POP_LOGGER.info("reviveQueueId={},retry msg, ck={}, msg queueId {}, offset {}, reviveDelay={}, result is {} ",
                queueId, popCheckPoint, messageExt.getQueueId(), messageExt.getQueueOffset(),
                (System.currentTimeMillis() - popCheckPoint.getReviveTime()) / 1000, putMessageResult);
        }
        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            POP_LOGGER.error("reviveQueueId={}, revive error, msg is: {}", queueId, msgInner);
            return false;
        }
        this.brokerController.getPopInflightMessageCounter().decrementInFlightMessageNum(popCheckPoint);
        this.brokerController.getBrokerStatsManager().incBrokerPutNums(popCheckPoint.getTopic(), 1);
        this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
        this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        return true;
    }

    private void initPopRetryOffset(String retryTopic, String consumerGroup, int retryQueueNum) {
        for (int i = 0; i < retryQueueNum; i++) {
            long offset = this.brokerController.getConsumerOffsetManager().queryOffset(consumerGroup, retryTopic, i);
            if (offset < 0) {
                this.brokerController.getConsumerOffsetManager().commitOffset("initPopRetryOffset", consumerGroup, retryTopic, i, 0);
            }
        }
    }

    public void addRetryTopicIfNotExist(String retryTopic, String consumerGroup) {
        if (brokerController != null) {
            TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(retryTopic);
            if (topicConfig != null && !brokerController.getBrokerConfig().isUseSeparateRetryQueue()) {
                return;
            }

            int retryQueueNum = PopAckConstants.retryQueueNum;
            if (brokerController.getBrokerConfig().isUseSeparateRetryQueue()) {
                String normalTopic = KeyBuilder.parseNormalTopic(retryTopic, consumerGroup);
                TopicConfig normalConfig = brokerController.getTopicConfigManager().selectTopicConfig(normalTopic); // always exists
                retryQueueNum = normalConfig.getWriteQueueNums();
                if (topicConfig != null && topicConfig.getWriteQueueNums() == normalConfig.getWriteQueueNums()) {
                    return;
                }
            }

            // create new one, or update in case of queue expansion
            topicConfig = new TopicConfig(retryTopic);
            topicConfig.setReadQueueNums(retryQueueNum);
            topicConfig.setWriteQueueNums(retryQueueNum);
            topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            topicConfig.setPerm(6);
            topicConfig.setTopicSysFlag(0);
            brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

            initPopRetryOffset(retryTopic, consumerGroup, retryQueueNum);
        }
    }

    private int getRetryQueueId(String retryTopic, MessageExt messageExt) {
        if (!brokerController.getBrokerConfig().isUseSeparateRetryQueue()) {
            return 0;
        }
        int oriQueueId = messageExt.getQueueId(); // original qid of normal or retry topic
        if (oriQueueId > brokerController.getTopicConfigManager().selectTopicConfig(retryTopic).getWriteQueueNums() - 1) {
            POP_LOGGER.warn("not expected, {}, {}, {}", retryTopic, oriQueueId, messageExt.getMsgId());
            return 0; // fallback
        }
        return oriQueueId;
    }

    /**
     * Pull a batch of messages from the revive topic at the given offset.
     *
     * <p>If the offset becomes illegal (e.g. the revive topic was truncated),
     * the revive offset is corrected to {@code nextBeginOffset - 1} so that
     * the next scan starts from a valid position.
     *
     * @param offset  the queue offset to start reading from
     * @param queueId the revive queue id
     * @return a list of decoded messages, or {@code null} if at the tail
     */
    protected List<MessageExt> getReviveMessage(long offset, int queueId) {
        PullResult pullResult = getMessage(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, offset, 32, true);
        if (pullResult == null) {
            return null;
        }
        if (reachTail(pullResult, offset)) {
            if (this.brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("reviveQueueId={}, reach tail,offset {}", queueId, offset);
            }
        } else if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            POP_LOGGER.error("reviveQueueId={}, OFFSET_ILLEGAL {}, result is {}", queueId, offset, pullResult);
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip offset correct topic={}, reviveQueueId={}", reviveTopic, queueId);
                return null;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, pullResult.getNextBeginOffset() - 1);
        }
        return pullResult.getMsgFoundList();
    }

    private boolean reachTail(PullResult pullResult, long offset) {
        return pullResult.getPullStatus() == PullStatus.NO_NEW_MSG
            || pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL && offset == pullResult.getMaxOffset();
    }

    // Triple<MessageExt, info, needRetry>
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getBizMessage(PopCheckPoint popCheckPoint, long offset) {
        return this.brokerController.getEscapeBridge().getMessageAsync(popCheckPoint.getTopic(), offset, popCheckPoint.getQueueId(), popCheckPoint.getBrokerName(), false);
    }

    public PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        boolean deCompressBody) {
        GetMessageResult getMessageResult = this.brokerController.getMessageStore().getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    pullStatus = PullStatus.FOUND;
                    foundList = decodeMsgList(getMessageResult, deCompressBody);
                    brokerController.getBrokerStatsManager().incGroupGetNums(group, topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().incGroupGetSize(group, topic, getMessageResult.getBufferTotalSize());
                    brokerController.getBrokerStatsManager().incBrokerGetNums(topic, getMessageResult.getMessageCount());
                    brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1).getStoreTimestamp());

                    Attributes attributes = this.brokerController.getBrokerMetricsManager().newAttributesBuilder()
                        .put(LABEL_TOPIC, topic)
                        .put(LABEL_CONSUMER_GROUP, group)
                        .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic) || MixAll.isSysConsumerGroup(group))
                        .build();
                    this.brokerController.getBrokerMetricsManager().getMessagesOutTotal().add(getMessageResult.getMessageCount(), attributes);
                    this.brokerController.getBrokerMetricsManager().getThroughputOutTotal().add(getMessageResult.getBufferTotalSize(), attributes);

                    break;
                case NO_MATCHED_MESSAGE:
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    POP_LOGGER.debug("no matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                    POP_LOGGER.debug("no new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_TOO_SMALL:
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    POP_LOGGER.warn("offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case OFFSET_OVERFLOW_ONE:
                    // no need to print WARN, because we use "offset + 1" to get the next message
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    break;
                default:
                    assert false;
                    break;
            }

            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            try {
                long maxQueueOffset = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                if (maxQueueOffset > offset) {
                    POP_LOGGER.error("get message from store return null. topic={}, groupId={}, requestOffset={}, maxQueueOffset={}",
                        topic, group, offset, maxQueueOffset);
                }
            } catch (ConsumeQueueException e) {
                POP_LOGGER.error("Failed to get max offset in queue", e);
            }
            return null;
        }
    }

    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult, boolean deCompressBody) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            if (messageBufferList != null) {
                for (int i = 0; i < messageBufferList.size(); i++) {
                    ByteBuffer bb = messageBufferList.get(i);
                    if (bb == null) {
                        POP_LOGGER.error("bb is null {}", getMessageResult);
                        continue;
                    }
                    MessageExt msgExt = MessageDecoder.decode(bb, true, deCompressBody);
                    if (msgExt == null) {
                        POP_LOGGER.error("decode msgExt is null {}", getMessageResult);
                        continue;
                    }
                    // use CQ offset, not offset in Message
                    msgExt.setQueueOffset(getMessageResult.getMessageQueueOffset().get(i));
                    foundList.add(msgExt);
                }
            }
        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * Pull Message from revive topic then transfer to checkpoint and ack messages.
     *
     * <p>This method reads messages from the revive topic starting from the
     * current offset. Each message is classified by its tag:
     * <ul>
     *   <li>{@link PopAckConstants#CK_TAG} — a checkpoint, deserialized from
     *       JSON and stored in the map by its merge key</li>
     *   <li>{@link PopAckConstants#ACK_TAG} or
     *       {@link PopAckConstants#BATCH_ACK_TAG} — an ack, matched to its
     *       corresponding checkpoint via the merge key. The ack offset is translated
     *       to a sub-message index ({@link PopCheckPoint#indexOfAck}) and
     *       the checkpoint's bitMap is updated via {@link DataConverter#setBit}</li>
     * </ul>
     *
     * <p>AckMsg that arrive after their checkpoint has already been processed
     * ({@code enableSkipLongAwaitingAck}) are handled by creating a mock CK
     * via {@link #mockCkForAck} so that the revive offset can still be
     * committed correctly.
     *
     * <p>The scan stops when any of:
     * <ul>
     *   <li>No more messages in the revive topic (tail reached)</li>
     *   <li>Scan time exceeds {@code reviveScanTime}</li>
     *   <li>The elapsed time since the first CK's revive time exceeds
     *       {@code ackTimeInterval + 1s}</li>
     * </ul>
     *
     * @param consumeReviveObj the mutable container that receives the collected
     *                         CKs and the computed {@code endTime}
     */
    protected void consumeReviveMessage(ConsumeReviveObj consumeReviveObj) {
        // init context parameters
        HashMap<String, PopCheckPoint> map = consumeReviveObj.map;
        HashMap<String, PopCheckPoint> mockPointMap = new HashMap<>();
        long startScanTime = System.currentTimeMillis();
        long endTime = 0;
        long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(PopAckConstants.REVIVE_GROUP, reviveTopic, queueId);
        long oldOffset = Math.max(reviveOffset, consumeOffset);
        consumeReviveObj.oldOffset = oldOffset;
        POP_LOGGER.info("reviveQueueId={}, old offset is {} ", queueId, oldOffset);
        long offset = oldOffset + 1;
        int noMsgCount = 0;
        long firstRt = 0;
        // offset self amend

        while (true) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }

            // pull revive messages
            List<MessageExt> messageExts = getReviveMessage(offset, queueId);
            if (messageExts == null || messageExts.isEmpty()) {
                long old = endTime;
                long timerDelay = brokerController.getMessageStore().getTimerMessageStore().getDequeueBehind();
                long commitLogDelay = brokerController.getMessageStore().getTimerMessageStore().getEnqueueBehind();
                // move endTime
                if (endTime != 0 && System.currentTimeMillis() - endTime > 3 * PopAckConstants.SECOND && timerDelay <= 0 && commitLogDelay <= 0) {
                    endTime = System.currentTimeMillis();
                }
                POP_LOGGER.debug("reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, timerDelay={}, commitLogDelay={} ",
                    queueId, offset, old, endTime, timerDelay, commitLogDelay);
                if (endTime - firstRt > PopAckConstants.ackTimeInterval + PopAckConstants.SECOND) {
                    break;
                }
                noMsgCount++;
                // Fixme: why sleep is useful here?
                try {
                    Thread.sleep(100);
                } catch (Throwable ignore) {
                }
                if (noMsgCount * 100L > 4 * PopAckConstants.SECOND) {
                    break;
                } else {
                    continue;
                }
            } else {
                noMsgCount = 0;
            }

            if (System.currentTimeMillis() - startScanTime > brokerController.getBrokerConfig().getReviveScanTime()) {
                POP_LOGGER.info("reviveQueueId={}, scan timeout ", queueId);
                break;
            }

            // convert message to PopCheckPoint and AckMsg
            for (MessageExt messageExt : messageExts) {
                if (PopAckConstants.CK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), DataConverter.CHARSET_UTF8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={},find ck, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    PopCheckPoint point = JSON.parseObject(raw, PopCheckPoint.class);
                    if (point.getTopic() == null || point.getCId() == null) {
                        continue;
                    }
                    map.put(point.getTopic() + point.getCId() + point.getQueueId() + point.getStartOffset() + point.getPopTime() + point.getBrokerName(), point);
                    brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveCkGetCount(point, queueId);
                    point.setReviveOffset(messageExt.getQueueOffset());
                    if (firstRt == 0) {
                        firstRt = point.getReviveTime();
                    }
                } else if (PopAckConstants.ACK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), StandardCharsets.UTF_8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }
                    AckMsg ackMsg = JSON.parseObject(raw, AckMsg.class);
                    brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveAckGetCount(ackMsg, queueId);
                    String brokerName = StringUtils.isNotBlank(ackMsg.getBrokerName()) ?
                        ackMsg.getBrokerName() : brokerController.getBrokerConfig().getBrokerName();
                    String mergeKey = ackMsg.getTopic() + ackMsg.getConsumerGroup() + ackMsg.getQueueId() + ackMsg.getStartOffset() + ackMsg.getPopTime() + brokerName;
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        // default value of enableSkipLongAwaitingAck is false
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }
                        if (mockCkForAck(messageExt, ackMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        // merge ackMsg into checkpoint
                        int indexOfAck = point.indexOfAck(ackMsg.getAckOffset());
                        if (indexOfAck > -1) {
                            point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                        } else {
                            POP_LOGGER.error("invalid ack index, {}, {}", ackMsg, point);
                        }
                    }
                } else if (PopAckConstants.BATCH_ACK_TAG.equals(messageExt.getTags())) {
                    String raw = new String(messageExt.getBody(), StandardCharsets.UTF_8);
                    if (brokerController.getBrokerConfig().isEnablePopLog()) {
                        POP_LOGGER.info("reviveQueueId={}, find batch ack, offset:{}, raw : {}", messageExt.getQueueId(), messageExt.getQueueOffset(), raw);
                    }

                    BatchAckMsg bAckMsg = JSON.parseObject(raw, BatchAckMsg.class);
                    brokerController.getBrokerMetricsManager().getPopMetricsManager().incPopReviveAckGetCount(bAckMsg, queueId);
                    String brokerName = StringUtils.isNotBlank(bAckMsg.getBrokerName()) ?
                        bAckMsg.getBrokerName() : brokerController.getBrokerConfig().getBrokerName();
                    String mergeKey = bAckMsg.getTopic() + bAckMsg.getConsumerGroup() + bAckMsg.getQueueId() + bAckMsg.getStartOffset() + bAckMsg.getPopTime() + brokerName;
                    PopCheckPoint point = map.get(mergeKey);
                    if (point == null) {
                        // default value of enableSkipLongAwaitingAck is false
                        if (!brokerController.getBrokerConfig().isEnableSkipLongAwaitingAck()) {
                            continue;
                        }
                        if (mockCkForAck(messageExt, bAckMsg, mergeKey, mockPointMap) && firstRt == 0) {
                            firstRt = mockPointMap.get(mergeKey).getReviveTime();
                        }
                    } else {
                        // merge ackMsgs into checkpoint
                        List<Long> ackOffsetList = bAckMsg.getAckOffsetList();
                        for (Long ackOffset : ackOffsetList) {
                            int indexOfAck = point.indexOfAck(ackOffset);
                            if (indexOfAck > -1) {
                                point.setBitMap(DataConverter.setBit(point.getBitMap(), indexOfAck, true));
                            } else {
                                POP_LOGGER.error("invalid batch ack index, {}, {}", bAckMsg, point);
                            }
                        }
                    }
                }
                long deliverTime = messageExt.getDeliverTimeMs();
                if (deliverTime > endTime) {
                    endTime = deliverTime;
                }
            }
            offset = offset + messageExts.size();
        }
        consumeReviveObj.map.putAll(mockPointMap);
        consumeReviveObj.endTime = endTime;
    }

    /**
     * Create a mock CK for an ack whose original CK has already been processed.
     *
     * <p>When an ack arrives long after its CK has been consumed (e.g. network
     * delay), the CK is no longer in the scan map. If {@code enableSkipLongAwaitingAck}
     * is enabled, this method creates a synthetic CK so that the revive offset
     * can still be advanced correctly in {@link #mergeAndRevive}.
     *
     * @param messageExt   the revive topic message that carried the ack
     * @param ackMsg       the decoded ack
     * @param mergeKey     the merge key for the CK lookup
     * @param mockPointMap map to collect the mock CKs
     * @return {@code true} if a mock CK was created
     */
    private boolean mockCkForAck(MessageExt messageExt, AckMsg ackMsg, String mergeKey, HashMap<String, PopCheckPoint> mockPointMap) {
        long ackWaitTime = System.currentTimeMillis() - messageExt.getDeliverTimeMs();
        long reviveAckWaitMs = brokerController.getBrokerConfig().getReviveAckWaitMs();
        if (ackWaitTime > reviveAckWaitMs) {
            // will use the reviveOffset of popCheckPoint to commit offset in mergeAndRevive
            PopCheckPoint mockPoint = createMockCkForAck(ackMsg, messageExt.getQueueOffset());
            POP_LOGGER.warn(
                    "ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{}, mockCk:{}",
                    reviveAckWaitMs, mergeKey, ackMsg, mockPoint);
            mockPointMap.put(mergeKey, mockPoint);
            return true;
        }
        return false;
    }

    /**
     * Build a synthetic checkpoint from an ack message.
     *
     * <p>The mock CK has {@code num = 0} and empty bitMap, meaning no actual
     * messages to revive. Its only purpose is to carry the {@code reviveOffset}
     * so that the revive consumer offset can be committed past this ack.
     *
     * @param ackMsg       the ack message
     * @param reviveOffset the queue offset of the ack message in the revive topic
     * @return a mock checkpoint with no sub-messages
     */
    private PopCheckPoint createMockCkForAck(AckMsg ackMsg, long reviveOffset) {
        PopCheckPoint point = new PopCheckPoint();
        point.setStartOffset(ackMsg.getStartOffset());
        point.setPopTime(ackMsg.getPopTime());
        point.setQueueId(ackMsg.getQueueId());
        point.setCId(ackMsg.getConsumerGroup());
        point.setTopic(ackMsg.getTopic());
        point.setNum((byte) 0);
        point.setBitMap(0);
        point.setReviveOffset(reviveOffset);
        point.setBrokerName(ackMsg.getBrokerName());
        return point;
    }

    /**
     * Process collected checkpoints and revive all un-acked sub-messages.
     *
     * <p>Checkpoints are sorted by revive offset. For each one:
     * <ul>
     *   <li>Skip if the revive time has not yet elapsed (within
     *       {@code ackTimeInterval + 1s} of {@code endTime})</li>
     *   <li>Skip if the normal topic or consumer group no longer exists</li>
     *   <li>Wait if too many revives are already in-flight (max 3)</li>
     *   <li>Call {@link #reviveMsgFromCk} to re-publish un-acked messages</li>
     * </ul>
     *
     * <p>After processing, the revive topic offset is advanced past all
     * processed checkpoints.
     *
     * @param consumeReviveObj the container with collected CKs and scan state
     * @throws Throwable if any revive operation fails
     */
    protected void mergeAndRevive(ConsumeReviveObj consumeReviveObj) throws Throwable {
        // sort checkpoints and init newOffset
        ArrayList<PopCheckPoint> sortList = consumeReviveObj.genSortList();
        POP_LOGGER.info("reviveQueueId={}, ck listSize={}", queueId, sortList.size());
        if (sortList.size() != 0) {
            POP_LOGGER.info("reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={}; last ck, startOffset={}, reviveOffset={}", queueId, sortList.get(0).getStartOffset(),
                sortList.get(0).getReviveOffset(), sortList.get(sortList.size() - 1).getStartOffset(), sortList.get(sortList.size() - 1).getReviveOffset());
        }
        long newOffset = consumeReviveObj.oldOffset;

        for (PopCheckPoint popCheckPoint : sortList) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip ck process, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                break;
            }
            if (consumeReviveObj.endTime - popCheckPoint.getReviveTime() <= (PopAckConstants.ackTimeInterval + PopAckConstants.SECOND)) {
                break;
            }

            // check normal topic, skip ck , if normal topic is not exist
            String normalTopic = KeyBuilder.parseNormalTopic(popCheckPoint.getTopic(), popCheckPoint.getCId());
            if (brokerController.getTopicConfigManager().selectTopicConfig(normalTopic) == null) {
                POP_LOGGER.warn("reviveQueueId={}, can not get normal topic {}, then continue", queueId, popCheckPoint.getTopic());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }
            if (null == brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(popCheckPoint.getCId())) {
                POP_LOGGER.warn("reviveQueueId={}, can not get cid {}, then continue", queueId, popCheckPoint.getCId());
                newOffset = popCheckPoint.getReviveOffset();
                continue;
            }

            // Concurrency control for revive: skip first long-running revive task.
            while (inflightReviveRequestMap.size() > 3) {
                waitForRunning(100);
                Pair<Long, Boolean> pair = inflightReviveRequestMap.firstEntry().getValue();
                // if first revive task is timeout, reput it to revive topic, then skip
                if (!pair.getObject2() && System.currentTimeMillis() - pair.getObject1() > 1000 * 30) {
                    PopCheckPoint oldCK = inflightReviveRequestMap.firstKey();
                    // reput checkpoint to revive topic
                    rePutCK(oldCK, pair);
                    inflightReviveRequestMap.remove(oldCK);
                    POP_LOGGER.warn("stay too long, remove from reviveRequestMap, {}, {}, {}, {}", popCheckPoint.getTopic(),
                            popCheckPoint.getBrokerName(), popCheckPoint.getQueueId(), popCheckPoint.getStartOffset());
                }
            }

            // revive message
            reviveMsgFromCk(popCheckPoint);
            newOffset = popCheckPoint.getReviveOffset();
        }

        // commit offset
        if (newOffset > consumeReviveObj.oldOffset) {
            if (!shouldRunPopRevive) {
                POP_LOGGER.info("slave skip commit, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                return;
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, newOffset);
        }
        reviveOffset = newOffset;
        consumeReviveObj.newOffset = newOffset;
    }

    /**
     * Revive all un-acked sub-messages in a checkpoint:
     * - reput message to revive topic
     * - put message to retry topic
     *
     * <p>For each sub-message whose bit is not set in the bitMap, the original
     * message is fetched via {@link #getBizMessage} and re-published to the
     * retry topic via {@link #reviveRetry}. All revive attempts run
     * concurrently via {@link CompletableFuture#allOf}.
     *
     * <p>After all attempts complete:
     * <ul>
     *   <li>Failed offsets are re-queued via {@link #rePutCK}</li>
     *   <li>The {@link #inflightReviveRequestMap} is updated and completed
     *       entries are removed in order, advancing the revive offset</li>
     * </ul>
     *
     * @param popCheckPoint the checkpoint whose un-acked messages should be revived
     */
    private void reviveMsgFromCk(PopCheckPoint popCheckPoint) {
        // env check and init
        if (!shouldRunPopRevive) {
            POP_LOGGER.info("slave skip retry, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
            return;
        }
        inflightReviveRequestMap.put(popCheckPoint, new Pair<>(System.currentTimeMillis(), false));
        List<CompletableFuture<Pair<Long, Boolean>>> futureList = new ArrayList<>(popCheckPoint.getNum());

        // put message to retry topic if checkpoint was not acked
        for (int j = 0; j < popCheckPoint.getNum(); j++) {
            // if checkpoint was acked, skip
            if (DataConverter.getBit(popCheckPoint.getBitMap(), j)) {
                continue;
            }

            // get message by checkpoint, then put message to retry topic
            long msgOffset = popCheckPoint.ackOffsetByIndex((byte) j);
            CompletableFuture<Pair<Long, Boolean>> future = getBizMessage(popCheckPoint, msgOffset)
                .thenApply(rst -> {
                    // validate message
                    MessageExt message = rst.getLeft();
                    if (message == null) {
                        POP_LOGGER.info("reviveQueueId={}, can not get biz msg, topic:{}, qid:{}, offset:{}, brokerName:{}, info:{}, retry:{}, then continue",
                            queueId, popCheckPoint.getTopic(), popCheckPoint.getQueueId(), msgOffset, popCheckPoint.getBrokerName(), UtilAll.frontStringAtLeast(rst.getMiddle(), 60), rst.getRight());
                        return new Pair<>(msgOffset, !rst.getRight()); // Pair.object2 means OK or not, Triple.right value means needRetry
                    }
                    boolean result = reviveRetry(popCheckPoint, message);
                    return new Pair<>(msgOffset, result);
                });
            futureList.add(future);
        }

        // reput checkpoint to revive topic if retry failed
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
            .whenComplete((v, e) -> {
                // reput checkpoint
                for (CompletableFuture<Pair<Long, Boolean>> future : futureList) {
                    Pair<Long, Boolean> pair = future.getNow(new Pair<>(0L, false));
                    if (!pair.getObject2()) {
                        rePutCK(popCheckPoint, pair);
                    }
                }

                // update ack status of inflight checkpoint
                if (inflightReviveRequestMap.containsKey(popCheckPoint)) {
                    inflightReviveRequestMap.get(popCheckPoint).setObject2(true);
                }

                // commit offset and remove inflight checkpoint
                for (Map.Entry<PopCheckPoint, Pair<Long, Boolean>> entry : inflightReviveRequestMap.entrySet()) {
                    PopCheckPoint oldCK = entry.getKey();
                    Pair<Long, Boolean> pair = entry.getValue();
                    if (pair.getObject2()) {
                        brokerController.getConsumerOffsetManager().commitOffset(PopAckConstants.LOCAL_HOST, PopAckConstants.REVIVE_GROUP, reviveTopic, queueId, oldCK.getReviveOffset());
                        inflightReviveRequestMap.remove(oldCK);
                    } else {
                        break;
                    }
                }
            });
    }

    /**
     * Re-write a checkpoint to the revive topic after a failed revive attempt.
     *
     * <p>When a sub-message cannot be revived (e.g. the original message is
     * temporarily unavailable), the CK is re-published with:
     * <ul>
     *   <li>A single sub-message targeting the failed offset</li>
     *   <li>An increased {@code rePutTimes} and an extended invisible time
     *       based on the backoff interval</li>
     *   <li>A cleared bitMap, so the next revive cycle will retry it</li>
     * </ul>
     *
     * <p>If {@code rePutTimes} exceeds the backoff table length and
     * {@code skipWhenCKRePutReachMaxTimes} is set, the CK is dropped.
     *
     * @param oldCK the original checkpoint that failed to revive
     * @param pair   the failed offset and result (object1 = offset, object2 = result)
     */
    private void rePutCK(PopCheckPoint oldCK, Pair<Long, Boolean> pair) {
        int rePutTimes = oldCK.parseRePutTimes();
        if (rePutTimes >= ckRewriteIntervalsInSeconds.length && brokerController.getBrokerConfig().isSkipWhenCKRePutReachMaxTimes()) {
            POP_LOGGER.warn("rePut CK reach max times, drop it. {}, {}, {}, {}-{}, {}, {}, {}", oldCK.getTopic(), oldCK.getCId(),
                    oldCK.getBrokerName(), oldCK.getQueueId(), pair.getObject1(), oldCK.getPopTime(), oldCK.getInvisibleTime(), rePutTimes);
            return;
        }

        PopCheckPoint newCk = new PopCheckPoint();
        newCk.setBitMap(0);
        newCk.setNum((byte) 1);
        newCk.setPopTime(oldCK.getPopTime());
        newCk.setInvisibleTime(oldCK.getInvisibleTime());
        newCk.setStartOffset(pair.getObject1());
        newCk.setCId(oldCK.getCId());
        newCk.setTopic(oldCK.getTopic());
        newCk.setQueueId(oldCK.getQueueId());
        newCk.setBrokerName(oldCK.getBrokerName());
        newCk.addDiff(0);
        newCk.setRePutTimes(String.valueOf(rePutTimes + 1)); // always increment even if removed from reviveRequestMap
        if (oldCK.getReviveTime() <= System.currentTimeMillis()) {
            // never expect an ACK matched in the future, we just use it to rewrite CK and try to revive retry message next time
            int intervalIndex = rePutTimes >= ckRewriteIntervalsInSeconds.length ? ckRewriteIntervalsInSeconds.length - 1 : rePutTimes;
            newCk.setInvisibleTime(oldCK.getInvisibleTime() + ckRewriteIntervalsInSeconds[intervalIndex] * 1000);
        }
        MessageExtBrokerInner ckMsg = brokerController.getPopMessageProcessor().buildCkMsg(newCk, queueId);
        brokerController.getMessageStore().putMessage(ckMsg);
    }

    public long getReviveBehindMillis() throws ConsumeQueueException {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        long maxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId);
        if (maxOffset - reviveOffset > 1) {
            return Math.max(0, System.currentTimeMillis() - currentReviveMessageTimestamp);
        }
        return 0;
    }

    public long getReviveBehindMessages() throws ConsumeQueueException {
        if (currentReviveMessageTimestamp <= 0) {
            return 0;
        }
        // the next pull offset is reviveOffset + 1
        long diff = brokerController.getMessageStore().getMaxOffsetInQueue(reviveTopic, queueId) - reviveOffset - 1;
        return Math.max(0, diff);
    }

    /**
     * Main loop: periodically consume revive messages and revive timed-out CKs.
     *
     * <p>Each iteration:
     * <ol>
     *   <li>Waits for {@code reviveInterval} (configurable)</li>
     *   <li>Calls {@link #consumeReviveMessage} to scan the revive topic and
     *       merge checkpoints with their corresponding AckMsg</li>
     *   <li>Calls {@link #mergeAndRevive} to re-publish all un-acked
     *       sub-messages whose revive time has elapsed</li>
     *   <li>If no checkpoints were processed, increases a {@code slow} counter and
     *       sleeps longer — the idle interval ramps up to
     *       {@code reviveMaxSlow * reviveInterval}</li>
     * </ol>
     */
    @Override
    public void run() {
        int slow = 1;
        while (!this.isStopped()) {
            try {
                // env check
                if (System.currentTimeMillis() < brokerController.getShouldStartTime()) {
                    POP_LOGGER.info("PopReviveService Ready to run after {}", brokerController.getShouldStartTime());
                    this.waitForRunning(1000);
                    continue;
                }
                this.waitForRunning(brokerController.getBrokerConfig().getReviveInterval());
                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("skip start revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                if (!brokerController.getMessageStore().getMessageStoreConfig().isTimerWheelEnable()) {
                    POP_LOGGER.warn("skip revive topic because timerWheelEnable is false");
                    continue;
                }

                POP_LOGGER.info("start revive topic={}, reviveQueueId={}", reviveTopic, queueId);

                // consume revive message
                ConsumeReviveObj consumeReviveObj = new ConsumeReviveObj();
                consumeReviveMessage(consumeReviveObj);

                if (!shouldRunPopRevive) {
                    POP_LOGGER.info("slave skip scan, revive topic={}, reviveQueueId={}", reviveTopic, queueId);
                    continue;
                }

                // merge checkpoint and ackMsg then revive
                mergeAndRevive(consumeReviveObj);

                // wait and logging
                ArrayList<PopCheckPoint> sortList = consumeReviveObj.sortList;
                long delay = 0;
                if (sortList != null && !sortList.isEmpty()) {
                    delay = (System.currentTimeMillis() - sortList.get(0).getReviveTime()) / 1000;
                    currentReviveMessageTimestamp = sortList.get(0).getReviveTime();
                    slow = 1;
                } else {
                    currentReviveMessageTimestamp = System.currentTimeMillis();
                }

                POP_LOGGER.info("reviveQueueId={}, revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
                    queueId, consumeReviveObj.oldOffset, consumeReviveObj.newOffset, delay);

                if (sortList == null || sortList.isEmpty()) {
                    POP_LOGGER.info("reviveQueueId={}, has no new msg, take a rest {}", queueId, slow);
                    this.waitForRunning(slow * brokerController.getBrokerConfig().getReviveInterval());
                    if (slow < brokerController.getBrokerConfig().getReviveMaxSlow()) {
                        slow++;
                    }
                }

            } catch (Throwable e) {
                POP_LOGGER.error("reviveQueueId={}, revive error", queueId, e);
            }
        }
    }

    static class ConsumeReviveObj {
        HashMap<String, PopCheckPoint> map = new HashMap<>();
        ArrayList<PopCheckPoint> sortList;
        long oldOffset;
        long endTime;
        long newOffset;

        ArrayList<PopCheckPoint> genSortList() {
            if (sortList != null) {
                return sortList;
            }
            sortList = new ArrayList<>(map.values());
            sortList.sort((o1, o2) -> (int) (o1.getReviveOffset() - o2.getReviveOffset()));
            return sortList;
        }
    }
}
